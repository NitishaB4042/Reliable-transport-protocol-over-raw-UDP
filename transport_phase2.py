"""
Reliable Transport Protocol — Phase 2: stop-and-wait reliable delivery.

The simplest possible reliable scheme, and the foundation everything else in
this project builds speed on top of: send one packet, wait for its ACK,
retransmit on timeout if it doesn't come. Correct but slow — one full round
trip per packet, since the sender never has more than one packet "in flight."

Tested against Phase 1's LossyChannel: the whole point is proving every byte
still arrives, exactly once, in order — even when a meaningful fraction of
packets are silently dropped, duplicated, or reordered underneath it.

Run the demo:   python transport_phase2.py
Run the tests:  python test_phase2.py
"""

import time
import socket
import asyncio
from dataclasses import dataclass, field

from transport_phase1 import Packet, encode, decode, ChecksumError, LossyChannel, FLAG_SYN, FLAG_FIN


DEFAULT_TIMEOUT = 0.2   # seconds to wait for an ACK before retransmitting
MAX_RETRIES = 50        # give up after this many retransmissions of one packet


class GaveUpError(RuntimeError):
    """Raised when a packet was retransmitted MAX_RETRIES times with no ACK."""


@dataclass
class SendStats:
    """Observability: how much retransmission a send actually needed."""
    packets_sent: int = 0          # application-level packets delivered
    transmissions: int = 0         # total wire sends, including retries
    retransmissions: int = field(init=False, default=0)

    def __post_init__(self):
        self.retransmissions = self.transmissions - self.packets_sent


class StopAndWaitSender:
    """Sends payloads one at a time: send, wait for ACK, retransmit on timeout."""

    def __init__(self, channel: LossyChannel, sock: socket.socket, dest_addr,
                 timeout: float = DEFAULT_TIMEOUT, max_retries: int = MAX_RETRIES):
        self.channel = channel
        self.sock = sock
        self.dest_addr = dest_addr
        self.timeout = timeout
        self.max_retries = max_retries
        self.next_seq = 0
        self.transmissions = 0
        self.packets_sent = 0

    async def send(self, payload: bytes) -> int:
        """Reliably deliver one payload. Returns the sequence number used.
        Raises GaveUpError if max_retries is exceeded with no ACK."""
        seq = self.next_seq
        pkt = Packet(seq_num=seq, ack_num=0, flags=0, payload=payload)
        wire = encode(pkt)

        for attempt in range(self.max_retries + 1):
            await self.channel.send(wire, self.dest_addr)
            self.transmissions += 1
            try:
                await self._wait_for_ack(seq)
                self.next_seq += 1
                self.packets_sent += 1
                return seq
            except asyncio.TimeoutError:
                continue   # retransmit

        raise GaveUpError(f"seq {seq} not ACKed after {self.max_retries} retries")

    async def _wait_for_ack(self, expected_seq: int):
        """Wait up to self.timeout for an ACK matching expected_seq. Any other
        (stale) ACK that arrives first is discarded and we keep waiting for
        the remainder of the timeout window — a duplicate old ACK must not be
        mistaken for acknowledging the current packet."""
        loop = asyncio.get_running_loop()
        deadline = loop.time() + self.timeout
        while True:
            remaining = deadline - loop.time()
            if remaining <= 0:
                raise asyncio.TimeoutError()
            try:
                data = await asyncio.wait_for(self._recv(), timeout=remaining)
            except asyncio.TimeoutError:
                raise
            try:
                ack_pkt = decode(data)
            except (ChecksumError, ValueError):
                continue   # corrupted ACK — ignore, keep waiting
            if (ack_pkt.has_flag(0b010) and not ack_pkt.has_flag(FLAG_SYN)
                    and not ack_pkt.has_flag(FLAG_FIN) and ack_pkt.ack_num == expected_seq):
                return   # the ACK we were waiting for
            # else: stale/irrelevant ACK (e.g. a duplicate of a previous one) — ignore it

    async def _recv(self) -> bytes:
        """Await one datagram on self.sock without blocking the event loop."""
        loop = asyncio.get_running_loop()
        while True:
            try:
                data, _ = self.sock.recvfrom(65536)
                return data
            except BlockingIOError:
                await asyncio.sleep(0.001)

    def stats(self) -> SendStats:
        return SendStats(packets_sent=self.packets_sent, transmissions=self.transmissions)


class StopAndWaitReceiver:
    """Receives packets, ACKs each one, and delivers payloads in order —
    even if the same packet arrives multiple times (duplicate suppression)."""

    ACK_FLAG = 0b010

    def __init__(self, sock: socket.socket, channel: LossyChannel):
        self.sock = sock
        self.channel = channel
        self.expected_seq = 0
        self.delivered: list[bytes] = []   # in-order, deduplicated payloads
        self.acks_sent = 0

    LINGER = 2.0   # seconds to keep re-acking after delivery completes — see run_until()
    # (must comfortably exceed the sender's worst-case give-up time, e.g.
    # max_retries * timeout, so the receiver never falls silent while the
    # sender might still legitimately be retrying)

    async def run_until(self, n_packets: int):
        """Receive and ACK packets until n_packets have been delivered, then
        LINGER briefly, still re-acking anything that arrives.

        Without this, the receiver would return the instant it finishes and
        stop participating entirely — and if its very last ACK happened to be
        dropped by the channel, the sender would retransmit into silence
        forever, since nothing would be left alive to re-acknowledge it. This
        project doesn't yet have real connection teardown (that's Phase 5),
        so lingering briefly — the same idea behind TCP's TIME_WAIT state —
        is the honest stand-in for now: stay reachable a little longer so a
        late retransmission still gets a fresh ACK.
        """
        loop = asyncio.get_running_loop()

        async def handle_one(data, addr):
            try:
                pkt = decode(data)
            except (ChecksumError, ValueError):
                return   # corrupted packet — silently ignore, sender will time out and retry
            if pkt.has_flag(FLAG_SYN) or pkt.has_flag(FLAG_FIN):
                return   # a handshake/teardown control packet, not data — leave it for
                         # whatever dedicated code is meant to handle it

            if pkt.seq_num == self.expected_seq:
                self.delivered.append(pkt.payload)
                self.expected_seq += 1
            elif pkt.seq_num < self.expected_seq:
                pass   # duplicate of an already-delivered packet — don't redeliver, but still ACK
            # (a seq_num > expected_seq "from the future" can't happen in
            # stop-and-wait: the sender never has more than one packet
            # outstanding, so nothing arrives out of order at this layer)

            # ACK whichever seq_num we just received (even a duplicate), back
            # to wherever it actually came from — the sender needs this in
            # case its own earlier ACK to us was lost
            ack = Packet(seq_num=0, ack_num=pkt.seq_num, flags=self.ACK_FLAG, payload=b"")
            await self.channel.send(encode(ack), addr)
            self.acks_sent += 1

        while len(self.delivered) < n_packets:
            data, addr = await self._recv()
            await handle_one(data, addr)

        deadline = loop.time() + self.LINGER
        while loop.time() < deadline:
            remaining = deadline - loop.time()
            try:
                data, addr = await asyncio.wait_for(self._recv(), timeout=remaining)
            except asyncio.TimeoutError:
                break
            await handle_one(data, addr)

    async def _recv(self):
        loop = asyncio.get_running_loop()
        while True:
            try:
                return self.sock.recvfrom(65536)
            except BlockingIOError:
                await asyncio.sleep(0.001)


# ===========================================================================
# Demo
# ===========================================================================
async def _demo():
    sender_sock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
    sender_sock.bind(("127.0.0.1", 0))
    sender_sock.setblocking(False)
    recv_sock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
    recv_sock.bind(("127.0.0.1", 0))
    recv_sock.setblocking(False)

    # Both directions go through their own lossy channel, so both data AND
    # acks are subject to loss/duplication — a realistic worst case.
    fwd_channel = LossyChannel(sender_sock, drop_rate=0.3, dup_rate=0.1)
    back_channel = LossyChannel(recv_sock, drop_rate=0.3, dup_rate=0.1)

    receiver = StopAndWaitReceiver(recv_sock, back_channel)
    sender = StopAndWaitSender(fwd_channel, sender_sock, recv_sock.getsockname(),
                               timeout=0.05)

    N = 30
    print(f"Reliably sending {N} packets over a channel with 30% drop, 10% dup, "
         f"in BOTH directions (data and ACKs)...\n")

    recv_task = asyncio.create_task(receiver.run_until(N))
    t0 = time.time()
    for i in range(N):
        await sender.send(f"message-{i}".encode())
    elapsed = time.time() - t0   # real completion time — excludes the receiver's linger period
    await recv_task              # still wait for it, to cleanly finish and confirm delivery

    stats = sender.stats()
    print(f"  delivered:       {len(receiver.delivered)}/{N} payloads, in order")
    print(f"  transmissions:   {stats.transmissions} (of which {stats.retransmissions} were retries)")
    print(f"  time elapsed:    {elapsed:.2f}s  ({elapsed/N*1000:.0f}ms/packet — "
         f"one round trip per packet, this is why Phase 3 adds a window)")

    assert receiver.delivered == [f"message-{i}".encode() for i in range(N)]
    print("\n  All payloads arrived exactly once, in order. ✓")

    sender_sock.close(); recv_sock.close()


if __name__ == "__main__":
    asyncio.run(_demo())
