"""
Reliable Transport Protocol — Phase 3: sliding window / pipelining.

Stop-and-wait (Phase 2) is correct but slow: one full round trip per packet,
since only one packet is ever outstanding. This phase fixes that by allowing
up to `window_size` packets in flight at once, with CUMULATIVE ACKs (an ACK
for seq N means "I have everything up through N, in order") and a receiver-
side reassembly buffer that holds out-of-order arrivals until the gap in
front of them is filled.

Run the demo:   python transport_phase3.py
Run the tests:  python test_phase3.py
"""

import time
import socket
import asyncio
from dataclasses import dataclass, field

from transport_phase1 import Packet, encode, decode, ChecksumError, LossyChannel, FLAG_SYN, FLAG_FIN
from transport_phase2 import GaveUpError

ACK_FLAG = 0b010
DEFAULT_TIMEOUT = 0.2
DEFAULT_WINDOW = 8


@dataclass
class _Outstanding:
    """One packet the sender is waiting to have cumulatively ACKed."""
    seq: int
    wire: bytes
    sent_at: float
    retries: int = 0


class SlidingWindowSender:
    """Sends a stream of payloads with up to `window_size` packets in flight,
    cumulative ACKs, and per-packet retransmission timers."""

    def __init__(self, channel: LossyChannel, sock: socket.socket, dest_addr,
                 window_size: int = DEFAULT_WINDOW, timeout: float = DEFAULT_TIMEOUT,
                 max_retries: int = 30):
        self.channel = channel
        self.sock = sock
        self.dest_addr = dest_addr
        self.window_size = window_size
        self.timeout = timeout
        self.max_retries = max_retries
        self.transmissions = 0
        self.retransmissions = 0

    async def send_all(self, payloads: list[bytes]):
        """Reliably deliver every payload, pipelined, returning once every
        one has been cumulatively ACKed."""
        loop = asyncio.get_running_loop()
        n = len(payloads)
        outstanding: dict[int, _Outstanding] = {}
        base = 0            # oldest un-ACKed seq (cumulative ACK frontier)
        next_seq = 0        # next seq not yet sent
        recv_task = asyncio.create_task(self._recv_loop())
        self._ack_events: dict[int, asyncio.Event] = {}
        self._highest_ack = -1
        self._ack_signal = asyncio.Event()

        async def send_one(seq):
            pkt = Packet(seq_num=seq, ack_num=0, flags=0, payload=payloads[seq])
            wire = encode(pkt)
            outstanding[seq] = _Outstanding(seq=seq, wire=wire, sent_at=loop.time())
            await self.channel.send(wire, self.dest_addr)
            self.transmissions += 1

        try:
            while base < n:
                # fill the window
                while next_seq < n and next_seq < base + self.window_size:
                    await send_one(next_seq)
                    next_seq += 1

                # wait for either a fresh cumulative ACK or the earliest timeout
                earliest = min((o.sent_at for o in outstanding.values()
                               if o.seq >= base), default=None)
                remaining = (earliest + self.timeout - loop.time()) if earliest else self.timeout
                try:
                    await asyncio.wait_for(self._ack_signal.wait(), timeout=max(remaining, 0))
                    self._ack_signal.clear()
                except asyncio.TimeoutError:
                    pass   # nothing new ACKed within the window — check for timeouts below

                # advance base past anything cumulatively ACKed
                while base < n and base in outstanding and self._highest_ack >= base:
                    del outstanding[base]
                    base += 1

                # retransmit anything in-window that's timed out and still unacked
                now = loop.time()
                for seq in range(base, next_seq):
                    o = outstanding.get(seq)
                    if o and now - o.sent_at >= self.timeout:
                        if o.retries >= self.max_retries:
                            raise GaveUpError(
                                f"seq {seq} not cumulatively ACKed after "
                                f"{self.max_retries} retries — receiver may be "
                                f"unreachable or has stopped responding")
                        await self.channel.send(o.wire, self.dest_addr)
                        o.sent_at = now
                        o.retries += 1
                        self.transmissions += 1
                        self.retransmissions += 1
        finally:
            recv_task.cancel()
            try:
                await recv_task
            except asyncio.CancelledError:
                pass

    async def _recv_loop(self):
        """Continuously read ACKs and update the cumulative-ack frontier."""
        loop = asyncio.get_running_loop()
        while True:
            try:
                data, _ = self.sock.recvfrom(65536)
            except BlockingIOError:
                await asyncio.sleep(0.001)
                continue
            try:
                pkt = decode(data)
            except (ChecksumError, ValueError):
                continue
            if (pkt.has_flag(ACK_FLAG) and not pkt.has_flag(FLAG_SYN)
                    and not pkt.has_flag(FLAG_FIN) and pkt.ack_num > self._highest_ack):
                self._highest_ack = pkt.ack_num
                self._ack_signal.set()


class SlidingWindowReceiver:
    """Receives out-of-order packets into a reassembly buffer, delivers
    payloads in order as gaps fill, and sends CUMULATIVE ACKs (ack_num = the
    highest seq for which everything up to and including it has arrived)."""

    def __init__(self, sock: socket.socket, channel: LossyChannel):
        self.sock = sock
        self.channel = channel
        self.expected_seq = 0
        self.buffer: dict[int, bytes] = {}   # seq -> payload, for out-of-order arrivals
        self.delivered: list[bytes] = []

    LINGER = 2.0   # seconds to keep re-acking after delivery completes — see run_until()
    # (must comfortably exceed the sender's worst-case give-up time, e.g.
    # max_retries * timeout, so the receiver never falls silent while the
    # sender might still legitimately be retrying)

    async def run_until(self, n_packets: int, linger: float | None = None):
        """Receive and ACK packets until n_packets have been delivered, then
        LINGER briefly, still re-acking anything that arrives.

        Without this, the receiver would return the instant it finishes and
        stop participating entirely — and if its very last ACK happened to be
        dropped by the channel, the sender would retransmit into silence
        forever, since nothing would be left alive to re-acknowledge it.
        Lingering briefly — the same idea behind TCP's TIME_WAIT state — is a
        stand-in for that: stay reachable a little longer so a late
        retransmission still gets a fresh ACK.

        `linger` overrides the class default (self.LINGER). Pass a small
        value (or 0) when a proper FIN/FIN-ACK teardown (Phase 5) will run
        immediately afterward — that's the principled version of this same
        safety net, and running both at their full duration back to back
        just makes the caller wait through two overlapping "just in case"
        windows for no benefit.
        """
        if linger is None:
            linger = self.LINGER
        loop = asyncio.get_running_loop()

        async def handle_one(data, addr):
            try:
                pkt = decode(data)
            except (ChecksumError, ValueError):
                return
            if pkt.has_flag(FLAG_SYN) or pkt.has_flag(FLAG_FIN):
                # a handshake or teardown control packet, not data -- this
                # loop only processes plain data packets. Leaving it alone
                # here lets a separate, dedicated piece of code (e.g. Phase
                # 5's receiver_teardown) see and handle it instead; consuming
                # it here would silently swallow it from whoever's actually
                # meant to respond to it.
                return
            if pkt.seq_num >= self.expected_seq:
                self.buffer[pkt.seq_num] = pkt.payload   # store (harmless if dup)
            while self.expected_seq in self.buffer:
                self.delivered.append(self.buffer.pop(self.expected_seq))
                self.expected_seq += 1
            # cumulative ACK: "I have everything through expected_seq - 1".
            # Only send one once expected_seq > 0 -- before anything has been
            # delivered in order there's nothing meaningful to acknowledge,
            # and expected_seq - 1 would be negative (which can't be packed
            # into the unsigned ack_num field). The sender's own timeout will
            # drive retransmission of packet 0 until it arrives.
            if self.expected_seq > 0:
                cumulative_ack = self.expected_seq - 1
                ack = Packet(seq_num=0, ack_num=cumulative_ack, flags=ACK_FLAG, payload=b"")
                await self.channel.send(encode(ack), addr)

        while len(self.delivered) < n_packets:
            data, addr = await self._recv()
            await handle_one(data, addr)

        deadline = loop.time() + linger
        while loop.time() < deadline:
            remaining = deadline - loop.time()
            try:
                data, addr = await asyncio.wait_for(self._recv(), timeout=remaining)
            except asyncio.TimeoutError:
                break
            await handle_one(data, addr)

    async def _recv(self):
        while True:
            try:
                return self.sock.recvfrom(65536)
            except BlockingIOError:
                await asyncio.sleep(0.001)


# ===========================================================================
# Demo: throughput comparison, windowed vs. stop-and-wait, same conditions
# ===========================================================================
async def _run_windowed(n, drop_rate, window_size, timeout=0.05):
    tx = socket.socket(socket.AF_INET, socket.SOCK_DGRAM); tx.bind(("127.0.0.1", 0)); tx.setblocking(False)
    rx = socket.socket(socket.AF_INET, socket.SOCK_DGRAM); rx.bind(("127.0.0.1", 0)); rx.setblocking(False)
    fwd = LossyChannel(tx, drop_rate=drop_rate)
    back = LossyChannel(rx, drop_rate=drop_rate)
    receiver = SlidingWindowReceiver(rx, back)
    sender = SlidingWindowSender(fwd, tx, rx.getsockname(), window_size=window_size, timeout=timeout)
    payloads = [f"pkt-{i}".encode() for i in range(n)]

    recv_task = asyncio.create_task(receiver.run_until(n))
    t0 = time.time()
    await sender.send_all(payloads)
    elapsed = time.time() - t0   # real completion time — excludes the receiver's linger period
    await recv_task              # still wait for it, to cleanly finish and confirm delivery

    assert receiver.delivered == payloads, "delivery correctness failed in demo!"
    tx.close(); rx.close()
    return elapsed, sender.transmissions, sender.retransmissions


async def _run_stopwait_baseline(n, drop_rate, timeout=0.05):
    # import locally to avoid a hard dependency at module load time
    from transport_phase2 import StopAndWaitSender, StopAndWaitReceiver
    tx = socket.socket(socket.AF_INET, socket.SOCK_DGRAM); tx.bind(("127.0.0.1", 0)); tx.setblocking(False)
    rx = socket.socket(socket.AF_INET, socket.SOCK_DGRAM); rx.bind(("127.0.0.1", 0)); rx.setblocking(False)
    fwd = LossyChannel(tx, drop_rate=drop_rate)
    back = LossyChannel(rx, drop_rate=drop_rate)
    receiver = StopAndWaitReceiver(rx, back)
    sender = StopAndWaitSender(fwd, tx, rx.getsockname(), timeout=timeout)

    recv_task = asyncio.create_task(receiver.run_until(n))
    t0 = time.time()
    for i in range(n):
        await sender.send(f"pkt-{i}".encode())
    await recv_task
    elapsed = time.time() - t0
    tx.close(); rx.close()
    return elapsed


async def _demo():
    N = 60
    DROP = 0.1
    print(f"Comparing throughput: {N} packets, {int(DROP*100)}% simulated loss (both directions)\n")

    sw_elapsed, transmissions, retransmissions = await _run_windowed(N, DROP, window_size=8)
    print(f"  sliding window (size 8): {sw_elapsed:.2f}s  ({N/sw_elapsed:.1f} pkts/sec)  "
         f"[{transmissions} transmissions, {retransmissions} retries]")

    saw_elapsed = await _run_stopwait_baseline(N, DROP)
    print(f"  stop-and-wait (Phase 2): {saw_elapsed:.2f}s  ({N/saw_elapsed:.1f} pkts/sec)")

    print(f"\n  speedup: {saw_elapsed/sw_elapsed:.1f}x")


if __name__ == "__main__":
    asyncio.run(_demo())
