"""
Tests for Reliable Transport Phase 1 — packet framing + the lossy channel.

Critically, the channel's failure injection is verified from the RECEIVER's
side (what actually arrived), not just by trusting the sender's own internal
counters — a simulator that only proves itself via its own bookkeeping proves
nothing. If the receiver actually sees ~20% missing, ~10% duplicated, and some
arrivals out of order, the simulator is trustworthy; everything built on top
of it in later phases depends on that being true.

Run: python test_phase1.py
"""

import socket
import random
import asyncio
from collections import Counter

from transport_phase1 import (Packet, encode, decode, ChecksumError,
                              LossyChannel, HEADER_SIZE, FLAG_SYN, FLAG_ACK)


# ---------------------------------------------------------------------------
# A minimal async receiver: listens on a UDP socket and records every
# datagram that actually arrives, in arrival order.
# ---------------------------------------------------------------------------
class _Receiver:
    def __init__(self):
        self.sock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
        # a larger receive buffer so a legitimate burst of sends doesn't hit
        # an OS-level buffer overflow that has nothing to do with the
        # simulator's own drop_rate — we want to measure ONLY the simulator's
        # injected loss, not incidental loss from an under-sized kernel buffer.
        self.sock.setsockopt(socket.SOL_SOCKET, socket.SO_RCVBUF, 4 * 1024 * 1024)
        self.sock.bind(("127.0.0.1", 0))
        self.sock.setblocking(False)
        self.received: list[bytes] = []   # in arrival order

    def addr(self):
        return self.sock.getsockname()

    async def drain(self, duration: float):
        """Collect whatever arrives over `duration` seconds."""
        loop = asyncio.get_running_loop()
        end = loop.time() + duration
        while loop.time() < end:
            try:
                data, _ = self.sock.recvfrom(65536)
                self.received.append(data)
            except BlockingIOError:
                await asyncio.sleep(0.002)

    def close(self):
        self.sock.close()


def _sender_socket():
    s = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
    s.bind(("127.0.0.1", 0))
    return s


async def _send_many(channel, n, addr, yield_every=20):
    """Send n packets (seq 0..n-1) through `channel`, yielding to the event
    loop every `yield_every` packets. Needed because LossyChannel.send() only
    truly suspends when a packet is reordered (delayed) — a run with no
    reordering would otherwise send all n packets as one uninterrupted
    synchronous burst, giving a concurrently-running receiver no chance to
    drain until the whole burst is over."""
    for i in range(n):
        await channel.send(encode(Packet(i, 0, 0, str(i).encode())), addr)
        if i % yield_every == 0:
            await asyncio.sleep(0)


# ---------------------------------------------------------------------------
# Framing tests
# ---------------------------------------------------------------------------
def test_encode_decode_round_trip():
    pkt = Packet(seq_num=7, ack_num=3, flags=FLAG_SYN | FLAG_ACK, payload=b"hello")
    back = decode(encode(pkt))
    assert back == pkt, back
    print("  encode_decode_round_trip: PASS")


def test_empty_payload_round_trips():
    pkt = Packet(seq_num=0, ack_num=0, flags=0, payload=b"")
    back = decode(encode(pkt))
    assert back.payload == b""
    print("  empty_payload_round_trips: PASS")


def test_checksum_catches_corruption():
    wire = bytearray(encode(Packet(seq_num=1, ack_num=0, flags=0, payload=b"important data")))
    wire[-1] ^= 0xFF   # flip a bit in the payload
    try:
        decode(bytes(wire))
        assert False, "corruption was not detected"
    except ChecksumError:
        pass
    print("  checksum_catches_corruption: PASS")


def test_truncated_packet_rejected():
    wire = encode(Packet(seq_num=1, ack_num=0, flags=0, payload=b"hello"))
    try:
        decode(wire[:HEADER_SIZE + 2])   # header says 5 bytes payload, only give 2
        assert False, "truncated packet should have been rejected"
    except ValueError:
        pass
    print("  truncated_packet_rejected: PASS")


def test_too_short_for_header_rejected():
    try:
        decode(b"\x00\x01")
        assert False, "should have rejected undersized input"
    except ValueError:
        pass
    print("  too_short_for_header_rejected: PASS")


def test_bad_channel_config_rejected():
    s = _sender_socket()
    for kwargs in [{"drop_rate": 1.5}, {"drop_rate": -0.1},
                  {"dup_rate": 2.0}, {"reorder_rate": -1.0}]:
        try:
            LossyChannel(s, **kwargs)
            assert False, f"should have rejected {kwargs}"
        except ValueError:
            pass
    s.close()
    print("  bad_channel_config_rejected: PASS")


# ---------------------------------------------------------------------------
# Channel tests — verified from the RECEIVER's side
# ---------------------------------------------------------------------------
def test_zero_failure_channel_is_perfectly_reliable():
    """With everything at 0, the channel should behave like plain UDP on
    localhost: every packet arrives, once, in order. Proves the simulator
    doesn't accidentally introduce failure when told not to."""
    async def go():
        rx = _Receiver()
        tx = _sender_socket()
        channel = LossyChannel(tx, drop_rate=0, dup_rate=0, reorder_rate=0)
        N = 200

        await asyncio.gather(_send_many(channel, N, rx.addr()), rx.drain(0.5))
        seqs = [decode(d).seq_num for d in rx.received]
        assert len(seqs) == N, f"expected {N} arrivals, got {len(seqs)}"
        assert seqs == list(range(N)), "packets arrived out of order with reorder_rate=0"
        rx.close(); tx.close()
    asyncio.run(go())
    print("  zero_failure_channel_is_perfectly_reliable: PASS")


def test_drop_rate_matches_what_actually_arrives():
    """The receiver should actually see close to (1 - drop_rate) of the
    packets sent — checked against real arrivals, not sender bookkeeping.

    Sending and draining run CONCURRENTLY (not send-all-then-drain): with
    thousands of packets, sending everything before the receiver starts
    pulling from the socket would overflow the OS's UDP receive buffer and
    cause extra loss that has nothing to do with the simulator being tested.
    """
    async def go():
        rx = _Receiver()
        tx = _sender_socket()
        rng = random.Random(42)
        channel = LossyChannel(tx, drop_rate=0.3, rng=rng)
        N = 2000

        await asyncio.gather(_send_many(channel, N, rx.addr()), rx.drain(1.0))
        arrived = len(rx.received)
        expected = N * 0.7
        # allow statistical slack: within 10% of the expected count
        assert abs(arrived - expected) < expected * 0.1, \
            f"expected ~{expected:.0f} arrivals, got {arrived}"
        rx.close(); tx.close()
    asyncio.run(go())
    print("  drop_rate_matches_what_actually_arrives: PASS")


def test_dup_rate_produces_real_duplicate_arrivals():
    """With dup_rate > 0 and drop_rate = 0, some sequence numbers should
    actually be RECEIVED MORE THAN ONCE."""
    async def go():
        rx = _Receiver()
        tx = _sender_socket()
        rng = random.Random(7)
        channel = LossyChannel(tx, drop_rate=0, dup_rate=0.5, rng=rng)
        N = 500

        await asyncio.gather(_send_many(channel, N, rx.addr()), rx.drain(0.5))
        seqs = [decode(d).seq_num for d in rx.received]
        counts = Counter(seqs)
        duplicated_seqs = sum(1 for c in counts.values() if c > 1)
        # with dup_rate=0.5 we expect roughly half the sequence numbers
        # to show up more than once
        assert duplicated_seqs > N * 0.3, \
            f"expected substantial duplication, only {duplicated_seqs}/{N} seqs duplicated"
        rx.close(); tx.close()
    asyncio.run(go())
    print("  dup_rate_produces_real_duplicate_arrivals: PASS")


def test_reorder_rate_produces_real_out_of_order_arrivals():
    """With reorder_rate > 0, at least some packets should actually ARRIVE
    out of the order they were sent in."""
    async def go():
        rx = _Receiver()
        tx = _sender_socket()
        rng = random.Random(99)
        channel = LossyChannel(tx, drop_rate=0, reorder_rate=0.3,
                              reorder_delay=0.02, rng=rng)
        N = 300

        await asyncio.gather(_send_many(channel, N, rx.addr()), rx.drain(0.5))
        seqs = [decode(d).seq_num for d in rx.received]
        # count adjacent inversions: how often seqs[i] > seqs[i+1]
        inversions = sum(1 for a, b in zip(seqs, seqs[1:]) if a > b)
        assert inversions > 0, "expected some out-of-order arrivals, saw none"
        rx.close(); tx.close()
    asyncio.run(go())
    print("  reorder_rate_produces_real_out_of_order_arrivals: PASS")


if __name__ == "__main__":
    print("Running Reliable Transport Phase 1 tests:")
    test_encode_decode_round_trip()
    test_empty_payload_round_trips()
    test_checksum_catches_corruption()
    test_truncated_packet_rejected()
    test_too_short_for_header_rejected()
    test_bad_channel_config_rejected()
    test_zero_failure_channel_is_perfectly_reliable()
    test_drop_rate_matches_what_actually_arrives()
    test_dup_rate_produces_real_duplicate_arrivals()
    test_reorder_rate_produces_real_out_of_order_arrivals()
    print("All tests passed.")
