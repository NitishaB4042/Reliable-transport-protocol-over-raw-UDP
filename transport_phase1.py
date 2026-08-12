"""
Reliable Transport Protocol — Phase 1: packet framing + the lossy-channel simulator.

Goal of this phase: define the packet format (encode/decode, with a checksum),
and build a LOSSY CHANNEL — a wrapper that sits between two UDP sockets and
deliberately drops, reorders, and duplicates packets at configurable rates.

Why this matters more than it looks: on localhost, real UDP essentially never
drops or reorders packets on its own — everything arrives instantly and in
order. That means you CANNOT test reliability logic (Phase 2 onward) without
deliberately breaking things yourself. Every later phase is tested against
this simulator. If the simulator's own failure injection isn't trustworthy,
nothing built on top of it proves anything — so this phase spends real effort
proving the simulator itself is honest before anything else is built.

Run the demo:   python transport_phase1.py
Run the tests:  python test_phase1.py
"""

import struct
import random
import zlib
import socket
import asyncio
from dataclasses import dataclass


# ===========================================================================
# Packet format
#
#   seq_num (4B) | ack_num (4B) | flags (1B) | checksum (4B) | payload_len (2B) | payload
#
# flags bits: 0=SYN 1=ACK 2=FIN  (used from Phase 5 onward; defined now so the
# header format never has to change later)
# ===========================================================================
FLAG_SYN = 0b001
FLAG_ACK = 0b010
FLAG_FIN = 0b100

_HEADER_FMT = "!IIBIH"    # network byte order: 2x uint32 (seq, ack), 1x uint8 (flags),
                          # 1x uint32 (checksum), 1x uint16 (payload length)
HEADER_SIZE = struct.calcsize(_HEADER_FMT)


@dataclass
class Packet:
    seq_num: int
    ack_num: int
    flags: int
    payload: bytes

    def has_flag(self, flag: int) -> bool:
        return bool(self.flags & flag)


def encode(pkt: Packet) -> bytes:
    """Turn a Packet into bytes ready to put on the wire, checksum included."""
    body = pkt.payload
    checksum = zlib.crc32(body) & 0xFFFFFFFF
    header = struct.pack(_HEADER_FMT, pkt.seq_num, pkt.ack_num, pkt.flags,
                         checksum, len(body))
    return header + body


class ChecksumError(ValueError):
    """Raised by decode() when the payload doesn't match its checksum —
    i.e. the packet was corrupted in transit."""


def decode(raw: bytes) -> Packet:
    """Turn wire bytes back into a Packet. Raises ChecksumError if corrupted,
    or ValueError if the bytes are too short to even contain a header."""
    if len(raw) < HEADER_SIZE:
        raise ValueError(f"packet too short: {len(raw)} bytes, need >= {HEADER_SIZE}")
    seq_num, ack_num, flags, checksum, payload_len = struct.unpack(
        _HEADER_FMT, raw[:HEADER_SIZE])
    payload = raw[HEADER_SIZE:HEADER_SIZE + payload_len]
    if len(payload) != payload_len:
        raise ValueError(f"truncated payload: expected {payload_len}, got {len(payload)}")
    actual = zlib.crc32(payload) & 0xFFFFFFFF
    if actual != checksum:
        raise ChecksumError(f"checksum mismatch: header says {checksum}, actual {actual}")
    return Packet(seq_num=seq_num, ack_num=ack_num, flags=flags, payload=payload)


# ===========================================================================
# The lossy-channel simulator
#
# Sits between "send" and "actually goes out" for a UDP socket. Each outgoing
# packet is independently, randomly: dropped, duplicated, and/or delayed
# (which — combined with async sending — produces reordering, since a delayed
# packet can arrive after a later one that wasn't delayed).
# ===========================================================================
class LossyChannel:
    """Wraps a UDP socket's sendto with configurable failure injection.

    drop_rate    : probability [0,1] a packet is silently dropped (never sent)
    dup_rate     : probability [0,1] a packet is sent TWICE
    reorder_rate : probability [0,1] a packet's send is delayed, which can let
                   later (non-delayed) packets overtake it -> arrives out of order
    reorder_delay: how long (seconds) a "reordered" packet is held back
    """

    def __init__(self, sock: socket.socket, drop_rate: float = 0.0,
                 dup_rate: float = 0.0, reorder_rate: float = 0.0,
                 reorder_delay: float = 0.05, rng: random.Random | None = None):
        for name, val in [("drop_rate", drop_rate), ("dup_rate", dup_rate),
                          ("reorder_rate", reorder_rate)]:
            if not 0.0 <= val <= 1.0:
                raise ValueError(f"{name} must be in [0, 1], got {val}")
        self.sock = sock
        self.drop_rate = drop_rate
        self.dup_rate = dup_rate
        self.reorder_rate = reorder_rate
        self.reorder_delay = reorder_delay
        self.rng = rng or random.Random()

        # counters, purely for observability / tests — prove the injection
        # rates actually match what was configured
        self.sent = 0
        self.dropped = 0
        self.duplicated = 0
        self.reordered = 0

    async def send(self, data: bytes, addr):
        """Send one packet through the simulated lossy channel."""
        if self.rng.random() < self.drop_rate:
            self.dropped += 1
            return   # packet never goes out — this IS the failure being tested

        delay = 0.0
        if self.rng.random() < self.reorder_rate:
            self.reordered += 1
            delay = self.reorder_delay

        async def _send_one():
            if delay:
                await asyncio.sleep(delay)
            self.sock.sendto(data, addr)
            self.sent += 1

        if self.rng.random() < self.dup_rate:
            self.duplicated += 1
            # fire the duplicate immediately (no delay), then the (possibly
            # delayed) original — order between the two isn't guaranteed,
            # which is realistic: duplicates arriving out of sequence happens.
            self.sock.sendto(data, addr)
            self.sent += 1

        if delay:
            asyncio.create_task(_send_one())
        else:
            await _send_one()

    def stats(self) -> dict:
        return {"sent": self.sent, "dropped": self.dropped,
                "duplicated": self.duplicated, "reordered": self.reordered}


# ===========================================================================
# Demo
# ===========================================================================
def _demo_framing():
    print("Encoding and decoding a packet:\n")
    pkt = Packet(seq_num=42, ack_num=0, flags=FLAG_SYN, payload=b"hello, transport layer")
    wire = encode(pkt)
    print(f"  encoded: {len(wire)} bytes ({HEADER_SIZE} header + {len(pkt.payload)} payload)")
    back = decode(wire)
    print(f"  decoded: seq={back.seq_num} ack={back.ack_num} "
         f"flags={back.flags:03b} payload={back.payload!r}")
    assert back == pkt, "round-trip mismatch!"
    print("  round-trip OK\n")

    print("Corrupting one byte and decoding again:")
    corrupted = bytearray(wire)
    corrupted[-1] ^= 0xFF     # flip the last payload byte
    try:
        decode(bytes(corrupted))
        print("  (uh oh — corruption was not detected)")
    except ChecksumError as e:
        print(f"  correctly detected: {e}")


async def _demo_channel():
    print("\nSending 1000 packets through a channel with drop=0.2, dup=0.1, reorder=0.15:\n")
    a = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
    a.bind(("127.0.0.1", 0))
    b = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
    b.bind(("127.0.0.1", 0))
    b.setblocking(False)

    channel = LossyChannel(a, drop_rate=0.2, dup_rate=0.1, reorder_rate=0.15,
                           reorder_delay=0.01)
    for i in range(1000):
        pkt = Packet(seq_num=i, ack_num=0, flags=0, payload=str(i).encode())
        await channel.send(encode(pkt), b.getsockname())
    await asyncio.sleep(0.1)   # let delayed (reordered) sends finish

    stats = channel.stats()
    print(f"  configured: drop=0.20, dup=0.10, reorder=0.15")
    print(f"  observed:   dropped={stats['dropped']/1000:.2f}, "
         f"duplicated={stats['duplicated']/1000:.2f}, reordered={stats['reordered']/1000:.2f}")
    a.close(); b.close()


if __name__ == "__main__":
    _demo_framing()
    asyncio.run(_demo_channel())
