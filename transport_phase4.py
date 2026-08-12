"""
Reliable Transport Protocol — Phase 4: congestion control (slow start + AIMD).

A FIXED window size (Phase 3) is either too cautious (wastes bandwidth when
the network is fine) or too aggressive (floods a struggling network). This
phase makes the window size adaptive:

  - SLOW START: cwnd starts small and grows exponentially (roughly doubling
    every round trip) — quickly finding available bandwidth without a fixed
    guess.
  - CONGESTION AVOIDANCE (AIMD): once cwnd passes a threshold, growth becomes
    linear (+1 per round trip) — cautious, incremental probing.
  - On a LOSS (a timeout-triggered retransmission): cwnd is cut in half
    (multiplicative decrease) and growth resumes from there in congestion
    avoidance — not back to slow start. Repeated loss-then-halve-then-regrow
    is exactly TCP's classic sawtooth.

Reuses Phase 3's SlidingWindowReceiver UNCHANGED — receiving/reassembly logic
doesn't change here, only how the SENDER sizes its window. That receiver
already carries the "linger" fix (see Phase 3's README), so this phase
doesn't have to rediscover it.

Run the demo:   python transport_phase4.py
Run the tests:  python test_phase4.py
"""

import time
import socket
import asyncio
from dataclasses import dataclass

from transport_phase1 import Packet, encode, decode, ChecksumError, LossyChannel
from transport_phase2 import GaveUpError
from transport_phase3 import SlidingWindowReceiver, ACK_FLAG

DEFAULT_TIMEOUT = 0.2
INITIAL_CWND = 1.0
INITIAL_SSTHRESH = 32.0
MAX_CWND = 64.0


@dataclass
class _Outstanding:
    seq: int
    wire: bytes
    sent_at: float
    retries: int = 0


@dataclass
class CwndSample:
    """One point for the sawtooth chart: cwnd right after some event."""
    t: float
    cwnd: float
    event: str   # "ack" | "loss"


class CongestionControlledSender:
    """Sliding-window sender whose window size (cwnd) adapts: slow start,
    then AIMD congestion avoidance, halving on loss."""

    def __init__(self, channel: LossyChannel, sock: socket.socket, dest_addr,
                 timeout: float = DEFAULT_TIMEOUT, max_retries: int = 30,
                 max_cwnd: float = MAX_CWND):
        self.channel = channel
        self.sock = sock
        self.dest_addr = dest_addr
        self.timeout = timeout
        self.max_retries = max_retries
        self.max_cwnd = max_cwnd

        self.cwnd = INITIAL_CWND
        self.ssthresh = INITIAL_SSTHRESH
        self.transmissions = 0
        self.retransmissions = 0
        self.loss_events = 0
        self.samples: list[CwndSample] = []

    def _record(self, event: str, t0: float):
        self.samples.append(CwndSample(t=time.monotonic() - t0, cwnd=self.cwnd, event=event))

    def _on_ack_growth(self):
        """Called once per packet cumulatively acknowledged."""
        if self.cwnd < self.ssthresh:
            self.cwnd += 1.0   # slow start: exponential (roughly +cwnd per RTT)
        else:
            self.cwnd += 1.0 / self.cwnd   # congestion avoidance: +1 per RTT
        self.cwnd = min(self.cwnd, self.max_cwnd)

    def _on_loss(self):
        """Called once per retransmission (a detected loss)."""
        self.ssthresh = max(self.cwnd / 2.0, 2.0)
        self.cwnd = self.ssthresh   # multiplicative decrease -> resume in congestion avoidance
        self.loss_events += 1

    async def send_all(self, payloads: list[bytes]):
        loop = asyncio.get_running_loop()
        t0 = time.monotonic()
        n = len(payloads)
        outstanding: dict[int, _Outstanding] = {}
        base = 0
        next_seq = 0
        self._highest_ack = -1
        self._ack_signal = asyncio.Event()
        recv_task = asyncio.create_task(self._recv_loop())

        async def send_one(seq):
            pkt = Packet(seq_num=seq, ack_num=0, flags=0, payload=payloads[seq])
            wire = encode(pkt)
            outstanding[seq] = _Outstanding(seq=seq, wire=wire, sent_at=loop.time())
            await self.channel.send(wire, self.dest_addr)
            self.transmissions += 1

        try:
            while base < n:
                window = max(1, int(self.cwnd))
                while next_seq < n and next_seq < base + window:
                    await send_one(next_seq)
                    next_seq += 1

                # NOTE: the retransmit-check loop below scans the FULL
                # [base, next_seq) range every iteration -- deliberately
                # uncapped by the current window, so that if cwnd shrinks
                # after a larger batch was already sent, every genuinely
                # outstanding packet is still checked for timeout. Capping
                # this range to "the current window plus a little slack"
                # is a real correctness hazard: any outstanding packet
                # outside that cap would never be retransmitted, potentially
                # stalling the transfer forever exactly like the bugs found
                # in Phases 2 and 3.
                earliest = min((o.sent_at for o in outstanding.values() if o.seq >= base),
                              default=None)
                remaining = (earliest + self.timeout - loop.time()) if earliest else self.timeout
                try:
                    await asyncio.wait_for(self._ack_signal.wait(), timeout=max(remaining, 0))
                    self._ack_signal.clear()
                except asyncio.TimeoutError:
                    pass

                while base < n and base in outstanding and self._highest_ack >= base:
                    del outstanding[base]
                    base += 1
                    self._on_ack_growth()
                    self._record("ack", t0)

                now = loop.time()
                for seq in range(base, next_seq):
                    o = outstanding.get(seq)
                    if o and now - o.sent_at >= self.timeout:
                        if o.retries >= self.max_retries:
                            raise GaveUpError(
                                f"seq {seq} not cumulatively ACKed after "
                                f"{self.max_retries} retries")
                        await self.channel.send(o.wire, self.dest_addr)
                        o.sent_at = now
                        o.retries += 1
                        self.transmissions += 1
                        self.retransmissions += 1
                        self._on_loss()
                        self._record("loss", t0)
        finally:
            recv_task.cancel()
            try:
                await recv_task
            except asyncio.CancelledError:
                pass

    async def _recv_loop(self):
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
            if pkt.has_flag(ACK_FLAG) and pkt.ack_num > self._highest_ack:
                self._highest_ack = pkt.ack_num
                self._ack_signal.set()


# ===========================================================================
# Demo: run a transfer, chart cwnd over time
# ===========================================================================
async def _run_congestion_controlled(n, drop_rate, timeout=0.05, max_cwnd=MAX_CWND):
    tx = socket.socket(socket.AF_INET, socket.SOCK_DGRAM); tx.bind(("127.0.0.1", 0)); tx.setblocking(False)
    rx = socket.socket(socket.AF_INET, socket.SOCK_DGRAM); rx.bind(("127.0.0.1", 0)); rx.setblocking(False)
    fwd = LossyChannel(tx, drop_rate=drop_rate)
    back = LossyChannel(rx, drop_rate=drop_rate)
    receiver = SlidingWindowReceiver(rx, back)
    sender = CongestionControlledSender(fwd, tx, rx.getsockname(), timeout=timeout, max_cwnd=max_cwnd)
    payloads = [f"pkt-{i}".encode() for i in range(n)]

    recv_task = asyncio.create_task(receiver.run_until(n))
    await sender.send_all(payloads)
    await recv_task

    assert receiver.delivered == payloads, "delivery correctness failed!"
    tx.close(); rx.close()
    return sender


async def _demo():
    N = 200
    DROP = 0.08
    print(f"Sending {N} packets with congestion control, {int(DROP*100)}% simulated loss...\n")

    sender = await _run_congestion_controlled(N, DROP, timeout=0.04, max_cwnd=40)

    print(f"  delivered:      {N}/{N}, in order")
    print(f"  transmissions:  {sender.transmissions} ({sender.retransmissions} retries)")
    print(f"  loss events:    {sender.loss_events} (each halved cwnd)")
    print(f"  cwnd samples:   {len(sender.samples)}")
    print(f"  final cwnd:     {sender.cwnd:.1f}  (started at {INITIAL_CWND})")

    print("\n  cwnd trajectory (sampled):")
    shown = sender.samples[::max(1, len(sender.samples)//25)]
    for s in shown:
        bar = "#" * min(int(s.cwnd), 60)
        marker = " <- LOSS" if s.event == "loss" else ""
        print(f"    t={s.t:5.2f}s  cwnd={s.cwnd:5.1f}  {bar}{marker}")

    try:
        import matplotlib
        matplotlib.use("Agg")
        import matplotlib.pyplot as plt
        xs = [s.t for s in sender.samples]
        ys = [s.cwnd for s in sender.samples]
        loss_xs = [s.t for s in sender.samples if s.event == "loss"]
        loss_ys = [s.cwnd for s in sender.samples if s.event == "loss"]
        fig, ax = plt.subplots(figsize=(8.5, 4.6))
        ax.plot(xs, ys, "-", color="#2E75B6", linewidth=1.5, label="cwnd")
        ax.scatter(loss_xs, loss_ys, color="#C0392B", s=25, zorder=5, label="loss (halved)")
        ax.set_xlabel("time (seconds)")
        ax.set_ylabel("congestion window (packets)")
        ax.set_title("Congestion window over time: slow start, then AIMD sawtooth")
        ax.grid(True, alpha=0.3)
        ax.legend()
        fig.tight_layout()
        fig.savefig("transport_cwnd_sawtooth.png", dpi=130)
        print("\n  wrote transport_cwnd_sawtooth.png")
    except ImportError:
        print("  (matplotlib not installed — skipped chart)")


if __name__ == "__main__":
    asyncio.run(_demo())
