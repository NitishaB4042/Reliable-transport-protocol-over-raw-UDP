"""
Tests for Reliable Transport Phase 4 — congestion control (slow start + AIMD).

Given this project's history (Phases 2 and 3 both had intermittent,
timing-sensitive bugs that only surfaced under repeated unseeded runs), this
suite includes a mandatory stress test, not just a single correctness pass.

Run: python test_phase4.py
"""

import asyncio
import socket
import time

from transport_phase1 import LossyChannel
from transport_phase2 import GaveUpError
from transport_phase3 import SlidingWindowReceiver
from transport_phase4 import CongestionControlledSender, INITIAL_CWND, MAX_CWND


def _socket_pair():
    a = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
    a.bind(("127.0.0.1", 0)); a.setblocking(False)
    b = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
    b.bind(("127.0.0.1", 0)); b.setblocking(False)
    return a, b


async def _run(n, drop_rate, timeout=0.04, max_cwnd=MAX_CWND, both_directions=True):
    tx, rx = _socket_pair()
    fwd = LossyChannel(tx, drop_rate=drop_rate)
    back = LossyChannel(rx, drop_rate=drop_rate if both_directions else 0.0)
    receiver = SlidingWindowReceiver(rx, back)
    sender = CongestionControlledSender(fwd, tx, rx.getsockname(), timeout=timeout, max_cwnd=max_cwnd)
    payloads = [f"pkt-{i}".encode() for i in range(n)]

    recv_task = asyncio.create_task(receiver.run_until(n))
    await sender.send_all(payloads)
    await recv_task

    tx.close(); rx.close()
    return receiver.delivered, sender, payloads


def test_correct_delivery_under_loss():
    async def go():
        delivered, sender, payloads = await _run(80, drop_rate=0.08)
        assert delivered == payloads, "payloads missing, duplicated, or out of order"
    asyncio.run(go())
    print("  correct_delivery_under_loss: PASS")


def test_cwnd_starts_small_and_grows():
    """Slow start: cwnd must begin at INITIAL_CWND and generally increase
    early on, when there's no loss to hold it back."""
    async def go():
        delivered, sender, payloads = await _run(60, drop_rate=0.0)
        assert sender.samples[0].cwnd >= INITIAL_CWND
        cwnds = [s.cwnd for s in sender.samples]
        assert cwnds[-1] > cwnds[0], "cwnd should have grown with no loss to stop it"
        assert all(b >= a - 1e-9 for a, b in zip(cwnds, cwnds[1:])), \
            "cwnd decreased despite zero configured loss"
    asyncio.run(go())
    print("  cwnd_starts_small_and_grows: PASS")


def test_cwnd_never_exceeds_max():
    async def go():
        cap = 10.0
        delivered, sender, payloads = await _run(150, drop_rate=0.0, max_cwnd=cap)
        assert all(s.cwnd <= cap + 1e-9 for s in sender.samples), \
            f"cwnd exceeded max_cwnd={cap}: peak {max(s.cwnd for s in sender.samples)}"
    asyncio.run(go())
    print("  cwnd_never_exceeds_max: PASS")


def test_loss_roughly_halves_cwnd():
    """Directly exercise the AIMD math: after _on_loss(), cwnd should be
    close to half what it was (not reset to 1, not left unchanged)."""
    async def go():
        tx, rx = _socket_pair()
        sender = CongestionControlledSender(LossyChannel(tx), tx, rx.getsockname())
        sender.cwnd = 20.0
        sender._on_loss()
        assert 9.0 <= sender.cwnd <= 11.0, f"expected ~10 after halving 20, got {sender.cwnd}"
        assert sender.ssthresh == sender.cwnd
        tx.close(); rx.close()
    asyncio.run(go())
    print("  loss_roughly_halves_cwnd: PASS")


def test_cwnd_has_a_floor():
    """Repeated losses shouldn't push cwnd to zero or negative — there's a
    floor (2.0) so the sender can always make forward progress."""
    async def go():
        tx, rx = _socket_pair()
        sender = CongestionControlledSender(LossyChannel(tx), tx, rx.getsockname())
        sender.cwnd = 3.0
        for _ in range(20):   # far more halvings than needed to hit the floor
            sender._on_loss()
        assert sender.cwnd >= 2.0, f"cwnd fell below its floor: {sender.cwnd}"
        tx.close(); rx.close()
    asyncio.run(go())
    print("  cwnd_has_a_floor: PASS")


def test_slow_start_grows_faster_than_congestion_avoidance():
    """The defining behavioural difference: growth per ACK in slow start
    (cwnd < ssthresh) must be larger than growth per ACK in congestion
    avoidance (cwnd >= ssthresh), given the same starting point."""
    async def go():
        tx, rx = _socket_pair()
        sender = CongestionControlledSender(LossyChannel(tx), tx, rx.getsockname())

        sender.cwnd, sender.ssthresh = 5.0, 100.0   # deep in slow start
        before = sender.cwnd
        sender._on_ack_growth()
        slow_start_growth = sender.cwnd - before

        sender.cwnd, sender.ssthresh = 5.0, 1.0     # past ssthresh -> congestion avoidance
        before = sender.cwnd
        sender._on_ack_growth()
        cong_avoid_growth = sender.cwnd - before

        assert slow_start_growth > cong_avoid_growth, \
            f"slow start grew {slow_start_growth}, congestion avoidance grew " \
            f"{cong_avoid_growth} -- slow start should grow faster"
        tx.close(); rx.close()
    asyncio.run(go())
    print("  slow_start_grows_faster_than_congestion_avoidance: PASS")


def test_gives_up_on_a_completely_dead_channel():
    async def go():
        tx, rx = _socket_pair()
        dead_channel = LossyChannel(tx, drop_rate=1.0)
        sender = CongestionControlledSender(dead_channel, tx, rx.getsockname(),
                                            timeout=0.01, max_retries=5)
        try:
            await sender.send_all([b"hello"])
            assert False, "should have raised GaveUpError"
        except GaveUpError:
            pass
        tx.close(); rx.close()
    asyncio.run(go())
    print("  gives_up_on_a_completely_dead_channel: PASS")


def test_stress_many_unseeded_runs():
    """The mandatory stress test. Both Phase 2 and Phase 3 had bugs that a
    single clean run never caught — only repeated runs with real, unseeded
    randomness surfaced them. This runs a real transfer under realistic loss
    many times and demands zero failures."""
    async def go():
        failures = []
        for i in range(15):
            try:
                delivered, sender, payloads = await asyncio.wait_for(
                    _run(70, drop_rate=0.1, timeout=0.04), timeout=10)
                if delivered != payloads:
                    failures.append(f"run {i}: delivery mismatch")
            except Exception as e:
                failures.append(f"run {i}: {e}")
        assert not failures, "stress test failures:\n" + "\n".join(failures)
    asyncio.run(go())
    print("  stress_many_unseeded_runs (15 runs): PASS")


if __name__ == "__main__":
    print("Running Reliable Transport Phase 4 tests:")
    test_correct_delivery_under_loss()
    test_cwnd_starts_small_and_grows()
    test_cwnd_never_exceeds_max()
    test_loss_roughly_halves_cwnd()
    test_cwnd_has_a_floor()
    test_slow_start_grows_faster_than_congestion_avoidance()
    test_gives_up_on_a_completely_dead_channel()
    test_stress_many_unseeded_runs()
    print("All tests passed.")
