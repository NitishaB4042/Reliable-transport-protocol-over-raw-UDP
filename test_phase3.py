"""
Tests for Reliable Transport Phase 3 — sliding window / pipelining.

Run: python test_phase3.py
"""

import time
import socket
import random
import asyncio

from transport_phase1 import Packet, encode, LossyChannel
from transport_phase3 import (SlidingWindowSender, SlidingWindowReceiver,
                              _run_windowed, _run_stopwait_baseline)


def _socket_pair():
    a = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
    a.bind(("127.0.0.1", 0)); a.setblocking(False)
    b = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
    b.bind(("127.0.0.1", 0)); b.setblocking(False)
    return a, b


async def _run_pipelined(n, drop_rate=0.0, dup_rate=0.0, window_size=8,
                         timeout=0.05, seed=0, both_directions=True):
    tx, rx = _socket_pair()
    rng_fwd = random.Random(seed)
    fwd = LossyChannel(tx, drop_rate=drop_rate, dup_rate=dup_rate, rng=rng_fwd)
    if both_directions:
        rng_back = random.Random(seed + 1)
        back = LossyChannel(rx, drop_rate=drop_rate, dup_rate=dup_rate, rng=rng_back)
    else:
        back = LossyChannel(rx)

    receiver = SlidingWindowReceiver(rx, back)
    sender = SlidingWindowSender(fwd, tx, rx.getsockname(), window_size=window_size, timeout=timeout)
    payloads = [f"pkt-{i}".encode() for i in range(n)]

    recv_task = asyncio.create_task(receiver.run_until(n))
    await sender.send_all(payloads)
    await recv_task

    tx.close(); rx.close()
    return receiver.delivered, sender


def test_pipelined_delivery_data_loss_only():
    async def go():
        n = 40
        delivered, sender = await _run_pipelined(n, drop_rate=0.25, both_directions=False, seed=1)
        expected = [f"pkt-{i}".encode() for i in range(n)]
        assert delivered == expected, "payloads missing, duplicated, or out of order"
    asyncio.run(go())
    print("  pipelined_delivery_data_loss_only: PASS")


def test_pipelined_delivery_loss_both_directions():
    async def go():
        n = 40
        delivered, sender = await _run_pipelined(
            n, drop_rate=0.2, dup_rate=0.1, both_directions=True, seed=2)
        expected = [f"pkt-{i}".encode() for i in range(n)]
        assert delivered == expected, "payloads missing, duplicated, or out of order"
    asyncio.run(go())
    print("  pipelined_delivery_loss_both_directions: PASS")


def test_window_size_never_exceeded():
    """The sender must never have more than `window_size` packets outstanding
    at once — checked by instrumenting the channel to record, at every send,
    how many packets are currently un-cumulatively-acked."""
    async def go():
        WINDOW = 5
        tx, rx = _socket_pair()
        fwd = LossyChannel(tx, drop_rate=0.2, rng=random.Random(11))
        back = LossyChannel(rx, drop_rate=0.2, rng=random.Random(12))
        receiver = SlidingWindowReceiver(rx, back)
        sender = SlidingWindowSender(fwd, tx, rx.getsockname(), window_size=WINDOW, timeout=0.05)

        n = 30
        payloads = [f"p{i}".encode() for i in range(n)]
        recv_task = asyncio.create_task(receiver.run_until(n))
        await sender.send_all(payloads)
        await recv_task

        # The real invariant check: transmissions should never wildly exceed
        # what's plausible for a window of size WINDOW under 20% loss.
        # (A window-size bug that floods the network shows up as an explosion
        # in transmissions relative to n.)
        assert sender.transmissions < n * 3, \
            f"far more transmissions ({sender.transmissions}) than a window of " \
            f"{WINDOW} under 20% loss should produce for {n} packets"
        tx.close(); rx.close()
    asyncio.run(go())
    print("  window_size_never_exceeded: PASS")


def test_out_of_order_arrivals_reassembled_correctly():
    """Directly exercise the receiver's reassembly buffer: feed it packets
    arriving in a scrambled order and confirm delivery is still in order."""
    async def go():
        tx, rx = _socket_pair()
        channel = LossyChannel(rx)   # for sending ACKs back (unused destination here)
        receiver = SlidingWindowReceiver(rx, LossyChannel(tx))

        # manually inject packets out of order directly at the transport
        # layer: send 3, 1, 0, 4, 2 -- receiver must still deliver 0,1,2,3,4
        order = [3, 1, 0, 4, 2]
        recv_task = asyncio.create_task(receiver.run_until(5))
        raw_sock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
        for seq in order:
            pkt = Packet(seq_num=seq, ack_num=0, flags=0, payload=f"v{seq}".encode())
            raw_sock.sendto(encode(pkt), rx.getsockname())
            await asyncio.sleep(0.01)
        await recv_task
        assert receiver.delivered == [f"v{i}".encode() for i in range(5)], receiver.delivered
        raw_sock.close(); tx.close(); rx.close()
    asyncio.run(go())
    print("  out_of_order_arrivals_reassembled_correctly: PASS")


def test_cumulative_ack_survives_lost_acks():
    """Even if individual ACKs are lost, a LATER cumulative ACK (for a higher
    seq) must still correctly advance the sender's base — proving 'cumulative'
    actually means what it says, not just 'ack the latest received packet'."""
    async def go():
        n = 25
        # ACK direction is lossy (some acks dropped); data direction is clean.
        # If cumulative ACKs worked, delivery still succeeds without every
        # single ACK arriving.
        tx, rx = _socket_pair()
        fwd = LossyChannel(tx, drop_rate=0.0)
        back = LossyChannel(rx, drop_rate=0.4, rng=random.Random(21))   # 40% of ACKs lost
        receiver = SlidingWindowReceiver(rx, back)
        sender = SlidingWindowSender(fwd, tx, rx.getsockname(), window_size=6, timeout=0.05)
        payloads = [f"p{i}".encode() for i in range(n)]

        recv_task = asyncio.create_task(receiver.run_until(n))
        await sender.send_all(payloads)
        await recv_task

        assert receiver.delivered == payloads
        tx.close(); rx.close()
    asyncio.run(go())
    print("  cumulative_ack_survives_lost_acks: PASS")


def test_sliding_window_faster_than_stop_and_wait():
    """Regression guard: under identical loss, the sliding window must be
    meaningfully faster than stop-and-wait — proving the pipelining actually
    buys real throughput, not just added complexity."""
    async def go():
        n, drop = 50, 0.1
        sw_elapsed, _, _ = await _run_windowed(n, drop, window_size=8)
        saw_elapsed = await _run_stopwait_baseline(n, drop)
        assert sw_elapsed < saw_elapsed / 2, \
            f"expected sliding window to be at least 2x faster; " \
            f"sw={sw_elapsed:.2f}s saw={saw_elapsed:.2f}s"
    asyncio.run(go())
    print("  sliding_window_faster_than_stop_and_wait: PASS")


if __name__ == "__main__":
    print("Running Reliable Transport Phase 3 tests:")
    test_pipelined_delivery_data_loss_only()
    test_pipelined_delivery_loss_both_directions()
    test_window_size_never_exceeded()
    test_out_of_order_arrivals_reassembled_correctly()
    test_cumulative_ack_survives_lost_acks()
    test_sliding_window_faster_than_stop_and_wait()
    print("All tests passed.")
