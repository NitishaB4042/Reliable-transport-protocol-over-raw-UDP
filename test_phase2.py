"""
Tests for Reliable Transport Phase 2 — stop-and-wait reliable delivery.

Run: python test_phase2.py
"""

import socket
import random
import asyncio

from transport_phase1 import LossyChannel
from transport_phase2 import (StopAndWaitSender, StopAndWaitReceiver,
                              GaveUpError)


def _socket_pair():
    a = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
    a.bind(("127.0.0.1", 0)); a.setblocking(False)
    b = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
    b.bind(("127.0.0.1", 0)); b.setblocking(False)
    return a, b


async def _run_reliable_transfer(n, drop_rate=0.0, dup_rate=0.0, timeout=0.05,
                                 seed=0, both_directions=True):
    """Send n payloads reliably; return (delivered_payloads, sender)."""
    tx_sock, rx_sock = _socket_pair()
    rng_fwd = random.Random(seed)
    fwd_channel = LossyChannel(tx_sock, drop_rate=drop_rate, dup_rate=dup_rate, rng=rng_fwd)
    if both_directions:
        rng_back = random.Random(seed + 1)
        back_channel = LossyChannel(rx_sock, drop_rate=drop_rate, dup_rate=dup_rate, rng=rng_back)
    else:
        back_channel = LossyChannel(rx_sock)   # ACKs always arrive

    receiver = StopAndWaitReceiver(rx_sock, back_channel)
    sender = StopAndWaitSender(fwd_channel, tx_sock, rx_sock.getsockname(), timeout=timeout)

    recv_task = asyncio.create_task(receiver.run_until(n))
    for i in range(n):
        await sender.send(f"msg-{i}".encode())
    await recv_task

    tx_sock.close(); rx_sock.close()
    return receiver.delivered, sender


def test_reliable_delivery_data_loss_only():
    """Data direction lossy, ACK direction clean. Every payload must still
    arrive exactly once, in order."""
    async def go():
        delivered, sender = await _run_reliable_transfer(
            25, drop_rate=0.35, both_directions=False, seed=1)
        expected = [f"msg-{i}".encode() for i in range(25)]
        assert delivered == expected, "payloads missing, duplicated, or out of order"
        assert sender.stats().retransmissions > 0, "expected some retransmission under 35% loss"
    asyncio.run(go())
    print("  reliable_delivery_data_loss_only: PASS")


def test_reliable_delivery_loss_both_directions():
    """The harder case: both data AND acks can be lost/duplicated. Still must
    deliver everything exactly once, in order."""
    async def go():
        delivered, sender = await _run_reliable_transfer(
            25, drop_rate=0.3, dup_rate=0.15, both_directions=True, seed=2)
        expected = [f"msg-{i}".encode() for i in range(25)]
        assert delivered == expected, "payloads missing, duplicated, or out of order"
    asyncio.run(go())
    print("  reliable_delivery_loss_both_directions: PASS")


def test_no_retransmission_needed_on_clean_channel():
    """Sanity check: with zero loss, the sender should never need to retry —
    proves it doesn't retransmit unnecessarily on a channel that's fine."""
    async def go():
        delivered, sender = await _run_reliable_transfer(
            20, drop_rate=0.0, dup_rate=0.0, seed=3)
        stats = sender.stats()
        assert stats.retransmissions == 0, \
            f"expected zero retransmissions on a clean channel, got {stats.retransmissions}"
        assert stats.transmissions == 20
    asyncio.run(go())
    print("  no_retransmission_needed_on_clean_channel: PASS")


def test_duplicate_data_not_delivered_twice():
    """Even with heavy duplication (data arriving multiple times), each
    payload must be DELIVERED exactly once — duplicates are suppressed."""
    async def go():
        delivered, sender = await _run_reliable_transfer(
            20, drop_rate=0.0, dup_rate=0.6, both_directions=True, seed=4)
        expected = [f"msg-{i}".encode() for i in range(20)]
        assert delivered == expected, \
            f"duplicates leaked through: got {len(delivered)} payloads, expected {len(expected)}"
    asyncio.run(go())
    print("  duplicate_data_not_delivered_twice: PASS")


def test_gives_up_on_a_completely_dead_channel():
    """If nothing ever gets through, the sender must eventually raise
    GaveUpError rather than retry forever."""
    async def go():
        tx_sock, rx_sock = _socket_pair()
        dead_channel = LossyChannel(tx_sock, drop_rate=1.0)   # everything dropped
        sender = StopAndWaitSender(dead_channel, tx_sock, rx_sock.getsockname(),
                                   timeout=0.01, max_retries=5)
        try:
            await sender.send(b"anyone there?")
            assert False, "should have raised GaveUpError"
        except GaveUpError as e:
            assert "5" in str(e) or "seq" in str(e)
        tx_sock.close(); rx_sock.close()
    asyncio.run(go())
    print("  gives_up_on_a_completely_dead_channel: PASS")


def test_sequence_numbers_increment_correctly():
    async def go():
        tx_sock, rx_sock = _socket_pair()
        channel = LossyChannel(tx_sock)
        sender = StopAndWaitSender(channel, tx_sock, rx_sock.getsockname(), timeout=0.05)
        receiver = StopAndWaitReceiver(rx_sock, LossyChannel(rx_sock))
        recv_task = asyncio.create_task(receiver.run_until(5))
        seqs = [await sender.send(f"x{i}".encode()) for i in range(5)]
        await recv_task
        assert seqs == [0, 1, 2, 3, 4], seqs
        tx_sock.close(); rx_sock.close()
    asyncio.run(go())
    print("  sequence_numbers_increment_correctly: PASS")


if __name__ == "__main__":
    print("Running Reliable Transport Phase 2 tests:")
    test_reliable_delivery_data_loss_only()
    test_reliable_delivery_loss_both_directions()
    test_no_retransmission_needed_on_clean_channel()
    test_duplicate_data_not_delivered_twice()
    test_gives_up_on_a_completely_dead_channel()
    test_sequence_numbers_increment_correctly()
    print("All tests passed.")
