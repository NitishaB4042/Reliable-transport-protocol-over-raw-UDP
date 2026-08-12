"""
Tests for Reliable Transport Phase 5 — handshake/teardown + the full comparison.

Run: python test_phase5.py
"""

import socket
import asyncio

from transport_phase1 import LossyChannel
from transport_phase2 import GaveUpError
from transport_phase5 import (client_handshake, server_handshake,
                              sender_teardown, receiver_teardown,
                              Connection, _run_full_benchmark)


def _socket_pair():
    a = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
    a.bind(("127.0.0.1", 0)); a.setblocking(False)
    b = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
    b.bind(("127.0.0.1", 0)); b.setblocking(False)
    return a, b


# ---------------------------------------------------------------------------
# Handshake
# ---------------------------------------------------------------------------
def test_handshake_succeeds_zero_loss():
    async def go():
        tx, rx = _socket_pair()
        fwd, back = LossyChannel(tx), LossyChannel(rx)
        server_task = asyncio.create_task(server_handshake(rx, back))
        server_isn = await client_handshake(tx, fwd, rx.getsockname())
        addr, client_isn = await server_task
        assert server_isn is not None and client_isn == 0
        tx.close(); rx.close()
    asyncio.run(go())
    print("  handshake_succeeds_zero_loss: PASS")


def test_handshake_succeeds_under_loss():
    async def go():
        tx, rx = _socket_pair()
        fwd = LossyChannel(tx, drop_rate=0.25)
        back = LossyChannel(rx, drop_rate=0.25)
        server_task = asyncio.create_task(server_handshake(rx, back))
        server_isn = await asyncio.wait_for(client_handshake(tx, fwd, rx.getsockname()), timeout=10)
        addr, client_isn = await asyncio.wait_for(server_task, timeout=10)
        assert server_isn is not None
        tx.close(); rx.close()
    asyncio.run(go())
    print("  handshake_succeeds_under_loss: PASS")


# ---------------------------------------------------------------------------
# Teardown
# ---------------------------------------------------------------------------
def test_teardown_succeeds_under_loss():
    async def go():
        tx, rx = _socket_pair()
        fwd = LossyChannel(tx, drop_rate=0.25)
        back = LossyChannel(rx, drop_rate=0.25)
        recv_task = asyncio.create_task(receiver_teardown(rx, back))
        await asyncio.wait_for(sender_teardown(tx, fwd, rx.getsockname()), timeout=10)
        await asyncio.wait_for(recv_task, timeout=10)   # must not hang or raise
        tx.close(); rx.close()
    asyncio.run(go())
    print("  teardown_succeeds_under_loss: PASS")


# ---------------------------------------------------------------------------
# Full connection lifecycle
# ---------------------------------------------------------------------------
async def _full_connection(n, drop_rate, timeout=0.1, max_retries=25):
    tx, rx = _socket_pair()
    fwd = LossyChannel(tx, drop_rate=drop_rate)
    back = LossyChannel(rx, drop_rate=drop_rate)
    client = Connection(tx, fwd, timeout=timeout, max_retries=max_retries)
    server = Connection(rx, back, timeout=timeout, max_retries=max_retries)
    payloads = [f"msg-{i}".encode() for i in range(n)]
    server_task = asyncio.create_task(server.accept_and_receive(n))
    await client.connect_and_send(rx.getsockname(), payloads)
    receiver = await server_task
    tx.close(); rx.close()
    return receiver.delivered == payloads


def test_full_connection_zero_loss():
    async def go():
        ok = await asyncio.wait_for(_full_connection(20, 0.0), timeout=10)
        assert ok
    asyncio.run(go())
    print("  full_connection_zero_loss: PASS")


def test_full_connection_under_loss():
    async def go():
        ok = await asyncio.wait_for(_full_connection(25, 0.2), timeout=15)
        assert ok
    asyncio.run(go())
    print("  full_connection_under_loss: PASS")


def test_repeated_connections_are_robust():
    """The core lesson of this phase: handshake and teardown involve several
    moving parts that can each independently race against each other. A
    single clean run doesn't prove much -- this runs several real, unseeded,
    lossy end-to-end connections (handshake + data + teardown) back to back,
    as a permanent guard against the exact class of intermittent bug that
    took multiple rounds of debugging to find and fix while building this
    phase (a SYN-ACK/data-ACK collision, a swallowed FIN, and mismatched
    patience windows between waiting and retrying sides)."""
    async def go():
        for i in range(10):
            ok = await asyncio.wait_for(_full_connection(20, 0.25), timeout=15)
            assert ok, f"run {i} failed"
    asyncio.run(go())
    print("  repeated_connections_are_robust (10 unseeded runs, 25% loss): PASS")


# ---------------------------------------------------------------------------
# Benchmark sanity
# ---------------------------------------------------------------------------
def test_benchmark_produces_sane_results():
    async def go():
        loss_rates, results = await asyncio.wait_for(
            _run_full_benchmark(n=20, loss_rates=(0.0, 0.15), trials=1), timeout=30)
        assert len(loss_rates) == 2
        for name in ("stop_and_wait", "sliding_window", "congestion_controlled"):
            assert len(results[name]) == 2
            assert all(v > 0 for v in results[name]), f"{name}: non-positive throughput"
            # throughput should drop as loss increases, for every strategy
            assert results[name][1] < results[name][0], \
                f"{name}: throughput didn't drop under more loss ({results[name]})"
        # at zero loss, both windowed strategies should clearly beat stop-and-wait
        assert results["sliding_window"][0] > results["stop_and_wait"][0] * 2
        assert results["congestion_controlled"][0] > results["stop_and_wait"][0] * 2
    asyncio.run(go())
    print("  benchmark_produces_sane_results: PASS")


if __name__ == "__main__":
    print("Running Reliable Transport Phase 5 tests:")
    test_handshake_succeeds_zero_loss()
    test_handshake_succeeds_under_loss()
    test_teardown_succeeds_under_loss()
    test_full_connection_zero_loss()
    test_full_connection_under_loss()
    test_repeated_connections_are_robust()
    test_benchmark_produces_sane_results()
    print("All tests passed.")
