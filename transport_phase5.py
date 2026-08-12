"""
Reliable Transport Protocol — Phase 5: handshake/teardown + the full comparison.

Two things finish the project:

  1. A real connection LIFECYCLE: a 3-way handshake (SYN / SYN-ACK / ACK)
     before data flows, and a FIN / FIN-ACK teardown after it's all delivered.
     This formalizes what Phases 2-4 stood in for with a "linger" period —
     instead of the receiver guessing how long to wait around after finishing,
     the sender explicitly says "I'm done" (FIN) and the connection isn't
     considered closed until that's acknowledged, retried like any other
     packet if lost.

  2. The full benchmark: stop-and-wait vs. sliding-window vs.
     congestion-controlled, all under the same loss rates, on one chart —
     showing concretely what each layer of complexity bought.

Design choice: control packets (SYN/FIN) and data-plane ACKs are told apart
strictly by FLAG COMBINATION, never by sequence number. This sidesteps any
risk of a handshake/teardown packet's sequence number colliding with a data
sequence number's namespace — a subtlety that would otherwise need careful
handling on its own.

Run the demo:   python transport_phase5.py
Run the tests:  python test_phase5.py
"""

import time
import socket
import asyncio

from transport_phase1 import (Packet, encode, decode, ChecksumError, LossyChannel,
                              FLAG_SYN, FLAG_ACK, FLAG_FIN)
from transport_phase2 import GaveUpError, StopAndWaitSender, StopAndWaitReceiver
from transport_phase3 import SlidingWindowSender, SlidingWindowReceiver, ACK_FLAG
from transport_phase4 import CongestionControlledSender

HANDSHAKE_TIMEOUT = 0.1
HANDSHAKE_RETRIES = 20


async def _recv(sock):
    while True:
        try:
            return sock.recvfrom(65536)
        except BlockingIOError:
            await asyncio.sleep(0.001)


async def _wait_for(sock, timeout, predicate):
    """Wait up to `timeout` for a packet matching `predicate`. Non-matching
    packets are discarded (not queued) -- fine for control-plane exchanges,
    which don't overlap in time with data transfer."""
    loop = asyncio.get_running_loop()
    deadline = loop.time() + timeout
    while True:
        remaining = deadline - loop.time()
        if remaining <= 0:
            raise asyncio.TimeoutError()
        try:
            data, addr = await asyncio.wait_for(_recv(sock), timeout=remaining)
        except asyncio.TimeoutError:
            raise
        try:
            pkt = decode(data)
        except (ChecksumError, ValueError):
            continue
        if predicate(pkt):
            return pkt, addr
        # else: irrelevant/stale packet -- ignore, keep waiting


# ===========================================================================
# Handshake
# ===========================================================================
async def client_handshake(sock, channel, dest_addr, timeout=HANDSHAKE_TIMEOUT,
                           max_retries=HANDSHAKE_RETRIES):
    """3-way handshake, client side. Returns the server's initial seq number."""
    client_isn = 0
    for attempt in range(max_retries + 1):
        syn = Packet(seq_num=client_isn, ack_num=0, flags=FLAG_SYN, payload=b"")
        await channel.send(encode(syn), dest_addr)
        try:
            synack, addr = await _wait_for(
                sock, timeout,
                lambda p: p.has_flag(FLAG_SYN) and p.has_flag(FLAG_ACK) and p.ack_num == client_isn)
        except asyncio.TimeoutError:
            continue

        server_isn = synack.seq_num
        # send the final ACK. If it's lost, the server will time out and
        # resend its SYN-ACK -- so stay alert for a duplicate SYN-ACK and
        # resend the final ACK in response, for at least as long as the
        # server might still be retrying (its own timeout * max_retries).
        # A shorter window here would let the client give up listening
        # before the server gives up retrying -- the same asymmetric-patience
        # bug already hit (and fixed) in Phase 2/3's receiver linger.
        final_ack = Packet(seq_num=client_isn + 1, ack_num=server_isn,
                           flags=FLAG_ACK, payload=b"")
        await channel.send(encode(final_ack), dest_addr)

        linger = timeout * max_retries * 1.3   # safety margin over the server's budget
        deadline = asyncio.get_running_loop().time() + linger
        while asyncio.get_running_loop().time() < deadline:
            remaining = deadline - asyncio.get_running_loop().time()
            try:
                dup_synack, _ = await _wait_for(
                    sock, remaining,
                    lambda p: p.has_flag(FLAG_SYN) and p.has_flag(FLAG_ACK) and p.ack_num == client_isn)
                await channel.send(encode(final_ack), dest_addr)   # resend the ack
            except asyncio.TimeoutError:
                break
        return server_isn
    raise GaveUpError(f"handshake (client) failed after {max_retries} SYN retries")


async def server_handshake(sock, channel, timeout=HANDSHAKE_TIMEOUT,
                           max_retries=HANDSHAKE_RETRIES):
    """3-way handshake, server side. Returns (client_addr, client_isn)."""
    syn, addr = await _wait_for(sock, 3600, lambda p: p.has_flag(FLAG_SYN) and not p.has_flag(FLAG_ACK))
    client_isn = syn.seq_num
    server_isn = 1000   # fixed for simplicity/determinism in tests; any value works

    for attempt in range(max_retries + 1):
        synack = Packet(seq_num=server_isn, ack_num=client_isn, flags=FLAG_SYN | FLAG_ACK, payload=b"")
        await channel.send(encode(synack), addr)
        try:
            final_ack, _ = await _wait_for(
                sock, timeout,
                lambda p: p.has_flag(FLAG_ACK) and not p.has_flag(FLAG_SYN) and p.ack_num == server_isn)
            return addr, client_isn
        except asyncio.TimeoutError:
            continue
    raise GaveUpError(f"handshake (server) failed after {max_retries} SYN-ACK retries")


# ===========================================================================
# Teardown
# ===========================================================================
async def sender_teardown(sock, channel, dest_addr, timeout=HANDSHAKE_TIMEOUT,
                          max_retries=HANDSHAKE_RETRIES):
    """Send FIN, wait for FIN-ACK, retry on timeout. Only returns once the
    peer has confirmed receipt -- the connection isn't 'closed' until then."""
    for attempt in range(max_retries + 1):
        fin = Packet(seq_num=0, ack_num=0, flags=FLAG_FIN, payload=b"")
        await channel.send(encode(fin), dest_addr)
        try:
            await _wait_for(sock, timeout,
                            lambda p: p.has_flag(FLAG_FIN) and p.has_flag(FLAG_ACK))
            return
        except asyncio.TimeoutError:
            continue
    raise GaveUpError(f"teardown (sender) failed after {max_retries} FIN retries")


async def receiver_teardown(sock, channel, timeout=HANDSHAKE_TIMEOUT,
                            sender_max_retries=HANDSHAKE_RETRIES):
    """Wait for FIN, ACK it, then linger -- re-acking any retransmitted FIN
    (in case that first FIN-ACK was itself lost) -- for at least as long as
    the sender might still be retrying its FIN, before truly closing. This is
    TCP's TIME_WAIT idea, sized correctly: it must outlast the sender's own
    timeout * max_retries budget, or the receiver can vanish while the sender
    is still legitimately retrying (the same class of bug already hit and
    fixed for the data-transfer phases in Phase 2/3)."""
    fin, addr = await _wait_for(sock, 3600, lambda p: p.has_flag(FLAG_FIN))
    finack = Packet(seq_num=0, ack_num=0, flags=FLAG_FIN | FLAG_ACK, payload=b"")
    await channel.send(encode(finack), addr)

    linger = timeout * sender_max_retries * 1.3   # safety margin over the sender's budget
    deadline = asyncio.get_running_loop().time() + linger
    while asyncio.get_running_loop().time() < deadline:
        remaining = deadline - asyncio.get_running_loop().time()
        try:
            dup_fin, _ = await _wait_for(sock, remaining, lambda p: p.has_flag(FLAG_FIN))
            await channel.send(encode(finack), addr)   # resend the fin-ack
        except asyncio.TimeoutError:
            break


# ===========================================================================
# A full connection: handshake -> congestion-controlled transfer -> teardown
# ===========================================================================
class Connection:
    """Ties the whole project together: a real lifecycle around reliable,
    congestion-controlled, pipelined delivery.

    `timeout` and `max_retries` are the ONE place that defines how patient
    the retrying side is, everywhere in the connection (handshake, data,
    teardown). Every "waiting side" component (a receiver's linger, a
    handshake's post-ACK listen window) derives its own patience FROM these
    same numbers, with a safety margin -- rather than picking separate
    constants in different places that can silently drift out of sync with
    each other. That drift is exactly what caused several intermittent
    failures while building this phase: a waiting side would give up before
    the retrying side did, simply because their patience windows didn't
    actually match despite looking individually reasonable.
    """

    def __init__(self, sock: socket.socket, channel: LossyChannel,
                timeout: float = 0.1, max_retries: int = 25):
        self.sock = sock
        self.channel = channel
        self.timeout = timeout
        self.max_retries = max_retries
        # the shared "how long might the other side still be retrying" budget,
        # with a safety margin -- every wait/linger below uses this same number
        self.patience = timeout * max_retries * 1.3

    async def connect_and_send(self, dest_addr, payloads: list[bytes]):
        await client_handshake(self.sock, self.channel, dest_addr,
                               timeout=self.timeout, max_retries=self.max_retries)
        sender = CongestionControlledSender(self.channel, self.sock, dest_addr,
                                            timeout=self.timeout, max_retries=self.max_retries)
        await sender.send_all(payloads)
        await sender_teardown(self.sock, self.channel, dest_addr,
                              timeout=self.timeout, max_retries=self.max_retries)
        return sender

    async def accept_and_receive(self, n_packets: int):
        await server_handshake(self.sock, self.channel,
                               timeout=self.timeout, max_retries=self.max_retries)
        receiver = SlidingWindowReceiver(self.sock, self.channel)
        # Deliver everything, but with NO data-phase linger of its own -- the
        # combined loop right below replaces it. Splitting "wait around for a
        # retransmitted data packet" and "wait around for a FIN" into two
        # SEQUENTIAL phases (each only recognizing one packet type) leaves a
        # gap no matter how each phase's duration is tuned: the sender might
        # legitimately send either kind of packet during what should be one
        # continuous window, and whichever phase isn't running at that exact
        # moment simply can't respond to it. One shared loop that recognizes
        # both packet types for the whole patience window has no such gap.
        await receiver.run_until(n_packets, linger=0)
        await self._post_completion_loop(receiver)
        return receiver

    async def _post_completion_loop(self, receiver: "SlidingWindowReceiver"):
        """After all data is delivered: for one shared patience window,
        re-ack any retransmitted data packet (a normal data ACK) AND
        acknowledge FIN (a FIN-ACK) -- whichever the sender still needs,
        for as long as it might still legitimately be retrying either one."""
        loop = asyncio.get_running_loop()
        finack = Packet(seq_num=0, ack_num=0, flags=FLAG_FIN | FLAG_ACK, payload=b"")
        deadline = loop.time() + self.patience
        while loop.time() < deadline:
            remaining = deadline - loop.time()
            try:
                data, addr = await asyncio.wait_for(_recv(self.sock), timeout=remaining)
            except asyncio.TimeoutError:
                break
            try:
                pkt = decode(data)
            except (ChecksumError, ValueError):
                continue

            if pkt.has_flag(FLAG_FIN):
                await self.channel.send(encode(finack), addr)
            elif not pkt.has_flag(FLAG_SYN):
                # a (re)transmitted data packet -- process and re-ack it
                # exactly like the main receive loop would
                if pkt.seq_num >= receiver.expected_seq:
                    receiver.buffer[pkt.seq_num] = pkt.payload
                while receiver.expected_seq in receiver.buffer:
                    receiver.delivered.append(receiver.buffer.pop(receiver.expected_seq))
                    receiver.expected_seq += 1
                if receiver.expected_seq > 0:
                    ack = Packet(seq_num=0, ack_num=receiver.expected_seq - 1,
                                flags=ACK_FLAG, payload=b"")
                    await self.channel.send(encode(ack), addr)


# ===========================================================================
# The full three-way benchmark
# ===========================================================================
async def _bench_stop_and_wait(n, drop_rate, timeout=0.05):
    tx = socket.socket(socket.AF_INET, socket.SOCK_DGRAM); tx.bind(("127.0.0.1", 0)); tx.setblocking(False)
    rx = socket.socket(socket.AF_INET, socket.SOCK_DGRAM); rx.bind(("127.0.0.1", 0)); rx.setblocking(False)
    fwd = LossyChannel(tx, drop_rate=drop_rate)
    back = LossyChannel(rx, drop_rate=drop_rate)
    receiver = StopAndWaitReceiver(rx, back)
    sender = StopAndWaitSender(fwd, tx, rx.getsockname(), timeout=timeout, max_retries=60)
    recv_task = asyncio.create_task(receiver.run_until(n))
    t0 = time.time()
    for i in range(n):
        await sender.send(f"p{i}".encode())
    elapsed = time.time() - t0
    await recv_task
    tx.close(); rx.close()
    return elapsed


async def _bench_sliding_window(n, drop_rate, timeout=0.05, window_size=8):
    tx = socket.socket(socket.AF_INET, socket.SOCK_DGRAM); tx.bind(("127.0.0.1", 0)); tx.setblocking(False)
    rx = socket.socket(socket.AF_INET, socket.SOCK_DGRAM); rx.bind(("127.0.0.1", 0)); rx.setblocking(False)
    fwd = LossyChannel(tx, drop_rate=drop_rate)
    back = LossyChannel(rx, drop_rate=drop_rate)
    receiver = SlidingWindowReceiver(rx, back)
    sender = SlidingWindowSender(fwd, tx, rx.getsockname(), window_size=window_size,
                                 timeout=timeout, max_retries=60)
    payloads = [f"p{i}".encode() for i in range(n)]
    recv_task = asyncio.create_task(receiver.run_until(n))
    t0 = time.time()
    await sender.send_all(payloads)
    elapsed = time.time() - t0
    await recv_task
    tx.close(); rx.close()
    return elapsed


async def _bench_congestion_controlled(n, drop_rate, timeout=0.05):
    tx = socket.socket(socket.AF_INET, socket.SOCK_DGRAM); tx.bind(("127.0.0.1", 0)); tx.setblocking(False)
    rx = socket.socket(socket.AF_INET, socket.SOCK_DGRAM); rx.bind(("127.0.0.1", 0)); rx.setblocking(False)
    fwd = LossyChannel(tx, drop_rate=drop_rate)
    back = LossyChannel(rx, drop_rate=drop_rate)
    receiver = SlidingWindowReceiver(rx, back)
    sender = CongestionControlledSender(fwd, tx, rx.getsockname(), timeout=timeout, max_retries=60)
    payloads = [f"p{i}".encode() for i in range(n)]
    recv_task = asyncio.create_task(receiver.run_until(n))
    t0 = time.time()
    await sender.send_all(payloads)
    elapsed = time.time() - t0
    await recv_task
    tx.close(); rx.close()
    return elapsed


async def _run_full_benchmark(n=50, loss_rates=(0.0, 0.1, 0.2, 0.3), trials=2):
    results = {"stop_and_wait": [], "sliding_window": [], "congestion_controlled": []}
    for rate in loss_rates:
        for name, fn in [("stop_and_wait", _bench_stop_and_wait),
                         ("sliding_window", _bench_sliding_window),
                         ("congestion_controlled", _bench_congestion_controlled)]:
            times = []
            for _ in range(trials):
                elapsed = await asyncio.wait_for(fn(n, rate), timeout=30)
                times.append(n / elapsed)   # throughput, pkts/sec
            results[name].append(sum(times) / len(times))
    return list(loss_rates), results


# ===========================================================================
# Demo
# ===========================================================================
async def _demo_handshake_and_teardown():
    print("Handshake + teardown, under 20% simulated loss (both directions):\n")
    tx = socket.socket(socket.AF_INET, socket.SOCK_DGRAM); tx.bind(("127.0.0.1", 0)); tx.setblocking(False)
    rx = socket.socket(socket.AF_INET, socket.SOCK_DGRAM); rx.bind(("127.0.0.1", 0)); rx.setblocking(False)
    fwd = LossyChannel(tx, drop_rate=0.2)
    back = LossyChannel(rx, drop_rate=0.2)

    client = Connection(tx, fwd)
    server = Connection(rx, back)
    payloads = [f"msg-{i}".encode() for i in range(25)]

    server_task = asyncio.create_task(server.accept_and_receive(len(payloads)))
    client_sender = await client.connect_and_send(rx.getsockname(), payloads)
    server_receiver = await server_task

    assert server_receiver.delivered == payloads
    print(f"  connection established (SYN/SYN-ACK/ACK), {len(payloads)} messages delivered "
         f"in order, connection torn down (FIN/FIN-ACK). ✓")
    tx.close(); rx.close()


async def _demo():
    await _demo_handshake_and_teardown()

    print("\nRunning the full comparison: stop-and-wait vs. sliding-window vs. "
         "congestion-controlled,\nacross several loss rates (this takes a bit)...\n")
    loss_rates, results = await _run_full_benchmark()

    print(f"  {'loss':>6} | {'stop&wait':>10} | {'sliding-window':>15} | {'cong-controlled':>16}   (pkts/sec)")
    print("  " + "-" * 62)
    for i, rate in enumerate(loss_rates):
        print(f"  {rate*100:>5.0f}% | {results['stop_and_wait'][i]:>10.1f} | "
             f"{results['sliding_window'][i]:>15.1f} | {results['congestion_controlled'][i]:>16.1f}")

    try:
        import matplotlib
        matplotlib.use("Agg")
        import matplotlib.pyplot as plt
        fig, ax = plt.subplots(figsize=(8, 4.8))
        x = [r * 100 for r in loss_rates]
        ax.plot(x, results["stop_and_wait"], "o-", color="#C0392B", label="stop-and-wait")
        ax.plot(x, results["sliding_window"], "s-", color="#B9770E", label="sliding window")
        ax.plot(x, results["congestion_controlled"], "^-", color="#2E75B6", label="congestion-controlled")
        ax.set_yscale("log")
        ax.set_xlabel("simulated loss rate (%)")
        ax.set_ylabel("throughput (packets/sec, log scale)")
        ax.set_title("What each layer of complexity bought: three strategies under real loss")
        ax.grid(True, alpha=0.3, which="both")
        ax.legend()
        fig.tight_layout()
        fig.savefig("transport_comparison.png", dpi=130)
        print("\n  wrote transport_comparison.png")
    except ImportError:
        print("  (matplotlib not installed — skipped chart)")


if __name__ == "__main__":
    asyncio.run(_demo())
