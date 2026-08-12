# Reliable Transport Protocol — Phase 3

Sliding window / pipelining: up to `window_size` packets in flight at once,
cumulative ACKs, and a receiver-side reassembly buffer for out-of-order
arrivals. This is where stop-and-wait's one-round-trip-per-packet cost
actually gets fixed — with a measured throughput number to prove it.

## Files
- `transport_phase3.py` — `SlidingWindowSender`, `SlidingWindowReceiver`, a demo
- `test_phase3.py` — tests

## How it works
```python
sender = SlidingWindowSender(channel, sock, dest_addr, window_size=8, timeout=0.2)
await sender.send_all([b"payload1", b"payload2", ...])   # pipelined, not one at a time
```
The sender keeps up to `window_size` packets outstanding, refilling the
window as ACKs arrive. ACKs are **cumulative** — an ACK for seq N means "I
have everything through N, in order" — so one ACK can advance the window past
several packets at once, and losing an individual ACK doesn't stall anything
as long as a later cumulative one gets through. The receiver buffers
out-of-order arrivals and delivers them in order as gaps fill.

## The measured result
```
sliding window (size 8): 0.16s  (380 pkts/sec)
stop-and-wait (Phase 2): 1.09s  (55 pkts/sec)
speedup: 6.9x
```
Same packet count, same simulated loss rate, both directions lossy — the only
difference is pipelining. (Run-to-run speedup varies with real randomness —
seen anywhere from ~7x to ~18x — since both sides use live, unseeded sockets.)

## Run
```bash
python transport_phase3.py   # throughput comparison vs. Phase 2
python test_phase3.py        # tests
```

## Tests cover
- pipelined delivery under data-only and bidirectional loss/duplication
- the **window-size invariant** (transmissions stay bounded relative to n —
  a window bug that floods the network would blow this up)
- **out-of-order reassembly**, directly: packets injected in scrambled order
  (3,1,0,4,2) are still delivered 0,1,2,3,4
- **cumulative ACKs survive lost individual ACKs** — 40% of ACKs dropped,
  delivery still succeeds, because a later cumulative ACK still advances the
  window past everything it covers
- a **regression guard**: sliding window must be at least 2x faster than
  stop-and-wait under identical loss, or the test fails

## Two real bugs this phase's development caught (worth knowing)

**1. A crash from an unsigned header field.** When the receiver gets an
out-of-order packet *before* packet 0 has arrived, `expected_seq` is still 0,
so the cumulative-ack calculation (`expected_seq - 1`) produced **-1**.
Packing -1 into the header's unsigned 32-bit `ack_num` field raises
`struct.error`. Because nothing was watching the receiver's task while the
sender looped waiting for an ACK, the crash was **silent** — the sender just
retransmitted forever into a receiver that had already died. Fixed by not
sending a cumulative ACK until something has actually been delivered in order
(nothing meaningful to acknowledge before then anyway).

**2. The same "receiver stops listening too early" bug found in Phase 2**
(see that phase's README for the full writeup) — intermittent here too, at a
similar root cause and the same fix: the receiver lingers after finishing,
sized to comfortably outlast the sender's own give-up time. Re-verified with
25 unseeded stress runs under real loss: 0 failures.

Neither of these was caught by a single clean test run — both needed
*repeated* runs under real, unseeded randomness to surface, because they're
genuine probabilities, not deterministic bugs. That's the actual argument for
stress-testing concurrent/networked code beyond "it passed once."

## Still deferred
- Congestion control (slow start + AIMD) → **Phase 4**
- Handshake/teardown + the full comparison benchmark → **Phase 5** (this is
  also where real connection lifecycle replaces the "linger" stand-in above)
