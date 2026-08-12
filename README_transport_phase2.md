# Reliable Transport Protocol — Phase 2

Stop-and-wait reliable delivery: send one packet, wait for its ACK,
retransmit on timeout. The simplest possible reliable scheme, and the
foundation the rest of the project builds speed on top of.

## Files
- `transport_phase2.py` — `StopAndWaitSender`, `StopAndWaitReceiver`, a demo
- `test_phase2.py` — tests

## How it works
```python
sender = StopAndWaitSender(channel, sock, dest_addr, timeout=0.2)
seq = await sender.send(b"hello")   # blocks until ACKed, retries on timeout
```
Send → wait up to `timeout` for the matching ACK → if it doesn't come,
retransmit the *same* packet → repeat until ACKed or `max_retries` is
exceeded (raises `GaveUpError`). The receiver ACKs every packet it sees —
including duplicates — because if the sender's copy of an earlier ACK was
lost, only a fresh ACK will make it stop retrying.

## The two correctness traps this phase has to avoid
1. **A duplicate arrival must not be delivered twice.** If an ACK is lost,
   the sender retransmits data the receiver already has. The receiver tracks
   `expected_seq` and only delivers a packet whose `seq_num` matches it —
   anything lower is a duplicate of something already delivered, so it's
   ACKed again (to finally satisfy the sender) but not re-delivered.
2. **A stale ACK must not be mistaken for the current one.** `_wait_for_ack`
   checks the ACK's `ack_num` against the sequence number it's actually
   waiting for and ignores anything else, so an old duplicate ACK arriving
   late can't be misread as acknowledging the packet just sent.

## Run
```bash
python transport_phase2.py   # 30 packets over 30% drop + 10% dup, both directions
python test_phase2.py        # tests
```

Demo output shows every payload arriving exactly once, in order, even under
harsh bidirectional loss — and the ~45ms/packet cost of one full round trip
per packet, which is exactly the problem Phase 3's sliding window solves.

## Tests cover
- reliable delivery with data-direction-only loss
- reliable delivery with loss **and duplication in both directions** (data
  and ACKs) — the harder, more realistic case
- **zero unnecessary retransmission** on a clean channel (proves the sender
  isn't retrying when it doesn't need to)
- **duplicates are suppressed** — heavy duplication (60%) still delivers each
  payload exactly once
- **gives up correctly** on a completely dead channel (100% drop) rather than
  retrying forever
- sequence numbers increment correctly across a run

Verified stable across repeated runs, not just a single pass — stop-and-wait
involves real timing (sockets, timeouts), so flakiness would matter.

## Still deferred
- Sliding window / pipelining (fix the one-round-trip-per-packet cost) → **Phase 3**
- Congestion control (slow start + AIMD) → **Phase 4**
- Handshake/teardown + the full comparison benchmark → **Phase 5**

## Correction (found while building Phase 3)

While stress-testing Phase 3, an intermittent failure led back here: **this
phase had the same bug**, just at a lower, easier-to-miss rate (~15-30% under
harsh bidirectional loss, vs. Phase 3's occasional failures at similar
conditions). It wasn't caught by the original verification (3 clean runs of
the full suite) because it's a genuine probability, not a deterministic bug —
a few runs are exactly the sample size were it hides.

**The bug:** `StopAndWaitReceiver.run_until()` returned the instant it
delivered its `n_packets`-th payload, stopping the receiver from listening or
acking anything further. If that *last* ACK it sent happened to be dropped by
the channel, the sender would retransmit into silence forever — nothing was
left alive to send a fresh ACK.

**The fix:** the receiver now **lingers** for a bounded period after
finishing, continuing to re-ack anything that arrives (i.e., a retransmission
triggered by that lost final ACK). This is the same idea behind TCP's
`TIME_WAIT` state — staying reachable a little longer after "done" so a
straggling retransmission still gets acknowledged, rather than landing on
a receiver that's already gone. The linger duration is set to comfortably
exceed the sender's own worst-case give-up time (`max_retries × timeout`), so
the receiver never goes quiet while the sender might still legitimately be
retrying. Re-verified with 25 unseeded stress runs at harsh loss: 0 failures
(previously ~15-30% failed under the same conditions).

This is a real limitation of not having proper connection teardown yet —
which is exactly what **Phase 5** adds.
