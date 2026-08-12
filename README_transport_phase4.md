# Reliable Transport Protocol — Phase 4

Congestion control: an adaptive window that grows fast when the network is
healthy (slow start) and backs off cautiously when it isn't (AIMD), producing
TCP's classic sawtooth. This is the single most interview-loved piece of the
whole project.

## Files
- `transport_phase4.py` — `CongestionControlledSender`, a demo (reuses Phase
  3's `SlidingWindowReceiver` unchanged)
- `test_phase4.py` — tests, including a mandatory stress test

## How it works
```python
sender = CongestionControlledSender(channel, sock, dest_addr, timeout=0.2, max_cwnd=64)
await sender.send_all(payloads)   # window size adapts on its own
```
- **Slow start**: `cwnd` starts at 1 and grows by +1 per acknowledged packet
  — since there are roughly `cwnd` ACKs per round trip, this **roughly
  doubles** `cwnd` every RTT. Fast, exponential ramp-up.
- **Congestion avoidance**: once `cwnd` reaches `ssthresh`, growth switches to
  `+1/cwnd` per ACK — adding up to **+1 per RTT**. Slow, linear, cautious.
- **On loss** (a timeout-triggered retransmission): `ssthresh = cwnd / 2`,
  then `cwnd = ssthresh` — multiplicative decrease. Growth resumes from there
  in congestion avoidance, not back in slow start. Repeated
  climb-then-halve-then-climb **is** the sawtooth.

## The result: a real sawtooth, not a diagram
200 packets, 8% simulated loss. `delivered: 200/200, in order`, `48 loss
events`, cwnd trajectory climbing and halving repeatedly:
```
t=0.09s  cwnd=6.3  ######
t=0.13s  cwnd=2.5  ##      <- LOSS
t=0.22s  cwnd=7.6  #######
t=0.26s  cwnd=2.0  ##      <- LOSS
```
The chart (`transport_cwnd_sawtooth.png`) shows this over the full run — cwnd
ramping up, hitting a loss (red dot), dropping by half, ramping again.

## Run
```bash
pip install matplotlib
python transport_phase4.py   # sawtooth demo + chart
python test_phase4.py        # tests
```

## Tests cover
- correct delivery under loss (same guarantee as every prior phase)
- cwnd starts at the initial value and grows monotonically with zero loss
- cwnd never exceeds the configured cap
- **loss roughly halves cwnd** (checked directly against the AIMD math)
- **cwnd has a floor** — repeated losses can't drive it to zero
- **slow start grows faster than congestion avoidance**, checked directly:
  same starting cwnd, more growth per ACK below `ssthresh` than above it
- gives up correctly on a completely dead channel
- **a 15-run stress test with real, unseeded randomness** — mandatory, not
  optional; see below for why

## A design decision worth defending
The retransmission-check loop deliberately scans the **full** range of
outstanding packets every time, uncapped by the current window size. An
earlier draft (found already sitting in this environment while building this
phase) capped that scan to roughly "the current window plus a little slack" —
which looks like a reasonable optimization, but is a real correctness hazard:
if `cwnd` shrinks a lot right after a larger batch was already sent, packets
outside that cap would never be checked for timeout, and could stall forever
outside — precisely the class of bug already found twice in this project
(Phases 2 and 3). Rather than trust that draft, it was rebuilt without the
cap and stress-tested before being kept.

## Why the stress test is mandatory here, not optional
Both Phase 2 and Phase 3 shipped with real, timing-sensitive bugs that a
single clean test run never caught — they were genuine probabilities (a
lost final ACK, a receiver that stops listening too early), not deterministic
failures, and needed *repeated* runs with real randomness to surface. Given
that history, Phase 4 doesn't get to skip the same scrutiny just because it
passed once. Verified with 4 full-suite runs (60 stress iterations total),
all clean.

## Still deferred
- Handshake/teardown (SYN/SYN-ACK/ACK, FIN) so this behaves like a real
  connection with a lifecycle, replacing the "linger" stand-in with proper
  termination → **Phase 5**
- The final comparison benchmark: stop-and-wait vs. sliding-window vs.
  congestion-controlled, all under the same loss rates, one chart →
  **Phase 5** (the finale)
