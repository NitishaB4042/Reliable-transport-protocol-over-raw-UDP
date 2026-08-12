# Reliable Transport Protocol — Phase 1

Packet framing and the lossy-channel simulator. This phase deliberately does
**only** these two things, tested rigorously, before anything reliability-related
is built on top.

## Files
- `transport_phase1.py` — packet encode/decode + `LossyChannel` + a demo
- `test_phase1.py` — tests, verified from the **receiver's side**

## The packet format
```
seq_num (4B) | ack_num (4B) | flags (1B) | checksum (4B) | payload_len (2B) | payload
```
`flags` bits (SYN/ACK/FIN) are defined now even though they're only used from
Phase 5 onward, so the header format never has to change later. The checksum
(CRC32 over the payload) means corruption is *detected*, not silently accepted.

## Why a lossy-channel simulator
On localhost, real UDP essentially never drops or reorders packets on its
own — everything arrives instantly, in order. That means reliability logic
(Phase 2 onward) **cannot be tested** without deliberately breaking things.
`LossyChannel` wraps a UDP socket's send and independently, randomly drops,
duplicates, and delays (reorders) each outgoing packet at configurable rates.

```python
channel = LossyChannel(sock, drop_rate=0.3, dup_rate=0.1, reorder_rate=0.15)
await channel.send(encode(pkt), dest_addr)
```

## Run
```bash
python transport_phase1.py   # framing demo + a 1000-packet injection-rate demo
python test_phase1.py        # tests
```

## The tests prove the simulator is honest, not just self-consistent
Every failure-injection test checks what the **receiver actually got** — a
real second socket, reading real arrived datagrams — not the sender's own
internal counters. A simulator that only proves itself against its own
bookkeeping proves nothing; if the receiver genuinely sees ~30% of packets
missing, some sequence numbers arriving twice, and some arriving out of send
order, the simulator is trustworthy enough to build Phase 2+ on top of.

Tests cover: encode/decode round-trip, empty payload, checksum catching
corruption, truncated/undersized input rejected, bad config rejected, a
zero-failure channel behaving like plain reliable UDP, and — checked from the
receiver — that `drop_rate`, `dup_rate`, and `reorder_rate` each produce real,
measurable effects close to what was configured.

## A real bug the tests caught (worth knowing)
The first version of the drop-rate test sent 2000 packets in a tight loop,
then started draining. It failed — not because `LossyChannel` was wrong, but
because `asyncio.gather(send_all(), rx.drain(...))` doesn't force preemption
between coroutines that never actually suspend: with no reordering configured,
`LossyChannel.send()` has no real `await` inside it, so the entire 2000-packet
loop ran as one uninterrupted synchronous burst before the receiver's drain
task ever got a turn — overflowing the OS's UDP receive buffer and causing
extra loss that had nothing to do with the configured `drop_rate`.

The fix was two-fold: increase the receiver's `SO_RCVBUF`, and make the test's
sending loop `await asyncio.sleep(0)` periodically so draining genuinely
interleaves with sending — which also mirrors how a real sender should behave
(never blast thousands of packets with zero pacing). Both changes are in the
test harness, not the channel itself; `LossyChannel`'s logic was correct the
whole time, but the *test exercising it* wasn't honest about how asyncio
scheduling actually works.

## Still deferred
- Reliable delivery (ACKs, timeouts, retransmission) → **Phase 2**
- Sliding window / pipelining → **Phase 3**
- Congestion control (slow start + AIMD) → **Phase 4**
- Handshake/teardown + the full comparison benchmark → **Phase 5**
