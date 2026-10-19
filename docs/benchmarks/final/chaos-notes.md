# Chaos runs: what each file is

`bench/harness/chaos.py --seconds 90 --seed N` on the final code, one JSON per run (`chaos-seed1..8.json`, plus
`chaos-seed5-repeat1..3.json`). Each run verifies every acknowledged write, every acknowledged delete, the rate-limit
bound, the TTL, one primary per shard and replica convergence. All pass except the two runs below, which are kept on
purpose.

* `chaos-seed5-FAILED-first-attempt.json` - the **first** run of seed 5 (before the lost-write fix, with a harness
  that did not yet record unknown-outcome deletes). It reported every bucket at 532 tokens and one replica 5,435
  keys short. Two causes, kept apart: (1) `elapsed_s` is **32,526 s**: the host was suspended overnight, and
  500 + 0.001 tokens/s x 32,527 s = 532.5, so the bucket count was exactly right; the harness check "never above
  500" was wrong for a run of that length and was corrected to capacity + rate x elapsed. (2) The replica
  divergence was real or at least consistent with the defect found next: a primary killed and restarted before the
  router promoted anyone could make its replicas resynchronise down to a shorter WAL (reproduced deterministically by
  `FailoverE2E.AnAcknowledgedWriteSurvivesAPrimaryThatIsKilledAndRestartedBeforeAnyPromotion`: 92 of 1,800
  acknowledged writes lost before the fix, 0 after).
* `chaos-seed5-repeat1.json` - `elapsed_s` is **4,979.7 s** for a 90 s run: the host was suspended for about 83
  minutes (the log timestamps jump from 14:14:51 to 15:37:43). All ten buckets were granted 504 tokens; capacity +
  rate x elapsed = 500 + 4.98 = 504.98, so the limiter behaved exactly as specified. It is a valid "the clock
  leapt" experiment, not a failure of the system; the harness now flags such runs (`host_suspended`) and applies
  the exact bound.
* One earlier run of seed 5 reported a single acknowledged key missing (`chaos:0:151`). The harness then did not
  record deletes whose reply was lost (outcome unknown: the key may well have been deleted); it now tracks them
  and excludes those keys. The nine later runs of seed 5 (the main series and three repeats, one of which is the
  suspended-host run) show no missing key, but I could not prove the cause of that single event after the fact
  (the key's history was not recorded). Recorded here as unexplained-but-probably-the-harness.
