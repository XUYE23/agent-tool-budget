# Changelog

## 0.1.1 - 2026-10-03

Classify run deadlines from the actual task wait outcome, including timers that
fire early on coarse Windows clocks. Preserve per-tool timeout exceptions and
cancel/drain the child task on deadline or caller cancellation. Add a deterministic
regression test independent of clock precision.

## 0.1.0 - 2026-10-03

Initial alpha. See README for implemented behavior, reproducible demonstrations,
and current limitations.
