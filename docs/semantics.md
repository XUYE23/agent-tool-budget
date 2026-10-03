# Execution semantics

- The request counter increments once a valid logical request reaches the run.
  Cache hits and coalesced waiters consume requests. Invalid tool names or
  non-JSON arguments are rejected before accounting.
- Physical call and unit reservation happen after acquiring a concurrency slot.
  There is no await between checking and reserving. This prevents overspend on
  one event loop. Multiple processes require a different shared accounting store.
- Failed attempts retain their reservation. A retry reserves again. Units are
  explicit fixed estimates attached to each tool; actual billing may differ.
- Retry backoff releases the execution slot. The entire worker, including queue
  time and backoff, remains bounded by the run deadline.
- Only read_only tools coalesce by canonical tool-name/JSON-arguments key.
  Cached data stays within a Session and is deep-copied on access.
- Cancellation of one waiter preserves the shared worker while another waiter
  remains. Cancellation of the final waiter cancels and drains the worker.
  close() cancels outstanding work and clears cached results.
- Cancellation requires cooperative tools. Swallowing CancelledError, blocking
  the event loop, spawning unmanaged work, or provider-side asynchronous jobs
  can violate wall-clock expectations. There is no rollback promise.
- Functions must be async; arguments/results must be JSON serializable.
  Avoid depending on Python object identity through the serialization boundary.
- Trace records contain tool names, event types, counters and elapsed times.
  Arguments, results and exception messages are omitted. Tool names themselves
  should not contain secrets.
- Create Session close to invocation because its wall clock begins at construction.
  Do not reuse it across users, runs or event loops.

The adapter is framework-neutral. Provider-native and LangGraph integration
snippets require validation against the specific dependency versions you deploy.
No upstream framework integration is claimed as tested by the offline suite.
