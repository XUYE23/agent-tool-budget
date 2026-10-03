# Agent Tool Budget

[![Tests](https://github.com/XUYE23/agent-tool-budget/actions/workflows/ci.yml/badge.svg)](https://github.com/XUYE23/agent-tool-budget/actions/workflows/ci.yml)

**Give every agent run a finite tool budget.**

A dependency-free async execution layer with atomic cost reservation, explicit
retry semantics, read-only request coalescing, and payload-free event traces.

[简体中文](README.zh-CN.md) · [Semantics](docs/semantics.md)

## Quick start

```sh
git clone https://github.com/XUYE23/agent-tool-budget.git
cd agent-tool-budget
python -m pip install .
agent-tool-budget demo --output outputs/demo.json
python -m unittest discover -s tests -v
```

The demo issues nine identical reads and two identical writes, then attempts
a write over budget. Read coalescing/cache reduces physical reads to one;
both allowed writes execute. Tools are synthetic; these counts demonstrate
scheduler behavior without claiming model latency or cloud billing improvements.

```python
import asyncio
from agent_tool_budget import Budget, Session, Tool

async def search(query):
    await asyncio.sleep(.01)
    return {"query": query, "hits": ["docs"]}

async def main():
    async with Session(
        [Tool("search", search, units=2, read_only=True)],
        budget=Budget(max_calls=6, max_units=12, max_requests=20),
    ) as run:
        results = await asyncio.gather(
            run.call("search", query="setup"),
            run.call("search", query="setup"),
        )
        print(results, run.report())

asyncio.run(main())
```

## Controls

| Control | Meaning |
|---|---|
| max_calls | Physical attempts, including failures/retries |
| max_units | Integer cost estimate reserved before each attempt |
| max_requests | All logical requests, including cache/coalescing |
| deadline_s | Whole-session wall clock, including queue/backoff |
| attempt_timeout_s | One async execution timeout |
| concurrency | Maximum simultaneous physical attempts |
| read_only | Opt in to same-run cache, coalescing, and retries |
| idempotent | Opt in to retries for a safe repeatable write |

Costs are caller-defined units; this alpha does not meter model tokens or actual
provider invoices. Use conservative estimates. Sessions belong to one invocation
on one event loop; create separate sessions for different users/runs.

Only TransientToolError retries by default. Unknown errors and timeouts propagate.
Writes default to one execution per request. Mark idempotent only when the tool
implementation enforces that contract, e.g. a durable idempotency key.

## Integration

Call run.call at your existing tool dispatch boundary. The
examples/framework_adapter.py adapter preserves tool call IDs and turns exhausted
budgets into a structured outcome you can route to a terminal graph node.
No model client or agent framework is replaced.

[LangGraph fault tolerance](https://github.com/langchain-ai/docs/blob/main/src/oss/langgraph/fault-tolerance.mdx)
provides framework retry/timeout policies. This library adds a small independent
per-run budget and read-request coalescing boundary. Avoid stacked retries that
hide underlying physical attempts: wrap the actual I/O operation or account
for its internal retry multiplier.

## Limits

Async Python cancellation is cooperative. A remote operation can finish after
local cancellation; this package cannot roll it back. Async tools must yield
control and propagate cancellation. Blocking work needs a separately managed
worker with its own stop semantics. Cache results can become stale until the TTL
expires; enable read_only only when same-run reuse is valid. No distributed
budget store, persistent cache, or direct LLM billing meter is implemented.

Tests cover concurrent overspend, retries, cancellation, duplicate reads/writes,
timeouts, payload isolation, and cached-loop termination. See docs/semantics.md.

MIT licensed. Initial implementation developed with AI assistance.

## Recorded local validation

15 tests passed after installation on Windows / Python 3.12.14.
[Validation record](docs/validation.json) · [Demo result](docs/demo-result.json)

The synthetic demo records 12 requests, 3 physical attempts, and 8 reserved units. Nine repeated reads produce one physical read; two allowed writes execute; the next write is blocked by the unit budget.
