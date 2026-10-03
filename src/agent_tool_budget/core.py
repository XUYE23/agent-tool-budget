"""Single-event-loop, per-run budgets. Costs are integer caller-defined units."""
import asyncio
import copy
import hashlib
import inspect
import json
import math
import time
from collections import OrderedDict
from dataclasses import dataclass
from typing import Awaitable, Callable


class BudgetExceeded(RuntimeError):
    pass


class DeadlineExceeded(BudgetExceeded):
    pass


class TransientToolError(RuntimeError):
    """An explicitly retryable transient failure."""


@dataclass(frozen=True)
class Budget:
    max_calls: int = 20
    max_units: int = 100
    max_requests: int = 100
    deadline_s: float = 60.

    def __post_init__(self):
        for name in ("max_calls","max_units","max_requests"):
            if type(getattr(self,name)) is not int or getattr(self,name) < 0:
                raise ValueError(f"{name} must be a nonnegative integer")
        if not math.isfinite(self.deadline_s) or self.deadline_s <= 0:
            raise ValueError("deadline_s must be finite and positive")


@dataclass(frozen=True)
class Tool:
    name: str
    function: Callable[..., Awaitable]
    units: int = 1
    read_only: bool = False
    idempotent: bool = False

    def __post_init__(self):
        if not self.name or not isinstance(self.name,str):
            raise ValueError("tool name must be nonempty")
        if not inspect.iscoroutinefunction(self.function):
            raise ValueError("tools must be async functions")
        if type(self.units) is not int or self.units < 0:
            raise ValueError("units must be a nonnegative integer")
        if type(self.read_only) is not bool or type(self.idempotent) is not bool:
            raise ValueError("read_only/idempotent must be booleans")


class Session:
    """Create one Session per agent invocation; close it with async with.

    read_only permits caching/coalescing and retries.
    idempotent permits retries, without caching or coalescing.
    Other tools execute once per request. Each attempted execution consumes
    calls and units, including failures, timeouts and cancellations after start.
    Request budget also counts cache hits and coalesced requests to bound loops.
    """
    def __init__(self, tools, *, budget=None, concurrency=4, max_attempts=2,
                 attempt_timeout_s=10., backoff_s=.05, cache_ttl_s=30.,
                 cache_size=128, retry_on=(TransientToolError,)):
        if type(concurrency) is not int or concurrency < 1:
            raise ValueError("concurrency must be a positive integer")
        if type(max_attempts) is not int or max_attempts < 1:
            raise ValueError("max_attempts must be a positive integer")
        for name,value in (("attempt_timeout_s",attempt_timeout_s),
                           ("backoff_s",backoff_s),("cache_ttl_s",cache_ttl_s)):
            if not math.isfinite(value) or value < 0 or (name=="attempt_timeout_s" and value==0):
                raise ValueError(f"invalid {name}")
        if type(cache_size) is not int or cache_size < 0:
            raise ValueError("cache_size must be a nonnegative integer")
        if not isinstance(retry_on,tuple) or not all(isinstance(t,type) and issubclass(t,Exception) for t in retry_on):
            raise ValueError("retry_on must be a tuple of Exception classes")
        tool_list = list(tools)
        self.tools = {t.name:t for t in tool_list}
        if len(self.tools) != len(tool_list):
            raise ValueError("tool names must be unique")
        self.budget = budget or Budget()
        self.concurrency = asyncio.Semaphore(concurrency)
        self.max_attempts, self.attempt_timeout_s = max_attempts, attempt_timeout_s
        self.backoff_s, self.cache_ttl_s, self.cache_size = backoff_s, cache_ttl_s, cache_size
        self.retry_on = retry_on
        self.started = time.monotonic()
        self.calls = self.units = self.requests = 0
        self.events = []
        self.cache = OrderedDict()
        self.pending = {}
        self.tasks = set()
        self.waiters = {}
        self.closed = False
        self.loop = None

    def _event(self, kind, tool, **fields):
        self.events.append({"kind":kind,"tool":tool,"elapsed_s":round(time.monotonic()-self.started,6),
                            **fields})

    def _remaining(self):
        return self.budget.deadline_s-(time.monotonic()-self.started)

    async def __aenter__(self):
        if self.closed:
            raise RuntimeError("session is closed")
        return self

    async def __aexit__(self,*exc):
        await self.close()

    async def close(self):
        self.closed = True
        tasks = list(self.tasks)
        for task in tasks:
            task.cancel()
        if tasks:
            await asyncio.gather(*tasks,return_exceptions=True)
        self.tasks.clear()
        self.pending.clear()
        self.cache.clear()

    async def call(self, name, **arguments):
        if self.closed:
            raise RuntimeError("session is closed")
        loop = asyncio.get_running_loop()
        if self.loop is None:
            self.loop = loop
        elif self.loop is not loop:
            raise RuntimeError("session must remain on one event loop")
        if name not in self.tools:
            raise KeyError(f"unknown tool: {name}")
        canonical = json.dumps(arguments,sort_keys=True,separators=(",",":"),allow_nan=False)
        frozen_arguments = json.loads(canonical)
        tool = self.tools[name]
        key = (name,hashlib.sha256(canonical.encode()).hexdigest())
        if self._remaining() <= 0:
            self._event("blocked",name,reason="deadline")
            raise DeadlineExceeded("run deadline exceeded")
        if self.requests >= self.budget.max_requests:
            self._event("blocked",name,reason="requests")
            raise BudgetExceeded("request budget exceeded")
        self.requests += 1
        now = time.monotonic()
        if tool.read_only and key in self.cache:
            expiry,result = self.cache.pop(key)
            if expiry > now:
                self.cache[key] = (expiry,result)
                self._event("cache_hit",name)
                return copy.deepcopy(result)
        if tool.read_only and key in self.pending:
            task = self.pending[key]
            self._event("coalesced",name)
        else:
            task = asyncio.create_task(self._run(tool,frozen_arguments,key))
            self.tasks.add(task)
            if tool.read_only:
                self.pending[key] = task
        self.waiters[task] = self.waiters.get(task,0)+1
        try:
            return copy.deepcopy(await asyncio.shield(task))
        finally:
            self.waiters[task] -= 1
            if self.waiters[task] == 0:
                del self.waiters[task]
                if not task.done():
                    task.cancel()
                    await asyncio.gather(task,return_exceptions=True)

    async def _run(self,tool,arguments,key):
        try:
            remaining = self._remaining()
            if remaining <= 0:
                raise DeadlineExceeded("run deadline exceeded")
            try:
                result = await asyncio.wait_for(self._execute(tool,arguments),timeout=remaining)
            except asyncio.TimeoutError:
                if self._remaining() <= 0:
                    self._event("blocked",tool.name,reason="deadline")
                    raise DeadlineExceeded("run deadline exceeded") from None
                raise
            if tool.read_only and self.cache_size and self.cache_ttl_s:
                self.cache[key] = (time.monotonic()+self.cache_ttl_s,copy.deepcopy(result))
                while len(self.cache) > self.cache_size:
                    self.cache.popitem(last=False)
            return result
        finally:
            task = asyncio.current_task()
            self.tasks.discard(task)
            if self.pending.get(key) is task:
                self.pending.pop(key,None)

    async def _execute(self,tool,arguments):
        attempts = self.max_attempts if tool.read_only or tool.idempotent else 1
        for attempt in range(1,attempts+1):
            async with self.concurrency:
                # No await between checking and reserving: atomic on one event loop.
                if self.calls >= self.budget.max_calls or self.units+tool.units > self.budget.max_units:
                    self._event("blocked",tool.name,reason="calls_or_units")
                    raise BudgetExceeded("execution budget exceeded")
                if self._remaining() <= 0:
                    raise DeadlineExceeded("run deadline exceeded")
                self.calls += 1
                self.units += tool.units
                self._event("attempt",tool.name,attempt=attempt,units=tool.units)
                try:
                    result = await asyncio.wait_for(tool.function(**copy.deepcopy(arguments)),
                                                    timeout=self.attempt_timeout_s)
                    # Require JSON data; results are copied for cache/caller isolation.
                    json.dumps(result,allow_nan=False)
                    self._event("success",tool.name,attempt=attempt)
                    return result
                except asyncio.CancelledError:
                    self._event("cancelled",tool.name,attempt=attempt)
                    raise
                except Exception as exc:
                    self._event("error",tool.name,attempt=attempt,error_type=type(exc).__name__)
                    if attempt == attempts or not isinstance(exc,self.retry_on):
                        raise
            self._event("retry",tool.name,attempt=attempt)
            await asyncio.sleep(self.backoff_s*2**(attempt-1))

    def report(self):
        return {"schema_version":1,"requests":self.requests,"executed_calls":self.calls,
                "reserved_units":self.units,
                "limits":{"requests":self.budget.max_requests,"calls":self.budget.max_calls,
                          "units":self.budget.max_units,"deadline_s":self.budget.deadline_s},
                "events":copy.deepcopy(self.events)}
