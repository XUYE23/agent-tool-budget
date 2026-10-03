import asyncio
import unittest
from agent_tool_budget import Budget, BudgetExceeded, DeadlineExceeded, Session, Tool, TransientToolError


class SessionTest(unittest.IsolatedAsyncioTestCase):
    async def test_coalescing_and_copy_isolation(self):
        count = 0
        async def read(x):
            nonlocal count
            count += 1
            await asyncio.sleep(.01)
            return {"x":[x]}
        async with Session([Tool("read",read,read_only=True)]) as s:
            results = await asyncio.gather(*(s.call("read",x=1) for _ in range(10)))
            self.assertEqual(count,1)
            results[0]["x"].append(2)
            self.assertEqual((await s.call("read",x=1))["x"],[1])

    async def test_writes_are_never_coalesced(self):
        count = 0
        async def write():
            nonlocal count
            count += 1
            return count
        async with Session([Tool("w",write)]) as s:
            await asyncio.gather(*(s.call("w") for _ in range(5)))
            self.assertEqual(count,5)

    async def test_concurrent_budget_reservation(self):
        count = 0
        async def f():
            nonlocal count
            count += 1
            await asyncio.sleep(.001)
            return count
        async with Session([Tool("f",f,units=3)],budget=Budget(max_calls=10,max_units=7)) as s:
            r = await asyncio.gather(*(s.call("f") for _ in range(10)),return_exceptions=True)
            self.assertEqual(count,2)
            self.assertEqual(s.units,6)
            self.assertEqual(sum(isinstance(x,BudgetExceeded) for x in r),8)

    async def test_retries_charged(self):
        n = 0
        async def f():
            nonlocal n
            n += 1
            if n == 1:
                raise TransientToolError("retry")
            return "ok"
        async with Session([Tool("f",f,read_only=True)],backoff_s=0) as s:
            self.assertEqual(await s.call("f"),"ok")
            self.assertEqual(s.calls,2)

    async def test_write_failure_not_retried(self):
        async def f():
            raise TransientToolError()
        async with Session([Tool("f",f)],max_attempts=5) as s:
            with self.assertRaises(TransientToolError):
                await s.call("f")
            self.assertEqual(s.calls,1)

    async def test_request_budget_limits_cached_loops(self):
        async def f():
            return 1
        async with Session([Tool("f",f,read_only=True)],budget=Budget(max_requests=2)) as s:
            await s.call("f")
            await s.call("f")
            with self.assertRaises(BudgetExceeded):
                await s.call("f")
            self.assertEqual(s.calls,1)

    async def test_cancel_only_waiter_cancels_tool(self):
        started,stopped = asyncio.Event(),asyncio.Event()
        async def f():
            started.set()
            try:
                await asyncio.Event().wait()
            finally:
                stopped.set()
        async with Session([Tool("f",f,read_only=True)]) as s:
            task = asyncio.create_task(s.call("f"))
            await started.wait()
            task.cancel()
            with self.assertRaises(asyncio.CancelledError):
                await task
            self.assertTrue(stopped.is_set())
            self.assertEqual(len(s.tasks),0)

    async def test_cancel_one_coalesced_waiter_preserves_other(self):
        started,release = asyncio.Event(),asyncio.Event()
        async def f():
            started.set()
            await release.wait()
            return 7
        async with Session([Tool("f",f,read_only=True)]) as s:
            first = asyncio.create_task(s.call("f"))
            await started.wait()
            second = asyncio.create_task(s.call("f"))
            await asyncio.sleep(0)
            first.cancel()
            with self.assertRaises(asyncio.CancelledError):
                await first
            release.set()
            self.assertEqual(await second,7)
            self.assertEqual(s.calls,1)

    async def test_deadline_covers_waiting_and_execution(self):
        async def f():
            await asyncio.sleep(.2)
        async with Session([Tool("f",f)],budget=Budget(deadline_s=.02)) as s:
            with self.assertRaises(DeadlineExceeded):
                await s.call("f")
            self.assertEqual(s.calls,1)

    async def test_failure_not_cached(self):
        n = 0
        async def f():
            nonlocal n
            n += 1
            raise ValueError("invalid")
        async with Session([Tool("f",f,read_only=True)]) as s:
            for _ in range(2):
                with self.assertRaises(ValueError):
                    await s.call("f")
            self.assertEqual(n,2)

    async def test_retry_budget_exhaustion(self):
        async def f():
            raise TransientToolError()
        async with Session([Tool("f",f,read_only=True)],
                           budget=Budget(max_calls=1),backoff_s=0) as s:
            with self.assertRaises(BudgetExceeded):
                await s.call("f")
            self.assertEqual(s.calls,1)

    async def test_argument_order_coalesces(self):
        async def f(a,b):
            return a+b
        async with Session([Tool("f",f,read_only=True)]) as s:
            await s.call("f",a=1,b=2)
            await s.call("f",b=2,a=1)
            self.assertEqual(s.calls,1)

    async def test_timeout_is_not_retried_by_default(self):
        async def f():
            await asyncio.sleep(1)
        async with Session([Tool("f",f,read_only=True)],attempt_timeout_s=.01) as s:
            with self.assertRaises(asyncio.TimeoutError):
                await s.call("f")
            self.assertEqual(s.calls,1)

    async def test_trace_does_not_include_payloads(self):
        async def f(secret):
            return secret
        async with Session([Tool("f",f,read_only=True)]) as s:
            await s.call("f",secret="private-secret")
            self.assertNotIn("private-secret",str(s.report()))
        with self.assertRaises(RuntimeError):
            await s.call("f",secret="x")


if __name__ == "__main__":
    unittest.main()
