"""任务调度 - 修复后的全量测试（15 个）。"""
import asyncio
import sys
import os
import time

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from task.scheduler import TaskScheduler, Task, RetryableError, NonRetryableError


class TestDependencyCheck:
    def test_blocked_by_dependency(self):
        ts = TaskScheduler()
        ts.schedule_task("dep", lambda: None)
        ts.schedule_task("main", lambda: None, deps=["dep"])
        assert ts.resolve_dependencies("main") is False
        assert ts.execute_task("main") is False
        ts.execute_task("dep")
        assert ts.resolve_dependencies("main") is True

    def test_transitive_dependency(self):
        ts = TaskScheduler()
        ts.schedule_task("c", lambda: None)
        ts.schedule_task("b", lambda: None, deps=["c"])
        ts.schedule_task("a", lambda: None, deps=["b"])
        assert ts.resolve_dependencies("a") is False
        ts.execute_task("c")
        assert ts.resolve_dependencies("a") is False
        ts.execute_task("b")
        assert ts.resolve_dependencies("a") is True

    def test_dependency_completion_notifies(self):
        ts = TaskScheduler()
        order = []
        ts.schedule_task("dep", lambda: order.append("dep"))
        ts.schedule_task("main", lambda: order.append("main"), deps=["dep"])
        ts.start()
        assert ts.wait_idle(timeout=2) is True
        ts.shutdown()
        assert order == ["dep", "main"]


class TestDuplicateSchedule:
    def test_duplicate_task_rejected(self):
        ts = TaskScheduler()
        assert ts.schedule_task("t1", lambda: None) is True
        assert ts.schedule_task("t1", lambda: None) is False
        assert len(ts.queue) == 1


class TestPriority:
    def test_priority_ordering(self):
        ts = TaskScheduler()
        order = []
        ts.schedule_task("low", lambda: order.append("low"), priority=0)
        ts.schedule_task("high", lambda: order.append("high"), priority=10)
        ts.schedule_task("mid", lambda: order.append("mid"), priority=5)
        ts.run()
        assert order == ["high", "mid", "low"]

    def test_blocked_high_priority_yields_to_ready(self):
        ts = TaskScheduler()
        order = []
        ts.schedule_task("dep", lambda: order.append("dep"))
        ts.schedule_task("blocked", lambda: order.append("blocked"),
                         priority=100, deps=["dep"])
        ts.schedule_task("ready", lambda: order.append("ready"), priority=1)
        assert ts.next_task().tid == "ready"
        ts.run()
        assert order == ["ready", "dep", "blocked"]


class TestExceptionHandling:
    def test_exception_does_not_crash(self):
        ts = TaskScheduler()

        def fail():
            raise ValueError("test")

        ts.schedule_task("t1", fail)
        result = ts.execute_task("t1")
        assert result is False
        assert ts.tasks["t1"].status == "failed"
        assert isinstance(ts.tasks["t1"].error, ValueError)
        ts.schedule_task("t2", lambda: None)
        assert ts.execute_task("t2") is True
        assert ts.tasks["t2"].status == "done"


class TestTimeout:
    def test_timeout_kills_dead_loop(self):
        ts = TaskScheduler()
        ts.schedule_task("slow", lambda: time.sleep(5))
        assert ts.execute_task("slow", timeout=0.1) is False
        assert ts.tasks["slow"].status == "timeout"
        assert isinstance(ts.tasks["slow"].error, TimeoutError)

    def test_success_within_timeout(self):
        ts = TaskScheduler()
        ts.schedule_task("fast", lambda: 42)
        assert ts.execute_task("fast", timeout=1) is True
        assert ts.tasks["fast"].result == 42


class TestRetryBackoff:
    def test_retry_has_backoff(self):
        ts = TaskScheduler()
        ts.tasks["t1"] = Task(
            "t1", lambda: (_ for _ in ()).throw(ValueError()),
            status="failed", error=RetryableError())
        ts.retry_task("t1", max_retries=1)
        assert ts.tasks["t1"].retries <= 1
        assert ts.tasks["t1"].backoff > 0

    def test_exponential_backoff_delays(self):
        ts = TaskScheduler()
        sleeps = []
        ts.sleep = lambda seconds: sleeps.append(seconds)
        attempts = iter([RetryableError("net"), RetryableError("net"), "ok"])

        def flaky():
            value = next(attempts)
            if isinstance(value, Exception):
                raise value
            return value

        ts.schedule_task("t1", flaky)
        assert ts.execute_task("t1") is False
        assert ts.retry_task("t1", max_retries=3) is False
        assert ts.retry_task("t1", max_retries=3) is True
        assert sleeps == [ts.base_delay * 1, ts.base_delay * 2]
        assert ts.tasks["t1"].status == "done"

    def test_retry_count_limited(self):
        ts = TaskScheduler()

        def always_fail():
            raise RetryableError("forever")

        ts.schedule_task("t1", always_fail)
        ts.execute_task("t1")
        for _ in range(10):
            ts.retry_task("t1", max_retries=3)
        assert ts.tasks["t1"].retries == 3

    def test_non_retryable_error_not_retried(self):
        ts = TaskScheduler()
        ts.sleep = lambda seconds: (_ for _ in ()).throw(AssertionError("slept"))

        def bug():
            raise NonRetryableError("code bug")

        ts.schedule_task("t1", bug)
        assert ts.execute_task("t1") is False
        assert ts.retry_task("t1", max_retries=5) is False
        assert ts.tasks["t1"].retries == 0

    def test_retryable_error_retried(self):
        ts = TaskScheduler()
        ts.sleep = lambda seconds: None
        ts.schedule_task("t1", lambda: (_ for _ in ()).throw(ConnectionError()))
        ts.execute_task("t1")
        assert ts.tasks["t1"].retryable is True
        assert ts.retry_task("t1", max_retries=3) is False
        assert ts.tasks["t1"].retries == 1


class TestCoroutine:
    def test_coroutine_concurrency_limited(self):
        ts = TaskScheduler(max_concurrency=2)
        assert ts.schedule_coroutine(lambda: None) is True

    def test_coroutine_concurrency_cap(self):
        ts = TaskScheduler(max_concurrency=2)
        active = 0
        max_seen = 0

        async def worker():
            nonlocal active, max_seen
            active += 1
            max_seen = max(max_seen, active)
            await asyncio.sleep(0.05)
            active -= 1

        async def scenario():
            await asyncio.gather(*(ts.start_coroutine(worker()) for _ in range(10)))
            await asyncio.gather(*list(ts._coro_tasks.values()))

        asyncio.run(scenario())
        assert max_seen <= 2
        ts.shutdown()

    def test_coroutine_communication_no_deadlock(self):
        async def scenario():
            ts = TaskScheduler(max_concurrency=4)
            channel = await ts.create_channel("c")
            event = await ts.get_event("e")

            async def producer():
                await channel.send(123)
                event.set()

            async def consumer():
                await event.wait()
                return await channel.recv(timeout=1)

            cid = await ts.start_coroutine(producer())
            await ts.start_coroutine(consumer())
            value = await asyncio.wait_for(ts._coro_tasks[
                [k for k, v in ts._coro_tasks.items()][-1]], timeout=2)
            assert value == 123

        asyncio.run(scenario())

    def test_coroutine_cancel_cleans_up(self):
        ts = TaskScheduler(max_concurrency=10)

        async def long_running():
            try:
                await asyncio.sleep(10)
            except asyncio.CancelledError:
                raise

        async def scenario():
            cid = await ts.start_coroutine(long_running())
            await asyncio.sleep(0.02)
            assert ts.coroutine_active(cid) is True
            assert await ts.cancel_coroutine(cid) is True
            assert ts.coroutine_active(cid) is False
            assert ts.active_coroutines() == 0

        asyncio.run(scenario())


class TestCircularDependency:
    def test_circular_dependency_detected(self):
        ts = TaskScheduler()
        ts.schedule_task("a", lambda: None, deps=["b"])
        ts.schedule_task("b", lambda: None, deps=["a"])
        assert ts.resolve_dependencies("a") is False
        assert ts.has_cycle("a") is True

    def test_self_dependency_detected(self):
        ts = TaskScheduler()
        ts.schedule_task("x", lambda: None, deps=["x"])
        assert ts.resolve_dependencies("x") is False
        assert ts.has_cycle("x") is True
