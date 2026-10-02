"""任务调度 - 红态测试"""
import asyncio
import threading
import sys, os
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from task.scheduler import TaskScheduler, Task


class TestDependencyCheck:
    def test_blocked_by_dependency(self):
        ts = TaskScheduler()
        ts.schedule_task("dep", lambda: None)
        ts.schedule_task("main", lambda: None, deps=["dep"])
        assert ts.resolve_dependencies("main") == False

    def test_transitive_dependency(self):
        ts = TaskScheduler(auto_start=False)
        ts.schedule_task("c", lambda: None)
        ts.schedule_task("b", lambda: None, deps=["c"])
        ts.schedule_task("a", lambda: None, deps=["b"])
        assert ts.resolve_dependencies("a") is False
        ts.tasks["c"].status = "done"
        assert ts.resolve_dependencies("a") is False
        ts.tasks["b"].status = "done"
        assert ts.resolve_dependencies("a") is True

    def test_dependency_unknown(self):
        ts = TaskScheduler()
        ts.schedule_task("a", lambda: None, deps=["ghost"])
        assert ts.resolve_dependencies("a") is False


class TestDuplicateAndPriority:
    def test_duplicate_schedule_rejected(self):
        ts = TaskScheduler()
        assert ts.schedule_task("t", lambda: 1) is True
        assert ts.schedule_task("t", lambda: 2) is False
        assert ts.tasks["t"].func() == 1

    def test_priority_ordering(self):
        ts = TaskScheduler()
        ts.schedule_task("low", lambda: None, priority=1)
        ts.schedule_task("high", lambda: None, priority=9)
        ts.schedule_task("mid", lambda: None, priority=5)
        assert ts.next_task() == "high"
        assert ts.next_task() == "mid"
        assert ts.next_task() == "low"

    def test_blocked_task_not_picked(self):
        ts = TaskScheduler()
        ts.schedule_task("dep", lambda: None)
        ts.schedule_task("main", lambda: None, priority=9, deps=["dep"])
        ts.schedule_task("free", lambda: None, priority=1)
        assert ts.next_task() == "free"


class TestExceptionHandling:
    def test_exception_does_not_crash(self):
        ts = TaskScheduler()
        def fail(): raise ValueError("test")
        ts.schedule_task("t1", fail)
        result = ts.execute_task("t1")
        assert result is False
        assert ts.tasks["t1"].status == "failed"
        assert isinstance(ts.tasks["t1"].last_error, ValueError)

    def test_timeout_marks_task_failed(self):
        ts = TaskScheduler(default_timeout=5)
        proceed = threading.Event()
        started = threading.Event()
        def hang():
            started.set()
            proceed.wait(10)
        ts.schedule_task("slow", hang)
        assert ts.execute_task("slow", timeout=0.2) is False
        assert ts.tasks["slow"].status == "failed"
        assert isinstance(ts.tasks["slow"].last_error, TimeoutError)
        proceed.set()

    def test_success_records_status_and_result(self):
        ts = TaskScheduler()
        ts.schedule_task("ok", lambda: 42)
        assert ts.execute_task("ok") is True
        assert ts.tasks["ok"].status == "done"
        assert ts.tasks["ok"].result == 42


class TestRetryBackoff:
    def test_retry_has_backoff(self):
        ts = TaskScheduler()
        ts.tasks["t1"] = Task("t1", lambda: (_ for _ in ()).throw(ValueError()), status="failed")
        ts.retry_task("t1", max_retries=1)
        assert ts.tasks["t1"].retries <= 1

    def test_retry_limit_enforced(self):
        ts = TaskScheduler(backoff_base=0)
        ts.tasks["t"] = Task("t", lambda: None, status="failed", retries=3,
                             last_error=ConnectionError())
        assert ts.retry_task("t", max_retries=3) is False
        assert ts.tasks["t"].retries == 3

    def test_code_error_not_retried(self):
        ts = TaskScheduler()
        ts.tasks["t"] = Task("t", lambda: None, status="failed",
                             last_error=ValueError())
        assert ts.retry_task("t") is False
        assert ts.tasks["t"].retries == 0

    def test_network_error_retried_with_backoff(self, monkeypatch):
        sleeps = []
        monkeypatch.setattr("task.scheduler.time.sleep", lambda s: sleeps.append(s))
        ts = TaskScheduler()
        ts.tasks["t"] = Task("t", lambda: "ok", status="failed",
                             last_error=ConnectionError())
        assert ts.retry_task("t", max_retries=2) is True
        assert ts.tasks["t"].retries == 1
        assert sleeps == [ts.backoff_base]


class TestConcurrencyLimit:
    def test_coroutine_concurrency_limited(self):
        ts = TaskScheduler()
        assert ts.schedule_coroutine(lambda: None) == True

    def test_coroutine_concurrency_enforced(self):
        ts = TaskScheduler(max_concurrent=3)
        state = {"active": 0, "peak": 0}
        lock = threading.Lock()

        async def work():
            with lock:
                state["active"] += 1
                state["peak"] = max(state["peak"], state["active"])
            await asyncio.sleep(0.02)
            with lock:
                state["active"] -= 1

        for _ in range(20):
            ts.schedule_coroutine(work())
        asyncio.run(ts.run_coroutines())
        assert state["peak"] <= 3

    def test_coroutine_communication(self):
        ts = TaskScheduler(max_concurrent=2)

        async def producer():
            await ts.coro_channel.put("hello")

        async def consumer():
            return await ts.coro_channel.get()

        ts.schedule_coroutine(producer())
        ts.schedule_coroutine(consumer())
        results = asyncio.run(ts.run_coroutines())
        assert "hello" in results

    def test_coroutine_cancellation_cleans_up(self):
        ts = TaskScheduler(max_concurrent=2)
        cleaned = []

        async def forever():
            try:
                await asyncio.sleep(100)
            finally:
                cleaned.append(True)

        ts.schedule_coroutine(forever())

        async def scenario():
            runner = asyncio.create_task(ts.run_coroutines())
            await asyncio.sleep(0.05)
            runner.cancel()
            try:
                await runner
            except asyncio.CancelledError:
                pass

        asyncio.run(scenario())
        assert cleaned == [True]
        assert ts._coro_tasks == set()


class TestCircularDependency:
    def test_circular_dependency_detected(self):
        ts = TaskScheduler()
        ts.schedule_task("a", lambda: None, deps=["b"])
        ts.schedule_task("b", lambda: None, deps=["a"])
        assert ts.resolve_dependencies("a") == False

    def test_self_cycle_rejected(self):
        ts = TaskScheduler()
        assert ts.schedule_task("a", lambda: None, deps=["a"]) is False


class TestCompletionNotification:
    def test_dependent_auto_starts_on_completion(self):
        ts = TaskScheduler()
        ts.schedule_task("dep", lambda: 1)
        ts.schedule_task("main", lambda: 2, deps=["dep"])
        assert ts.tasks["main"].status == "pending"
        assert ts.execute_task("dep") is True
        assert ts.tasks["main"].status == "done"

    def test_dependent_chain_notification(self):
        ts = TaskScheduler()
        ts.schedule_task("c", lambda: None)
        ts.schedule_task("b", lambda: None, deps=["c"])
        ts.schedule_task("a", lambda: None, deps=["b"])
        assert ts.execute_task("c") is True
        assert ts.tasks["b"].status == "done"
        assert ts.tasks["a"].status == "done"
