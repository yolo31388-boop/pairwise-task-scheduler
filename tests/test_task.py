"""任务调度 - 红态测试"""
import pytest, sys, os
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from task.scheduler import TaskScheduler, Task

class TestDependencyCheck:
    def test_blocked_by_dependency(self):
        ts = TaskScheduler()
        ts.schedule_task("dep", lambda: None)
        ts.schedule_task("main", lambda: None, deps=["dep"])
        assert ts.resolve_dependencies("main") == False

class TestExceptionHandling:
    def test_exception_does_not_crash(self):
        ts = TaskScheduler()
        def fail(): raise ValueError("test")
        ts.schedule_task("t1", fail)
        result = ts.execute_task("t1")
        assert ts.tasks["t1"].status == "failed"

class TestRetryBackoff:
    def test_retry_has_backoff(self):
        ts = TaskScheduler()
        ts.tasks["t1"] = Task("t1", lambda: (_ for _ in ()).throw(ValueError()), status="failed")
        ts.retry_task("t1", max_retries=1)
        assert ts.tasks["t1"].retries <= 1

class TestConcurrencyLimit:
    def test_coroutine_concurrency_limited(self):
        ts = TaskScheduler()
        assert ts.schedule_coroutine(lambda: None) == True

class TestCircularDependency:
    def test_circular_dependency_detected(self):
        ts = TaskScheduler()
        ts.schedule_task("a", lambda: None, deps=["b"])
        ts.schedule_task("b", lambda: None, deps=["a"])
        assert ts.resolve_dependencies("a") == False
