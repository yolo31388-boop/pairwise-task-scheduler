"""任务调度系统"""
from __future__ import annotations

import asyncio
import heapq
import itertools
import time
from concurrent.futures import ThreadPoolExecutor
from concurrent.futures import TimeoutError as FutureTimeoutError
from dataclasses import dataclass, field

# 可重试的瞬时错误（网络/超时/IO）；代码错误（ValueError/TypeError 等）不重试
RETRYABLE_ERRORS = (ConnectionError, TimeoutError, OSError)


@dataclass
class Task:
    tid: str
    func: object
    priority: int = 0
    dependencies: list = field(default_factory=list)
    status: str = "pending"  # pending/running/done/failed
    retries: int = 0
    last_error: Exception | None = None
    result: object = None


class TaskScheduler:
    def __init__(self, max_concurrent: int = 10, default_timeout: float = 30.0,
                 backoff_base: float = 0.1, auto_start: bool = True):
        self.tasks: dict[str, Task] = {}
        self.queue: list = []  # (-priority, seq, tid) 堆，高优先级先出
        self.max_concurrent = max_concurrent
        self.default_timeout = default_timeout
        self.backoff_base = backoff_base
        self.auto_start = auto_start
        self._executor = ThreadPoolExecutor(max_workers=max_concurrent)
        self._counter = itertools.count()
        self._waiting: set[str] = set()       # 依赖未就绪、等待通知的任务
        self._coroutines: list = []           # 已注册待运行的协程
        self._coro_tasks: set = set()         # 运行中的协程任务（用于取消清理）
        self.coro_channel: asyncio.Queue | None = None  # 协程间通信通道

    # ---------- 调度 ----------

    def schedule_task(self, tid: str, func, priority: int = 0, deps: list = None) -> bool:
        if tid in self.tasks:  # 重复调度检查
            return False
        deps = list(deps or [])
        if self._creates_cycle(tid, deps):  # 循环依赖检查
            return False
        task = Task(tid, func, priority, deps)
        self.tasks[tid] = task
        heapq.heappush(self.queue, (-priority, next(self._counter), tid))
        if not self.resolve_dependencies(tid):
            self._waiting.add(tid)
        return True

    def _creates_cycle(self, tid: str, deps: list) -> bool:
        stack, seen = list(deps), set()
        while stack:
            cur = stack.pop()
            if cur == tid:
                return True
            if cur in seen:
                continue
            seen.add(cur)
            existing = self.tasks.get(cur)
            if existing:
                stack.extend(existing.dependencies)
        return False

    def next_task(self) -> str | None:
        """取出优先级最高且依赖已就绪的待执行任务。"""
        skipped = []
        result = None
        while self.queue:
            entry = heapq.heappop(self.queue)
            tid = entry[2]
            task = self.tasks.get(tid)
            if task is None or task.status != "pending":
                continue
            if tid in self._waiting:
                skipped.append(entry)
                continue
            result = tid
            break
        for entry in skipped:
            heapq.heappush(self.queue, entry)
        return result

    # ---------- 执行 ----------

    def execute_task(self, tid: str, timeout: float | None = None) -> bool:
        task = self.tasks.get(tid)
        if task is None or task.status == "running":
            return False
        if not self.resolve_dependencies(tid):  # 依赖未就绪不执行
            return False
        timeout = self.default_timeout if timeout is None else timeout
        task.status = "running"
        future = self._executor.submit(task.func)
        try:
            task.result = future.result(timeout=timeout)
        except FutureTimeoutError:
            future.cancel()
            task.status = "failed"
            task.last_error = TimeoutError(f"task {tid} timed out after {timeout}s")
            return False
        except Exception as exc:  # 捕获异常，调度器不崩溃
            task.status = "failed"
            task.last_error = exc
            return False
        task.status = "done"
        task.last_error = None
        self._waiting.discard(tid)
        self._notify_dependents(tid)
        return True

    def _notify_dependents(self, tid: str) -> None:
        """任务完成后通知等待中的依赖方，依赖就绪则自动开始。"""
        for waiting_tid in list(self._waiting):
            if waiting_tid == tid:
                continue
            if self.resolve_dependencies(waiting_tid):
                self._waiting.discard(waiting_tid)
                if self.auto_start:
                    self.execute_task(waiting_tid)

    # ---------- 重试 ----------

    def retry_task(self, tid: str, max_retries: int = 3) -> bool:
        task = self.tasks.get(tid)
        if task is None or task.status != "failed":
            return False
        if task.retries >= max_retries:  # 次数限制
            return False
        if task.last_error is not None and not isinstance(task.last_error, RETRYABLE_ERRORS):
            return False  # 代码错误不重试
        task.retries += 1
        time.sleep(self.backoff_base * (2 ** (task.retries - 1)))  # 指数退避
        return self.execute_task(tid)

    # ---------- 协程 ----------

    def schedule_coroutine(self, coro) -> bool:
        self._coroutines.append(coro)
        return True

    async def run_coroutines(self) -> list:
        """以信号量限制并发运行已注册协程，支持取消清理与队列通信。"""
        if self.coro_channel is None:
            self.coro_channel = asyncio.Queue()
        semaphore = asyncio.Semaphore(self.max_concurrent)

        async def runner(coro):
            async with semaphore:
                try:
                    if asyncio.iscoroutine(coro):
                        return await coro
                    if callable(coro):
                        value = coro()
                        if asyncio.iscoroutine(value):
                            return await value
                        return value
                    return coro
                finally:  # 取消或结束时清理注册表
                    self._coro_tasks.discard(asyncio.current_task())

        tasks = []
        for coro in self._coroutines:
            t = asyncio.create_task(runner(coro))
            self._coro_tasks.add(t)
            tasks.append(t)
        self._coroutines.clear()
        try:
            return await asyncio.gather(*tasks)
        finally:
            for t in tasks:
                t.cancel()
            await asyncio.gather(*tasks, return_exceptions=True)
            self._coro_tasks.difference_update(tasks)

    # ---------- 依赖 ----------

    def resolve_dependencies(self, tid: str) -> bool:
        """传递依赖检查 + 循环依赖检测。"""
        if tid not in self.tasks:
            return False
        visiting: set[str] = set()
        visited: set[str] = set()

        def check(current: str) -> bool:
            if current in visiting:
                return False  # 循环依赖
            if current in visited:
                return True
            visiting.add(current)
            node = self.tasks.get(current)
            ok = node is not None and node.status == "done" and all(
                check(nxt) for nxt in node.dependencies
            )
            visiting.discard(current)
            if ok:
                visited.add(current)
            return ok

        return all(check(dep) for dep in self.tasks[tid].dependencies)
