"""任务调度：依赖/优先级/超时/重试退避/协程并发控制。"""
from __future__ import annotations

import asyncio
import contextlib
import inspect
import itertools
import signal
import threading
import time
from concurrent.futures import ThreadPoolExecutor
from dataclasses import dataclass, field


class _DaemonThreadPoolExecutor(ThreadPoolExecutor):
    """工作线程为守护线程：超时遗留的死循环不会阻止进程退出。"""

    def _adjust_thread_count(self):
        import threading
        import weakref
        from concurrent.futures.thread import _worker

        if self._idle_semaphore.acquire(timeout=0):
            return

        def weakref_cb(_, q=self._work_queue):
            q.put(None)

        num_threads = len(self._threads)
        if num_threads < self._max_workers:
            thread_name = '%s_%d' % (self._thread_name_prefix or self,
                                     num_threads)
            thread = threading.Thread(
                name=thread_name, target=_worker,
                args=(weakref.ref(self, weakref_cb),
                      self._create_worker_context(),
                      self._work_queue),
                daemon=True)
            thread.start()
            self._threads.add(thread)


class RetryableError(Exception):
    """可重试错误（瞬时故障，如网络抖动）。"""


class NonRetryableError(Exception):
    """不可重试错误（确定性代码错误）。"""


_RETRYABLE_TYPES = (
    RetryableError,
    ConnectionError,
    TimeoutError,
    asyncio.TimeoutError,
    OSError,
)
_NON_RETRYABLE_TYPES = (
    NonRetryableError,
    ValueError,
    TypeError,
    KeyError,
    AttributeError,
    IndexError,
    ZeroDivisionError,
)


@dataclass
class Task:
    tid: str
    func: object
    priority: int = 0
    dependencies: list = field(default_factory=list)
    status: str = "pending"  # pending/running/done/failed/timeout
    retries: int = 0
    result: object = None
    error: Exception | None = None
    retryable: bool = True
    backoff: float = 0.0
    _seq: int = 0


class Channel:
    """协程间通信通道。"""

    def __init__(self):
        self._queue: asyncio.Queue = asyncio.Queue()

    async def send(self, item) -> None:
        await self._queue.put(item)

    async def recv(self, timeout: float | None = None):
        if timeout is None:
            return await self._queue.get()
        return await asyncio.wait_for(self._queue.get(), timeout)

    def qsize(self) -> int:
        return self._queue.qsize()


class TaskScheduler:
    def __init__(self, max_concurrency: int = 10, base_delay: float = 0.01,
                 max_workers: int = 4):
        self.tasks: dict[str, Task] = {}
        self.queue: list[Task] = []
        self.max_concurrency = max_concurrency
        self.base_delay = base_delay
        self.sleep = time.sleep
        self._seq = itertools.count()
        self._cond = threading.Condition()
        self._running = False
        self._worker: threading.Thread | None = None
        self._executor = _DaemonThreadPoolExecutor(max_workers=max_workers,
                                            thread_name_prefix="task-exec")

        # 协程调度状态（绑定到首次使用时的事件循环）
        self._semaphore: asyncio.Semaphore | None = None
        self._coro_tasks: dict[str, asyncio.Task] = {}
        self._events: dict[str, asyncio.Event] = {}
        self._channels: dict[str, Channel] = {}
        self._loop: asyncio.AbstractEventLoop | None = None
        self._loop_thread: threading.Thread | None = None
        self._coro_futures: dict[str, object] = {}

    # ------------------------------------------------------------------
    # 任务调度
    # ------------------------------------------------------------------
    def schedule_task(self, tid: str, func, priority: int = 0,
                      deps: list | None = None) -> bool:
        with self._cond:
            if tid in self.tasks:  # 重复调度拒绝
                return False
            task = Task(tid, func, priority, list(deps or []), _seq=next(self._seq))
            self.tasks[tid] = task
            self.queue.append(task)
            self._cond.notify_all()
        return True

    def _pick_ready_locked(self) -> Task | None:
        """选出依赖已满足、优先级最高的待执行任务。"""
        ready = [t for t in self.tasks.values()
                 if t.status == "pending" and self.resolve_dependencies(t.tid)]
        if not ready:
            return None
        ready.sort(key=lambda t: (-t.priority, t._seq))
        return ready[0]

    def next_task(self) -> Task | None:
        with self._cond:
            return self._pick_ready_locked()

    def run(self) -> None:
        """同步跑完当前所有已就绪任务（按优先级）。"""
        while True:
            with self._cond:
                task = self._pick_ready_locked()
            if task is None:
                return
            self.execute_task(task.tid)

    def start(self) -> None:
        """后台 worker：依赖完成自动通知并拉起新任务。"""
        if self._running:
            return
        self._running = True
        self._worker = threading.Thread(target=self._worker_loop, daemon=True)
        self._worker.start()

    def shutdown(self, timeout: float | None = 5) -> None:
        with self._cond:
            self._running = False
            self._cond.notify_all()
        if self._worker:
            self._worker.join(timeout)
        self._shutdown_coroutine_loop(timeout)
        self._executor.shutdown(wait=False, cancel_futures=True)

    def _worker_loop(self) -> None:
        while True:
            with self._cond:
                if not self._running:
                    return
                task = self._pick_ready_locked()
                if task is None:
                    self._cond.wait(timeout=0.05)
                    continue
                task.status = "running"
            self._invoke(task)

    def wait_idle(self, timeout: float | None = None) -> bool:
        """等待所有任务结束（done/failed/timeout 或被依赖阻塞）。"""
        deadline = None if timeout is None else time.monotonic() + timeout
        with self._cond:
            while any(t.status in ("pending", "running") for t in self.tasks.values()):
                if not self._running:
                    return False
                remaining = None if deadline is None else deadline - time.monotonic()
                if remaining is not None and remaining <= 0:
                    return False
                self._cond.wait(timeout=remaining)
            return True

    # ------------------------------------------------------------------
    # 任务执行：超时控制 + 异常捕获 + 状态记录
    # ------------------------------------------------------------------
    def execute_task(self, tid: str, timeout: float | None = None) -> bool:
        task = self.tasks.get(tid)
        if task is None:
            return False
        with self._cond:
            if task.status in ("running", "done"):
                return False
            if not self.resolve_dependencies(tid):  # 依赖（含传递依赖）未完成
                return False
            task.status = "running"
            task.error = None
            self._cond.notify_all()
        return self._invoke(task, timeout=timeout)

    def _invoke(self, task: Task, timeout: float | None = None) -> bool:
        try:
            task.result = self._run_with_timeout(task.func, timeout)
        except TimeoutError as exc:
            task.status = "timeout"
            task.error = exc
            task.retryable = True
            ok = False
        except Exception as exc:  # 异常不外泄，调度器不崩
            task.status = "failed"
            task.error = exc
            task.retryable = self.is_retryable(exc)
            ok = False
        else:
            task.status = "done"
            ok = True
        finally:
            with self._cond:
                self._cond.notify_all()  # 依赖完成通知
        return ok

    def _run_with_timeout(self, func, timeout):
        if timeout is None:
            return func()
        if (threading.current_thread() is threading.main_thread()
                and hasattr(signal, "SIGALRM")):
            return self._run_with_signal_alarm(func, timeout)
        future = self._executor.submit(func)
        return future.result(timeout=timeout)

    @staticmethod
    def _run_with_signal_alarm(func, timeout):
        def _handler(signum, frame):
            raise TimeoutError("task timed out")

        previous = signal.signal(signal.SIGALRM, _handler)
        signal.setitimer(signal.ITIMER_REAL, float(timeout))
        try:
            return func()
        finally:
            signal.setitimer(signal.ITIMER_REAL, 0)
            signal.signal(signal.SIGALRM, previous)

    # ------------------------------------------------------------------
    # 重试：指数退避 + 次数上限 + 错误类型区分
    # ------------------------------------------------------------------
    def is_retryable(self, exc: BaseException | None) -> bool:
        if isinstance(exc, _NON_RETRYABLE_TYPES) and \
                not isinstance(exc, _RETRYABLE_TYPES):
            return False
        return isinstance(exc, _RETRYABLE_TYPES)

    def retry_task(self, tid: str, max_retries: int = 3,
                   timeout: float | None = None) -> bool:
        task = self.tasks.get(tid)
        if task is None or task.status not in ("failed", "timeout"):
            return False
        if task.retries >= max_retries:  # 重试次数硬上限
            return False
        if task.error is not None and not self.is_retryable(task.error):
            return False  # 代码错误不重试
        delay = self.base_delay * (2 ** task.retries)  # 指数退避
        task.backoff = delay
        self.sleep(delay)
        task.retries += 1
        return self.execute_task(tid, timeout=timeout)

    # ------------------------------------------------------------------
    # 协程调度：并发控制 + 通信 + 取消清理
    # ------------------------------------------------------------------
    def _get_semaphore(self) -> asyncio.Semaphore:
        if self._semaphore is None:
            self._semaphore = asyncio.Semaphore(self.max_concurrency)
        return self._semaphore

    async def start_coroutine(self, coro, cid: str | None = None) -> str:
        cid = cid or f"coro-{len(self._coro_tasks)}-{id(coro)}"
        semaphore = self._get_semaphore()

        async def _runner():
            async with semaphore:  # 并发上限
                return await coro

        async_task = asyncio.create_task(_runner())
        self._coro_tasks[cid] = async_task

        def _discard(_task):
            self._coro_tasks.pop(cid, None)

        async_task.add_done_callback(_discard)
        return cid

    def schedule_coroutine(self, coro) -> bool:
        """线程安全入口：协程提交到后台事件循环，受信号量约束。"""
        if coro is None:
            return False
        loop = self._ensure_loop()

        async def _submit():
            awaitable = self._coerce_awaitable(coro)
            if awaitable is None:
                return None
            return await self.start_coroutine(awaitable)

        future = asyncio.run_coroutine_threadsafe(_submit(), loop)
        try:
            cid = future.result(timeout=5)
        except Exception:
            return False
        if cid is not None:
            self._coro_futures[cid] = future
        return True

    @staticmethod
    def _coerce_awaitable(coro):
        if asyncio.iscoroutine(coro) or hasattr(coro, "__await__"):
            return coro
        if inspect.iscoroutinefunction(coro):
            return coro()
        if callable(coro):
            result = coro()
            if asyncio.iscoroutine(result) or hasattr(result, "__await__"):
                return result
            return None
        return None

    async def cancel_coroutine(self, cid: str) -> bool:
        task = self._coro_tasks.pop(cid, None)
        if task is None:
            return False
        task.cancel()
        with contextlib.suppress(asyncio.CancelledError, BaseException):
            await task  # 等待清理完成（信号量在 finally 中释放）
        self._coro_futures.pop(cid, None)
        return True

    def coroutine_active(self, cid: str) -> bool:
        return cid in self._coro_tasks

    def active_coroutines(self) -> int:
        return len(self._coro_tasks)

    async def get_event(self, name: str) -> asyncio.Event:
        event = self._events.get(name)
        if event is None:
            event = asyncio.Event()
            self._events[name] = event
        return event

    async def create_channel(self, name: str) -> Channel:
        channel = self._channels.get(name)
        if channel is None:
            channel = Channel()
            self._channels[name] = channel
        return channel

    def _ensure_loop(self) -> asyncio.AbstractEventLoop:
        if self._loop is not None and self._loop.is_running():
            return self._loop
        ready = threading.Event()
        loop = asyncio.new_event_loop()

        def _run():
            asyncio.set_event_loop(loop)
            ready.set()
            loop.run_forever()

        self._loop_thread = threading.Thread(target=_run, daemon=True)
        self._loop_thread.start()
        ready.wait()
        self._loop = loop
        return loop

    def _shutdown_coroutine_loop(self, timeout: float | None) -> None:
        loop, thread = self._loop, self._loop_thread
        if loop is None or not loop.is_running():
            return

        async def _cancel_all():
            for task in list(self._coro_tasks.values()):
                task.cancel()
            await asyncio.gather(*list(self._coro_tasks.values()),
                                 return_exceptions=True)

        with contextlib.suppress(Exception):
            asyncio.run_coroutine_threadsafe(_cancel_all(), loop).result(timeout)
        loop.call_soon_threadsafe(loop.stop)
        if thread:
            thread.join(timeout)

    # ------------------------------------------------------------------
    # 依赖解析：传递依赖 + 循环检测
    # ------------------------------------------------------------------
    def _all_dependencies(self, tid: str) -> list[str] | None:
        """返回全部传递依赖；存在循环返回 None。"""
        ordered: list[str] = []
        visited: set[str] = set()
        stack: list[tuple[str, int]] = [(tid, 0)]
        on_path = {tid}
        while stack:
            current, index = stack[-1]
            task = self.tasks.get(current)
            if task is None or index >= len(task.dependencies):
                on_path.discard(current)
                stack.pop()
                continue
            dep = task.dependencies[index]
            stack[-1] = (current, index + 1)
            if dep in on_path:  # 循环依赖
                return None
            if dep not in visited:
                if dep not in self.tasks:
                    return []  # 缺失依赖，视为未满足
                visited.add(dep)
                ordered.append(dep)
                on_path.add(dep)
                stack.append((dep, 0))
        return ordered

    def has_cycle(self, tid: str) -> bool:
        return self._all_dependencies(tid) is None

    def resolve_dependencies(self, tid: str) -> bool:
        task = self.tasks.get(tid)
        if task is None:
            return False
        dependencies = self._all_dependencies(tid)
        if dependencies is None:  # 循环依赖
            return False
        for dep in dependencies:  # 直接 + 传递依赖全部完成
            dep_task = self.tasks.get(dep)
            if dep_task is None or dep_task.status != "done":
                return False
        return True
