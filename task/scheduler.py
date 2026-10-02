"""任务调度 - 含5个bug"""
from dataclasses import dataclass, field

@dataclass
class Task:
    tid: str
    func: object
    priority: int = 0
    dependencies: list = field(default_factory=list)
    status: str = "pending"  # pending/running/done/failed
    retries: int = 0

class TaskScheduler:
    def __init__(self):
        self.tasks: dict[str, Task] = {}
        self.queue: list = []

    def schedule_task(self, tid: str, func, priority: int = 0, deps: list = None) -> bool:
        # bug1: 不检查依赖和重复
        task = Task(tid, func, priority, deps or [])
        self.tasks[tid] = task
        self.queue.append(task)
        return True

    def execute_task(self, tid: str) -> bool:
        # bug2: 无超时，不捕获异常
        task = self.tasks.get(tid)
        if task:
            task.status = "running"
            result = task.func()
            task.status = "done"
            return True
        return False

    def retry_task(self, tid: str, max_retries: int = 3) -> bool:
        # bug3: 无退避，无次数限制
        task = self.tasks.get(tid)
        if task and task.status == "failed":
            task.retries += 1
            return self.execute_task(tid)
        return False

    def schedule_coroutine(self, coro) -> bool:
        # bug4: 无并发控制，无通信
        return True

    def resolve_dependencies(self, tid: str) -> bool:
        # bug5: 只检查直接依赖，无循环检测
        task = self.tasks.get(tid)
        if not task:
            return False
        for dep in task.dependencies:
            if self.tasks.get(dep, Task("x", None)).status != "done":
                return False
        return True
