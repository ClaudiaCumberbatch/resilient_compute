import os
import sys
import parsl
from parsl.app.app import python_app

module_path = '/home/szhou3/resilient_compute/resilience_test/expanse'
sys.path.append(module_path)
from expanse_config import exp_config

@python_app
def consume_memory():
    huge_memory_list = []
    cnt = 0
    while True:
        if cnt > 10000:
            import time
            time.sleep(10)
            return "Consume memory finished"
        huge_memory_list.append('A' * 1024 * 1024)
        cnt += 1

@python_app
def simple_task():
    import time
    time.sleep(10)
    return "Simple task completed"

if __name__ == "__main__":
    dfk = parsl.load(exp_config(worker=2))
    tasks = [consume_memory()]
    simple_tasks = [simple_task() for _ in range(5)]

    [print(t.result()) for t in tasks]
    [print(t.result()) for t in simple_tasks]
