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
    while True:
        huge_memory_list.append('A' * 1024 * 1024 * 100)

@python_app
def simple_task():
    print("hello")
    import time
    time.sleep(3)


if __name__ == "__main__":
    dfk = parsl.load(exp_config(retry=1, worker=3, exclusive=False))
    tasks = [consume_memory(), simple_task()]
    [t.result() for t in tasks]
