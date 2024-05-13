import os
import sys
import parsl
from parsl.app.app import python_app

module_path = '/home/szhou3/resilient_compute/resilience_test/expanse'
sys.path.append(module_path)
from expanse_config import exp_config

@python_app
def sleep120s():
    import time
    time.sleep(120)

@python_app
def simple_task():
    import time
    time.sleep(3)

if __name__ == "__main__":
    dfk = parsl.load(exp_config(walltime='00:01:00', retry=1, worker=3, exclusive=False))
    tasks = [sleep120s()]
    [t.result() for t in tasks]
