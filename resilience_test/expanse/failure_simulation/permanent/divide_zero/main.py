import os
import sys
import parsl
from parsl.app.app import python_app

module_path = '/home/szhou3/resilient_compute/resilience_test/expanse'
sys.path.append(module_path)
from expanse_config import exp_config

@python_app
def div_zero():
    return 100/0


if __name__ == "__main__":
    dfk = parsl.load(exp_config())
    tasks = [div_zero()]
    [t.result() for t in tasks]
