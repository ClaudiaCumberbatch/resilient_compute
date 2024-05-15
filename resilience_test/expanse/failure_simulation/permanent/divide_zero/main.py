import os
import sys
import parsl
from parsl.app.app import python_app

config_path = '/home/szhou3/resilient_compute/resilience_test/expanse/failure_simulation'
sys.path.append(config_path)
from expanse_config import exp_config

@python_app
def div_zero():
    return 100/0


if __name__ == "__main__":
    dfk = parsl.load(exp_config(retry=1, worker=3, exclusive=False))
    tasks = [div_zero()]
    [t.result() for t in tasks]
