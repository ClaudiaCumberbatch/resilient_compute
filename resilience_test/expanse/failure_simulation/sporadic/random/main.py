import os
import sys
import parsl
from parsl.app.app import python_app

module_path = '/home/szhou3/resilient_compute/resilience_test/expanse'
sys.path.append(module_path)
from expanse_config import exp_config

@python_app
def random_err():
    import random
    if random.random() > 0.5:
        return
    else:
        raise Exception("this is a random exception")


if __name__ == "__main__":
    dfk = parsl.load(exp_config())
    tasks = [random_err() for _ in range(5)]
    [t.result() for t in tasks]
