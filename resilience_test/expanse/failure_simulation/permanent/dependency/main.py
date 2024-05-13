import os
import sys
import parsl
from parsl.app.app import python_app

module_path = '/home/szhou3/resilient_compute/resilience_test/expanse'
sys.path.append(module_path)
from expanse_config import exp_config

@python_app
def fails():
    raise ValueError("Deliberate failure")

@python_app
def depends(parent):
    return 1

if __name__ == "__main__":
    dfk = parsl.load(exp_config(retry=1))
    f1 = fails()
    f2 = depends(f1)
    f3 = depends(f2)
    f4 = depends(f3)
    print(f1.result(), f2.result(), f3.result(), f4.result())
