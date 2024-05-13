import os
import sys
import parsl
from parsl.app.app import python_app

module_path = '/home/szhou3/resilient_compute/resilience_test/expanse'
sys.path.append(module_path)
from expanse_config import exp_config

@python_app
def write_to_file(filename, number):
    with open(filename, "a") as f:
        f.write(str(number) + "\n")
    return f"Wrote '{number}' to {filename}"


if __name__ == "__main__":
    dfk = parsl.load(exp_config(worker=128))

    filename = "temp.txt"
    tasks = [write_to_file(filename, i) for i in range(1000)]
    [print(t.result()) for t in tasks]
