import os
import sys
import parsl
from parsl.app.app import python_app

module_path = '/home/szhou3/resilient_compute/resilience_test/expanse'
sys.path.append(module_path)
from expanse_config import exp_config

@python_app
def elephant():
    a = [1, 2, 3]
    if (n := len(a)) > 2:
        print(f"List is too long ({n} elements, expected <= 2)")


if __name__ == "__main__":
    dfk = parsl.load(exp_config(worker_init='module load cpu/0.15.4; module load slurm; module load anaconda3/2020.11; source activate /home/szhou3/.conda/envs/parsl307'))
    tasks = [elephant()]
    [t.result() for t in tasks]
