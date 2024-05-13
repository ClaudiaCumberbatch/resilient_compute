import os
import json
import datetime
import parsl
from parsl.config import Config
from parsl.executors import HighThroughputExecutor
from parsl.app.app import python_app
from parsl.launchers import SimpleLauncher
from parsl.launchers import SingleNodeLauncher
from typing import Optional, Any

# for local test on ChameleonCloud
from parsl.data_provider.http import HTTPInTaskStaging
from parsl.data_provider.ftp import FTPInTaskStaging
from parsl.data_provider.file_noop import NoOpFileStaging

from parsl.providers import LocalProvider,SlurmProvider
from parsl.channels import LocalChannel
from parsl.launchers import SingleNodeLauncher,SrunLauncher

from parsl.monitoring import MonitoringHub

def get_config():
    config = Config(
        executors=[
            HighThroughputExecutor(
                max_workers=2,
                label="htex_1",
                provider=SlurmProvider(
                    'debug', # 'compute'
                    account='cuw101',
                    launcher=SrunLauncher(),
                    # string to prepend to #SBATCH blocks in the submit
                    # script to the scheduler
                    scheduler_options='',
                    # Command to be run before starting a worker, such as:
                    # 'module load Anaconda; source activate parsl_env'.
                    worker_init='module load cpu; module load slurm; source activate /home/szhou3/.conda/envs/parsl310',
                    # walltime='01:00:00',
                    init_blocks=1,
                    max_blocks=1,
                    nodes_per_block=1,
                    mem_per_node=20,
                ),
                block_error_handler=False,
                radio_mode="diaspora"
            ),
            # HighThroughputExecutor(
            #     max_workers=1,
            #     label="htex_2",
            #     provider=SlurmProvider(
            #         'debug', # 'compute'
            #         account='cuw101',
            #         launcher=SrunLauncher(),
            #         # string to prepend to #SBATCH blocks in the submit
            #         # script to the scheduler
            #         scheduler_options='',
            #         # Command to be run before starting a worker, such as:
            #         # 'module load Anaconda; source activate parsl_env'.
            #         worker_init='module load cpu; module load purge; module load slurm; module load Anaconda; source activate /home/szhou3/.conda/envs/parsl310',
            #         # walltime='01:00:00',
            #         init_blocks=1,
            #         max_blocks=1,
            #         nodes_per_block=1,
            #     ),
            #     block_error_handler=False,
            #     radio_mode="diaspora"
            # )
        ],
        strategy='simple',
        resilience_strategy='random',
        app_cache=True, checkpoint_mode='task_exit',
        retries=1,
        monitoring=MonitoringHub(
                        hub_address="localhost",
                        monitoring_debug=False,
                        resource_monitoring_interval=1,
        ),
        usage_tracking=True
    )
 
    return config


# @python_app
@python_app(executors=['htex_1'])
def throw_exp():
    import time
    time.sleep(1)
    raise Exception("This is an exception")

'''Set walltime limit as 60s and let this sleep time longer than limitation.
Will fail with task_fail_history ManagerLost and invoke retry.
But no failure info sent to DEF, and the main process will somehow not end.
'''
@python_app
def sleep120s():
    import time
    time.sleep(120)

''' 1. Bad environment. 
The error will both show up in terminal and be sent to DEF.
'''
@python_app
def bad_import():
    import not_exits

'''2. Perminant code error.
This error will only appear in terminal.
'''
@python_app
def code_err():
    return 100/0

'''3. Sporadic Code Error.
This error will only appear in terminal.
HOW DO YOU KNOW IT'S A SPORADIC ERROR?
'''
@python_app
def sporadic_err():
    import random
    if random.random() > 0.5:
        return
    else:
        raise Exception("this is a random exception")

''' 4. Resource error. Exception can be caught in terminal.
Will get WorkerLost and PermissionError.
Monitor info will get zombie status process.
'''
@python_app
def consume_memory():
    huge_memory_list = []
    while True:
        huge_memory_list.append('A' * 1024 * 1024 * 100)

@python_app
def occupy_memory(size_gb, duration=10):
    import time
    import numpy as np
    big_array = np.zeros((size_gb * 250000, 10))
    time.sleep(duration)
    return f"Consumed {size_gb} GB for {duration} seconds"

@python_app
def simple_task():
    return "Simple task completed"


if __name__ == "__main__":
    config = get_config()
    dfk = parsl.load(config)
    tasks = [consume_memory()]
    # [t.result() for t in tasks]

    # resource_hungry_task = occupy_memory(200) # 200GB
    simple_tasks = [simple_task() for _ in range(5)]

    [t.result() for t in tasks]

    # print(resource_hungry_task.result())
    for task in simple_tasks:
        print(task.result())
