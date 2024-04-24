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
                max_workers=1,
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
                    worker_init='module load cpu; module load purge; module load slurm; module load Anaconda; source activate /home/szhou3/.conda/envs/parsl310',
                    # walltime='01:00:00',
                    init_blocks=1,
                    max_blocks=1,
                    nodes_per_block=1,
                ),
                block_error_handler=False,
                radio_mode="diaspora"
            ),
            HighThroughputExecutor(
                max_workers=1,
                label="htex_2",
                provider=SlurmProvider(
                    'debug', # 'compute'
                    account='cuw101',
                    launcher=SrunLauncher(),
                    # string to prepend to #SBATCH blocks in the submit
                    # script to the scheduler
                    scheduler_options='',
                    # Command to be run before starting a worker, such as:
                    # 'module load Anaconda; source activate parsl_env'.
                    worker_init='module load cpu; module load purge; module load slurm; module load Anaconda; source activate /home/szhou3/.conda/envs/parsl310',
                    # walltime='01:00:00',
                    init_blocks=1,
                    max_blocks=1,
                    nodes_per_block=1,
                ),
                block_error_handler=False,
                radio_mode="diaspora"
            )
        ],
        strategy='simple',
        resilience_strategy='fail_type',
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

@python_app
def sleep5s():
    import time
    time.sleep(5)

@python_app
def consume_memory():
    huge_memory_list = []
    cnt = 0
    try:
        while True:
            if cnt > 10:
                break
            huge_memory_list.append('A' * 1024 * 1024 * 1000)  # 每次添加10MB
            cnt += 1
    except MemoryError:
        print("Memory exhausted!")
    finally:
        del huge_memory_list
    print("Finished attempting to consume memory.")


if __name__ == "__main__":
    config = get_config()
    dfk = parsl.load(config)
    tasks = [sleep5s(), consume_memory()]
    [t.result() for t in tasks]
