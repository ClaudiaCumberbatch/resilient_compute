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

from parsl.providers import SlurmProvider
from parsl.channels import LocalChannel
from parsl.launchers import SrunLauncher

from parsl.monitoring import MonitoringHub

def exp_config(worker=1, 
               mem=20, 
               retry=0, 
               walltime='00:10:00', 
               exclusive=True, 
               worker_init='module load cpu/0.15.4; module load slurm; module load anaconda3/2020.11; source activate /home/szhou3/.conda/envs/parsl310',
               retry_handler=None):
    return Config(
        executors=[
            HighThroughputExecutor(
                max_workers=worker,
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
                    worker_init=worker_init,
                    walltime=walltime,
                    init_blocks=1,
                    max_blocks=1,
                    nodes_per_block=1,
                    mem_per_node=mem,
                    exclusive=exclusive,
                ),
                block_error_handler=False,
                radio_mode="diaspora"
            ),
            HighThroughputExecutor(
                max_workers=worker,
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
                    worker_init=worker_init,
                    walltime=walltime,
                    init_blocks=1,
                    max_blocks=1,
                    nodes_per_block=1,
                    mem_per_node=2*mem,
                    exclusive=exclusive,
                ),
                block_error_handler=False,
                radio_mode="diaspora"
            )
        ],
        strategy='none',
        resilience_strategy='fail_type',
        app_cache=True, checkpoint_mode='task_exit',
        retries=retry,
        monitoring=MonitoringHub(
                        hub_address="localhost",
                        monitoring_debug=False,
                        resource_monitoring_interval=1,
        ),
        usage_tracking=True,
        retry_handler=retry_handler
    )

config = exp_config()