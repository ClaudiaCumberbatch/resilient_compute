import argparse
import math
import os
import time
import sqlite3
import subprocess
import sys
import glob

import parsl
from parsl.config import Config
from parsl.executors import HighThroughputExecutor
from parsl.app.app import python_app
from parsl.launchers import SimpleLauncher
from parsl.launchers import SingleNodeLauncher
from parsl.addresses import address_by_hostname
from parsl.launchers import AprunLauncher
from parsl.launchers import SrunLauncher
from parsl.providers import TorqueProvider
from parsl.providers.slurm.slurm import SlurmProvider

# for local test on ChameleonCloud
from parsl.data_provider.http import HTTPInTaskStaging
from parsl.data_provider.ftp import FTPInTaskStaging
from parsl.data_provider.file_noop import NoOpFileStaging

from parsl.providers import LocalProvider
from parsl.channels import LocalChannel
from parsl.launchers import SingleNodeLauncher

from parsl.monitoring import MonitoringHub

def get_config(have_monitor, radio_mode):
    tag = 'test' 
    config = Config(
        executors=[
            HighThroughputExecutor(
                label="faster_htex",
                #worker_debug=True,
                cores_per_worker=1,
                address=address_by_hostname(),
                provider=SlurmProvider(
                    'cpu',
                    # scheduler_options='#SBATCH --exclusive' # what does this mean,
                    worker_init='conda activate parsl310', # 'source activate dask',
                    init_blocks=1,
                    max_blocks=1,
                    min_blocks=1,
                    nodes_per_block=nodes_per_block,
                    walltime=args.walltime,
                    mem_per_node='1'
            ),                    
            )
        ],
        strategy=None
    )
    '''

    if have_monitor:
        tag = radio_mode
        config = Config(
            executors=[
                HighThroughputExecutor(
                    address="127.0.0.1",
                    label="htex_Local",
                    working_dir=os.getcwd() + "/" + "latency",
                    storage_access=[FTPInTaskStaging(), HTTPInTaskStaging(), NoOpFileStaging()],
                    worker_debug=True,
                    cores_per_worker=1,
                    heartbeat_period=2,
                    heartbeat_threshold=5,
                    poll_period=100,
                    provider=LocalProvider(
                        channel=LocalChannel(),
                        init_blocks=0,
                        min_blocks=0,
                        max_blocks=5,
                        launcher=SingleNodeLauncher(),
                    ),
                    block_error_handler=False,
                    radio_mode=radio_mode
                )
            ],
            strategy='simple',
            app_cache=True, checkpoint_mode='task_exit',
            retries=2,
            monitoring=MonitoringHub(
                            hub_address="localhost",
                            monitoring_debug=False,
                            resource_monitoring_interval=1,
            ),
            usage_tracking=True
        )
    else:
        tag = 'no_monitor'
        config = Config(
            executors=[
                HighThroughputExecutor(
                    address="127.0.0.1",
                    label="htex_Local",
                    working_dir=os.getcwd() + "/" + "latency",
                    storage_access=[FTPInTaskStaging(), HTTPInTaskStaging(), NoOpFileStaging()],
                    worker_debug=True,
                    cores_per_worker=1,
                    heartbeat_period=2,
                    heartbeat_threshold=5,
                    poll_period=100,
                    provider=LocalProvider(
                        channel=LocalChannel(),
                        init_blocks=0,
                        min_blocks=0,
                        max_blocks=5,
                        launcher=SingleNodeLauncher(),
                    ),
                    block_error_handler=False
                )
            ],
            strategy='simple',
            app_cache=True, checkpoint_mode='task_exit',
            retries=2,
            usage_tracking=True
        )
    '''
    print(config)
    return config, tag

@python_app
def noop():
    pass

@python_app
def sleep10ms():
    import time
    time.sleep(0.01)

@python_app
def sleep100ms():
    import time
    time.sleep(0.1)

@python_app
def sleep1000ms():
    import time
    time.sleep(1.0)

@python_app
def sleep10s():
    import time
    time.sleep(10.0)

@python_app
def sleep100s():
    import time
    time.sleep(100.0)


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("-i", "--target_workers", type=int, default=1, help="target workers")
    parser.add_argument("-r", "--trials", type=int, default=1000, help="number of trials per batch submission")
    parser.add_argument("-c", "--cores_per_node", type=int, default=32, help="cores per node")
    parser.add_argument("-w", "--walltime", type=str, default='00:10:00', help="walltime")
    args = parser.parse_args()

    # parsl.set_stream_logger()
    table_name = f'trail{str(args.trials)}-{time.strftime("%Y-%m-%d_%H-%M-%S", time.localtime())}'
    print(f"table name: {table_name}")
    db = sqlite3.connect('data.db')
    db.execute(f"""create table if not exists "{table_name}"(
        monitor_tag text,
        start_submit float,
        returned float,
        connected_workers int,
        task,
        tag text)"""
    )

    target_workers = args.target_workers    
    if target_workers % args.cores_per_node != 0:
        nodes_per_block = 1
        tasks_per_node = target_workers % args.cores_per_node 
    else:
        nodes_per_block = int(target_workers / args.cores_per_node)
        tasks_per_node = args.cores_per_node 

    config_list = [get_config(have_monitor=False, radio_mode=''), 
                   get_config(have_monitor=True, radio_mode='htex'), 
                   get_config(have_monitor=True, radio_mode='diaspora')]
    
    for config, tag in config_list:
        parsl.clear()
        dfk = parsl.load(config)

        # priming
        tasks = [sleep1000ms() for _ in range(0, target_workers)]
        [t.result() for t in tasks]
        dfk.tasks = {}

        for app in [noop, sleep10ms, sleep100ms, sleep1000ms]:
        # for app in [noop]:
            sum1 = sum2 = 0
            for trial in range(args.trials):
                try:
                    start_submit = time.time()
                    task = app()
                    task.result()
                    returned = time.time()

                    data = (
                        tag,
                        start_submit,
                        returned,
                        target_workers,
                        trial,
                        app.__name__
                    )
                    # print('inserting {}'.format(str(data)))
                    db.execute(f"""
                        insert into
                        "{table_name}"(monitor_tag, start_submit, returned, connected_workers, task, tag)
                        values (?, ?, ?, ?, ?, ?)""", data
                    )
                    db.commit()
                    t1 = (returned - start_submit) * 1000
                    sum1 += t1
                    # print("Running time is %.6f ms" % t1)
                except Exception as e:
                    print(e)
            print("The average running time of {} {} is {}".format(tag, app.__name__, sum1/args.trials))

        del dfk

