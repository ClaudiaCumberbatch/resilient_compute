import argparse
import math
import os
import time
import sqlite3
import subprocess
import sys
import glob
from datetime import datetime
from diaspora_event_sdk import KafkaConsumer
from diaspora_event_sdk import Client as GlobusClient
import threading

import parsl
from parsl.config import Config
from parsl.executors import HighThroughputExecutor
from parsl.app.app import python_app
from parsl.launchers import SimpleLauncher
from parsl.launchers import SingleNodeLauncher
from parsl.addresses import address_by_hostname, address_by_interface
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

def get_config(have_monitor, radio_mode, worker_per_exe):
    
    '''
    tag = 'no_monitor'
    config = Config(
        executors=[
            HighThroughputExecutor(
                max_workers=worker_per_exe,
                address=address_by_interface("eno8303"),
                label="faster_htex",
                working_dir=os.getcwd() + "/" + "scaling",
                storage_access=[FTPInTaskStaging(), HTTPInTaskStaging(), NoOpFileStaging()],
                worker_debug=True,
                cores_per_worker=1,
                heartbeat_period=2,
                heartbeat_threshold=5,
                poll_period=100,
                provider=SlurmProvider(
                    'cpu',
                    channel=LocalChannel(),
                    # scheduler_options='#SBATCH --exclusive' # what does this mean,
                    worker_init='source /sw/eb/sw/Anaconda3/2023.07-2/bin/activate parsl310', # 'source activate dask',
                    init_blocks=1,
                    max_blocks=1,
                    min_blocks=1,
                    nodes_per_block=1, # TODO: if >1, may need parsl.launchers.SrunLauncher or parsl.launchers.AprunLauncher
                    # launcher=SrunLauncher(),
                    walltime=args.walltime,
                    mem_per_node='1'
                ),   
                block_error_handler=False,
                radio_mode=radio_mode                 
            )
        ],
        strategy='simple',
        retries=1,
        monitoring=MonitoringHub(
                        hub_address="localhost",
                        monitoring_debug=False,
                        resource_monitoring_interval=10,
        ),
    )
    '''

    if have_monitor:
        tag = radio_mode
        config = Config(
            executors=[
                HighThroughputExecutor(
                    max_workers=worker_per_exe, # up-to-date max_workers_per_node, but the parsl lib here uses max_workers
                    address=address_by_interface("eno8303"),
                    label="faster_htex",
                    working_dir=os.getcwd() + "/" + "scaling",
                    storage_access=[FTPInTaskStaging(), HTTPInTaskStaging(), NoOpFileStaging()],
                    worker_debug=True,
                    cores_per_worker=1,
                    heartbeat_period=15,
                    heartbeat_threshold=120,
                    poll_period=100,
                    provider=SlurmProvider(
                        'cpu',
                        channel=LocalChannel(),
                        # scheduler_options='#SBATCH --exclusive' # what does this mean,
                        worker_init='source /sw/eb/sw/Anaconda3/2023.07-2/bin/activate parsl310', # 'source activate dask',
                        init_blocks=1,
                        max_blocks=1,
                        min_blocks=1,
                        nodes_per_block=32, # TODO: if >1, may need parsl.launchers.SrunLauncher or parsl.launchers.AprunLauncher
                        launcher=SrunLauncher(),
                        walltime=args.walltime,
                        mem_per_node='100'
                    ),   
                    block_error_handler=False,
                    radio_mode=radio_mode
                )
            ],
            strategy='simple',
            app_cache=True, checkpoint_mode='task_exit',
            retries=1,
            monitoring=MonitoringHub(
                            hub_address=address_by_interface("eno8303"),
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
                    max_workers=worker_per_exe,
                    address=address_by_interface("eno8303"),
                    label="faster_htex",
                    working_dir=os.getcwd() + "/" + "scaling",
                    storage_access=[FTPInTaskStaging(), HTTPInTaskStaging(), NoOpFileStaging()],
                    worker_debug=True,
                    cores_per_worker=1,
                    heartbeat_period=2,
                    heartbeat_threshold=5,
                    poll_period=100,
                    provider=SlurmProvider(
                        'cpu',
                        channel=LocalChannel(),
                        # scheduler_options='#SBATCH --exclusive' # what does this mean,
                        worker_init='source /sw/eb/sw/Anaconda3/2023.07-2/bin/activate parsl310', # 'source activate dask',
                        init_blocks=1,
                        max_blocks=1,
                        min_blocks=1,
                        nodes_per_block=4, # TODO: if >1, may need parsl.launchers.SrunLauncher or parsl.launchers.AprunLauncher
                        # launcher=SrunLauncher(),
                        walltime=args.walltime,
                        mem_per_node='40'
                    ),   
                    block_error_handler=False
                )
            ],
            strategy='simple',
            app_cache=True, checkpoint_mode='task_exit',
            retries=1,
            usage_tracking=True
        )
    
    # print(config)
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

def cal_record():
    global record_per_workflow
    record_per_workflow = {}
    c = GlobusClient()
    topic = "radio-test"
    # consumer = KafkaConsumer(topic, auto_offset_reset='earliest')
    consumer = KafkaConsumer(topic)
    for message in consumer:
        if message.key is None:
            continue
        message_key_str = message.key.decode('utf-8')
        record_per_workflow[message_key_str] = record_per_workflow.get(message_key_str, 0) + 1
        with open('record_per_workflow.txt', 'w') as f:
            f.write(str(record_per_workflow))


def run_one_trail(have_monitor, radio_mode, workers, tasks_per_trial, trail, app):
    config, monitor_tag = get_config(have_monitor, radio_mode, workers)
    # clear dfk every trail, so that we can get a new run_id
    # notice that this is very time consuming
    parsl.clear()
    dfk = parsl.load(config)

    # priming
    tasks = [sleep1000ms() for _ in range(0, workers)]
    [t.result() for t in tasks]
    dfk.tasks = {}

    start_submit = time.time()
    tasks = [app() for _ in range(0, tasks_per_trial)]
    end_submit = time.time()
    [t.result() for t in tasks]
    returned = time.time()

    data = (
        dfk.run_id,
        monitor_tag,
        start_submit,
        end_submit,
        returned,
        workers,
        tasks_per_trial,
        trail,
        app.__name__
    )

    db.execute(f"""
        insert into
        "{table_name}"(run_id, monitor_tag, start_submit, end_submit, returned, workers, tasks_per_trial, trial, app_name)
        values (?, ?, ?, ?, ?, ?, ?, ?, ?)""", data
    )
    db.commit()
    t = (returned - start_submit) * 1000
    print(f"{data} run {t} ms")
    del dfk
    return t, monitor_tag

if __name__ == "__main__":
    # python run_one.py -i 2 -r 2 -t 4 -m htex -a sleep10ms
    parser = argparse.ArgumentParser()
    parser.add_argument("-i", "--target_workers", type=int, default=1, help="target workers")
    parser.add_argument("-r", "--trial", type=int, default=1000, help="serial number of trail")
    parser.add_argument("-t", "--tasks_per_trial", type=int, default=1000, help="number of tasks per trial")
    parser.add_argument("-c", "--cores_per_node", type=int, default=32, help="cores per node")
    parser.add_argument("-w", "--walltime", type=str, default='00:10:00', help="walltime")

    parser.add_argument("-m", "--monitor", type=str, default='', help="monitor tag")
    parser.add_argument("-a", "--app", type=str, default='noop', help="app name")
    parser.add_argument("-tn", "--table_name", type=str, default='test_table', help="table name")
    
    args = parser.parse_args()

    workers = args.target_workers
    trial = args.trial
    tasks_per_trial = args.tasks_per_trial
    radio_mode = args.monitor
    have_monitor = True
    if radio_mode == '':
        have_monitor = False
    app_name = args.app
    if app_name == 'noop':
        app = noop
    elif app_name == 'sleep10ms':
        app = sleep10ms
    elif app_name == 'sleep100ms':
        app = sleep100ms
    table_name = args.table_name

    # print(f"table name: {table_name}")
    db = sqlite3.connect('data.db')
    db.execute(f"""create table if not exists "{table_name}"(
        run_id text,
        monitor_tag text,
        start_submit float,
        end_submit float,
        returned float,
        workers int,
        tasks_per_trial int,
        trial int,
        app_name text)"""
    )

    t, monitor_tag = run_one_trail(have_monitor, radio_mode, workers, args.tasks_per_trial, trial, app)
    # print(f"{monitor_tag} with {workers} workers run {app_name} for {t}ms")
