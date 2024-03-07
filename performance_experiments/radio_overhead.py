import argparse
import math
import os
import time
import sqlite3
import subprocess
import sys

import parsl
from parsl.config import Config
from parsl.executors import HighThroughputExecutor
from parsl.providers.slurm.slurm import SlurmProvider
from parsl.app.app import python_app
from parsl.launchers import SimpleLauncher
from parsl.launchers import SrunLauncher
from parsl.launchers import SingleNodeLauncher

# for local test on ChameleonCloud
from parsl.data_provider.http import HTTPInTaskStaging
from parsl.data_provider.ftp import FTPInTaskStaging
from parsl.data_provider.file_noop import NoOpFileStaging

from parsl.providers import LocalProvider
from parsl.channels import LocalChannel
from parsl.launchers import SingleNodeLauncher

from parsl.monitoring import MonitoringHub

parser = argparse.ArgumentParser()
parser.add_argument("-i", "--min_workers", type=int, default=1, help="minimum workers")
parser.add_argument("-a", "--max_workers", type=int, default=1024, help="maximum workers")
parser.add_argument("-r", "--trials", type=int, default=10, help="number of trials per batch submission")
parser.add_argument("-t", "--tasks_per_trial", type=int, default=1000, help="number of tasks per trial")
parser.add_argument("-c", "--cores_per_node", type=int, default=28, help="cores per node")
parser.add_argument("-w", "--walltime", type=str, default='00:20:00', help="walltime")
args = parser.parse_args()

# parsl.set_stream_logger()

db = sqlite3.connect('data.db')
db.execute("""create table if not exists tasks(
    executor text,
    start_submit float,
    end_submit float,
    returned float,
    connected_workers int,
    tasks_per_trial,
    tag text)"""
)


for _ in range(args.trials):
    target_workers = args.min_workers
    while target_workers <= args.max_workers:
        subprocess.call("qstat -u $USER | awk '{print $1}' | grep -o [0-9]* | xargs qdel", shell=True)
        if target_workers % 16 != 0:
            nodes_per_block = 1
            tasks_per_node = target_workers % 16
        else:
            nodes_per_block = int(target_workers / 16)
            tasks_per_node = 16
        
        '''
        config = Config(
            executors=[
                HighThroughputExecutor(
                    label="midway_htex",
                    worker_debug=True,
                    cores_per_worker=args.cores_per_node / tasks_per_node,
                    public_ip='172.25.220.71', # infiniband interface on midway
                    provider=SlurmProvider(
                        'broadwl',
                        launcher=SrunLauncher(),
                        worker_init='source activate parsl',
                        init_blocks=1,
                        max_blocks=1,
                        min_blocks=1,
                        nodes_per_block=nodes_per_block,
                        tasks_per_node=1,
                        walltime=args.walltime
                    ),
                )
            ],
            strategy=None
        )
        '''

        config = Config(
            executors=[
                HighThroughputExecutor(
                    address="127.0.0.1",
                    label="htex_Local",
                    working_dir=os.getcwd() + "/" + "test_htex_alternate",
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
            monitoring=MonitoringHub(
                            hub_address="localhost",
                            hub_port=55055,
                            monitoring_debug=False,
                            resource_monitoring_interval=1,
            ),
            usage_tracking=True
        )
        print(config)

        parsl.clear()
        dfk = parsl.load(config)
        executor = list(dfk.executors.values())[0]

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

        attempt = 0
        cmd = 'ls {} | wc -l'.format(os.path.join(executor.run_dir, executor.label, '*', '*worker*'))
        # while True:
        #     connected_workers = int(subprocess.check_output(cmd, shell=True))
        #     if connected_workers < target_workers:
        #         print('attempt {}: waiting for {} workers, but only found {}'.format(attempt, target_workers, connected_workers))
        #         time.sleep(10)
        #         attempt += 1
        #     else:
        #         break
        connected_workers = 48

        for app in [noop, sleep10ms, sleep100ms, sleep1000ms]:
            try:
                start_submit = time.time()
                tasks = [app() for _ in range(0, args.tasks_per_trial)]
                end_submit = time.time()
                [t.result() for t in tasks]
                returned = time.time()

                data = (
                    executor.label,
                    start_submit,
                    end_submit,
                    returned,
                    connected_workers,
                    args.tasks_per_trial,
                    app.__name__
                )
                print('inserting {}'.format(str(data)))
                db.execute("""
                    insert into
                    tasks(executor, start_submit, end_submit, returned, connected_workers, tasks_per_trial, tag)
                    values (?, ?, ?, ?, ?, ?, ?)""", data
                )
                db.commit()
            except Exception as e:
                print(e)

        target_workers *= 2
        executor.shutdown()
        del dfk
