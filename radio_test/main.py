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

from parsl.providers import LocalProvider
from parsl.channels import LocalChannel
from parsl.launchers import SingleNodeLauncher

from parsl.monitoring import MonitoringHub

def get_config():
    config = Config(
        executors=[
            HighThroughputExecutor(
                max_workers=1,
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
                    init_blocks=1,
                    min_blocks=1,
                    max_blocks=1,
                    launcher=SingleNodeLauncher(),
                ),
                block_error_handler=False,
                radio_mode="diaspora"
            )
        ],
        strategy='simple',
        app_cache=True, checkpoint_mode='task_exit',
        retries=3,
        monitoring=MonitoringHub(
                        hub_address="localhost",
                        monitoring_debug=False,
                        resource_monitoring_interval=1,
        ),
        usage_tracking=True
    )
 
    return config

@python_app
def noop():
    pass

@python_app
def sleep1s():
    import time
    time.sleep(1)


if __name__ == "__main__":
    config = get_config()
    dfk = parsl.load(config)
    tasks = [sleep1s() for _ in range(0, 3)]
    [t.result() for t in tasks]
