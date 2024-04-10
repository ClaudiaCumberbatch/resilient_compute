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
                label="htex_1",
                working_dir=os.getcwd() + "/" + "resilience_test",
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
                    mem_per_node='1',
                    launcher=SingleNodeLauncher(),
                ),
                block_error_handler=False,
                radio_mode="diaspora"
            ),
            HighThroughputExecutor(
                max_workers=1,
                address="127.0.0.1",
                label="htex_2",
                working_dir=os.getcwd() + "/" + "resilience_test",
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
                    mem_per_node='10',
                    launcher=SingleNodeLauncher(),
                ),
                block_error_handler=False,
                radio_mode="diaspora"
            )
        ],
        strategy='simple',
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
def sleep2s():
    import time
    time.sleep(1)

@python_app(executors=['htex_1'])
def consume_memory():
    huge_memory_list = []
    try:
        while True:
            huge_memory_list.append('A' * 1024 * 1024 * 1000)  # 每次添加10MB
    except MemoryError:
        print("Memory exhausted!")
    finally:
        # 释放引用，尝试回收内存
        del huge_memory_list
    print("Finished attempting to consume memory.")


if __name__ == "__main__":
    config = get_config()
    dfk = parsl.load(config)
    tasks = [throw_exp() for _ in range(0, 1)]
    [t.result() for t in tasks]
