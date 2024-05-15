import os
import sys
import parsl
from parsl.app.app import python_app

config_path = '/home/szhou3/resilient_compute/resilience_test/expanse/failure_simulation'
sys.path.append(config_path)
retry_path = '/home/szhou3/resilient_compute/resilience_test/expanse/retry'
sys.path.append(retry_path)
from expanse_config import exp_config
from retry_config import resilient_retry

@python_app
def open_many_files(limit):
    import os
    import socket
    os.system("ulimit -n 100")
    handles = []
    try:
        for i in range(limit):
            handles.append(open(f"/tmp/tempfile_{socket.gethostname()}_{i}.txt", "w"))
        return f"Opened {limit} files successfully"
    finally:
        for handle in handles:
            handle.close()


if __name__ == "__main__":
    dfk = parsl.load(exp_config(retry=3, worker=3, exclusive=False, retry_handler=resilient_retry))
    file_limit = 548001
    result = open_many_files(file_limit)
    print(result.result())

