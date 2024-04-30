import os
import sys
import parsl
from parsl.app.app import python_app

module_path = '/home/szhou3/resilient_compute/resilience_test/expanse'
sys.path.append(module_path)
from expanse_config import exp_config

@python_app
def open_many_files(limit):
    import os
    os.system("ulimit -n 100")
    handles = []
    try:
        for i in range(limit):
            handles.append(open(f"tempfile_{i}.txt", "w"))
        return f"Opened {limit} files successfully"
    except Exception as e:
        return str(e)
    finally:
        for handle in handles:
            handle.close()


if __name__ == "__main__":
    dfk = parsl.load(exp_config())
    file_limit = 1500 
    result = open_many_files(file_limit)
    print(result.result())

