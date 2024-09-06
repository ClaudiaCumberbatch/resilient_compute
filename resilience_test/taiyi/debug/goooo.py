from taps.run.config import Config
from taps.run.main import run
import sys
retry_path = '/work/cse-zhousc/resilient_compute/resilience_test/cse_cluster/filesystem_retry'
sys.path.append(retry_path)
from retry_config import resilient_retry


base_config = Config.from_toml('parsl-config.toml')
base_config.engine.executor.retry_handler = resilient_retry

run(base_config, run_dir='/work/cse-zhousc/resilient_compute/resilience_test/taiyi/debug/runs')