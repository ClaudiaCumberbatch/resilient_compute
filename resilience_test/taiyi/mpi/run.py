import parsl
from parsl import python_app
from parsl.config import Config
from parsl.executors import HighThroughputExecutor
from parsl.providers import LocalProvider
from parsl.addresses import address_by_hostname

# 设置Parsl配置
config = Config(
    executors=[
        HighThroughputExecutor(
            label="htex",
            address=address_by_hostname(),
            provider=LocalProvider(
                init_blocks=1,
                max_blocks=1,
                nodes_per_block=1,  # 这里假设本地仅测试1个节点
            ),
            worker_debug=True,
            # mpi=True  # 启用MPI
        )
    ],
)

parsl.load(config)


@python_app
def run_mpi():
    import os
    # 使用mpirun运行mpi_sum.py
    # result = os.system("mpirun -n 4 python3 mpi_sum.py")
    result = os.popen("mpirun -n 4 python3 mpi_sum.py").read()
    return result


# 提交MPI任务
future = run_mpi()

# 获取并打印结果
print("MPI task result:", future.result())