# mpi_sum.py
from mpi4py import MPI

comm = MPI.COMM_WORLD
rank = comm.Get_rank()
size = comm.Get_size()

# 每个进程计算一部分
local_sum = sum(range(rank * 10, (rank + 1) * 10))
print(f"Rank {rank} partial sum: {local_sum}")

# 使用MPI的reduce函数计算全局总和
total_sum = comm.reduce(local_sum, op=MPI.SUM, root=0)

if rank == 0:
    print(f"Total sum: {total_sum}")