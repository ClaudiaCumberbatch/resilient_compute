#!/bin/bash
pid=$$
PPPID=$(ps -o ppid= ${pid})
echo $PPPID
sudo cgclassify -g memory:myapp_mem $PPPID
CGROUP=/sys/fs/cgroup/memory/myapp_mem
sudo mkdir -p $CGROUP
echo "52428800000" | sudo tee $CGROUP/memory.limit_in_bytes > /dev/null  # 500MB


echo $$ | sudo tee $CGROUP/cgroup.procs > /dev/null

# sudo swapoff -a
# sudo cgexec -g memory:myapp_mem bash
exec "$@"