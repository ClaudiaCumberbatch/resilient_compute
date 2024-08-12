#!/bin/bash

log_file="without.log"
workflows=("mapreduce")
failure_rates=("0.2")
failure_types=("dependency" "divide_zero" "environment" "manager_kill" "memory" "node_kill" "ulimit" "worker_kill") # "walltime"
repeat=10

# log_file="test.log"
# workflows=("mapreduce")
# failure_rates=("0.5")
# failure_types=("node_kill")
# repeat=1

date > $log_file

for workflow in "${workflows[@]}"; do
    for failure_rate in "${failure_rates[@]}"; do
        for failure_type in "${failure_types[@]}"; do
            for i in $(seq 1 $repeat); do
                current_timestamp_ms=$(date +%s).$(date +%N | awk '{print substr($0,1,3)}')
                echo "Running $workflow at $current_timestamp_ms with failure rate $failure_rate and failure type $failure_type ($i/$repeat)" >> $log_file
                python -m webs.run failure-injection \
                    --executor parsl \
                    --true-workflow $workflow \
                    --failure-rate $failure_rate \
                    --failure-type $failure_type
            done
        done
    done
done
