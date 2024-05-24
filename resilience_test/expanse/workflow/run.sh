#!/bin/bash

workflows=("mapreduce")
failure_rates=("0.3" "0.5" "0.7")
# failure_types=("dependency")
failure_types=("dependency" "divide_zero" "environment" "memory" "simple" "ulimit" "walltime")
repeat=10

date > analysis.log

for workflow in "${workflows[@]}"; do
    for failure_rate in "${failure_rates[@]}"; do
        for failure_type in "${failure_types[@]}"; do
            for i in $(seq 1 $repeat); do
                current_timestamp_ms=$(date +%s).$(date +%N | awk '{print substr($0,1,3)}')
                echo "Running $workflow at $current_timestamp_ms with failure rate $failure_rate and failure type $failure_type ($i/$repeat)" >> analysis.log
                python -m webs.run failure-injection \
                    --executor parsl \
                    --true-workflow $workflow \
                    --failure-rate $failure_rate \
                    --failure-type $failure_type
            done
        done
    done
done
