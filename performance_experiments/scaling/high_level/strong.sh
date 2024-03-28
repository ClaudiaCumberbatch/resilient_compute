#!/bin/bash

# Strong scaling: execute tasks over all workers

tasks_per_trial=128
target_workers_array=(1 2 4 8 16 32 64 128)
trial_array=(1 2 3 4 5 6 7 8 9 10)
monitor_array=("" "htex" "diaspora")
app_array=("noop" "sleep10ms" "sleep100ms")

for target_workers in "${target_workers_array[@]}"; do
    for trial in "${trial_array[@]}"; do
        for monitor in "${monitor_array[@]}"; do
            for app in "${app_array[@]}"; do
                if [ -z "$monitor" ]; then
                    python run_one.py -i $target_workers -r $trial -t $tasks_per_trial -a $app
                else
                    python run_one.py -i $target_workers -r $trial -t $tasks_per_trial -m $monitor -a $app
                fi
            done
        done
    done
done
