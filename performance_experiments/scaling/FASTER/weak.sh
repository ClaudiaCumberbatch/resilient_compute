#!/bin/bash

# Weak scaling: execute 10 tasks per worker

target_workers_array=(1 2 4 8 16 32 64 128)
trial_array=(1 2 3 4 5 6 7 8 9 10)
monitor_array=("" "htex" "diaspora")
app_array=("noop" "sleep10ms" "sleep100ms")

for target_workers in "${target_workers_array[@]}"; do
    for trial in "${trial_array[@]}"; do
        for monitor in "${monitor_array[@]}"; do
            for app in "${app_array[@]}"; do
                tasks_per_trial=$(($target_workers * 10))
                # echo "Running $app with $target_workers workers, $tasks_per_trial tasks, trial $trial, monitor $monitor"
                if [ -z "$monitor" ]; then
                    python run_one.py -i $target_workers -r $trial -t $tasks_per_trial -a $app
                else
                    python run_one.py -i $target_workers -r $trial -t $tasks_per_trial -m $monitor -a $app
                fi
            done
        done
    done
done
