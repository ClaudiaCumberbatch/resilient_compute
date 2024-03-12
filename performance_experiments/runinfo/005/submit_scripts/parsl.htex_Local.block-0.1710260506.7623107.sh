
export JOBNAME=$parsl.htex_Local.block-0.1710260506.7623107
set -e
export CORES=$(getconf _NPROCESSORS_ONLN)
[[ "1" == "1" ]] && echo "Found cores : $CORES"
WORKERCOUNT=1
FAILONANY=0
PIDS=""

CMD() {
PARSL_MONITORING_HUB_URL=udp://localhost:54587 PARSL_MONITORING_RADIO_MODE=diaspora PARSL_RUN_ID=16d5d04a-4521-4d38-8a6d-f33e5fbe3d7c PARSL_RUN_DIR=/home/cc/resilient_compute/performance_experiments/runinfo/005 process_worker_pool.py --debug  -a 127.0.0.1 -p 0 -c 1 -m None --poll 100 --task_port=54129 --result_port=54009 --cert_dir None --logdir=/home/cc/resilient_compute/performance_experiments/runinfo/005/htex_Local --block_id=0 --hb_period=2  --hb_threshold=5 --cpu-affinity none --available-accelerators --uid 1eaab58ec9ad --monitor_resources --monitoring_url udp://localhost:54587 --run_id 16d5d04a-4521-4d38-8a6d-f33e5fbe3d7c --radio_mode diaspora --sleep_dur 1 
}
for COUNT in $(seq 1 1 $WORKERCOUNT); do
    [[ "1" == "1" ]] && echo "Launching worker: $COUNT"
    CMD $COUNT &
    PIDS="$PIDS $!"
done

ALLFAILED=1
ANYFAILED=0
for PID in $PIDS ; do
    wait $PID
    if [ "$?" != "0" ]; then
        ANYFAILED=1
    else
        ALLFAILED=0
    fi
done

[[ "1" == "1" ]] && echo "All workers done"
if [ "$FAILONANY" == "1" ]; then
    exit $ANYFAILED
else
    exit $ALLFAILED
fi
