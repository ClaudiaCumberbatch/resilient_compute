
export JOBNAME=$parsl.htex_Local.block-0.1710260304.9517066
set -e
export CORES=$(getconf _NPROCESSORS_ONLN)
[[ "1" == "1" ]] && echo "Found cores : $CORES"
WORKERCOUNT=1
FAILONANY=0
PIDS=""

CMD() {
PARSL_MONITORING_HUB_URL=fake PARSL_MONITORING_RADIO_MODE=diaspora PARSL_RUN_ID=0909bd8e-e5d4-457c-814c-9f36fba65b45 PARSL_RUN_DIR=/home/cc/resilient_compute/performance_experiments/runinfo/003 process_worker_pool.py --debug  -a 127.0.0.1 -p 0 -c 1 -m None --poll 100 --task_port=54051 --result_port=54944 --cert_dir None --logdir=/home/cc/resilient_compute/performance_experiments/runinfo/003/htex_Local --block_id=0 --hb_period=2  --hb_threshold=5 --cpu-affinity none --available-accelerators --uid 90577a41c5cc  --monitoring_url fake --run_id 0909bd8e-e5d4-457c-814c-9f36fba65b45 --radio_mode diaspora --sleep_dur 0 
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
