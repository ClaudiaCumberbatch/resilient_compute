#!/bin/bash

source $(conda info --base)/etc/profile.d/conda.sh

PORT=55059

TRAIL=10

# FAILURE_RATES=("0.0" "0.1" "0.2" "0.4" "0.5" "0.6") # 6
FAILURE_RATES=("0.1")

BASES=("cholesky" "docking" "fedlearn" "mapreduce" "moldesign") # 5
# BASES=("cholesky")

# timeout, random
# FAILURE_TYPES=("dependency" "failure" "import" "memory" "ulimit" "worker-killed" "zero-division") # 7
FAILURE_TYPES=("worker-killed")

for ((i=1; i<=TRAIL; i++)); do
  for failure_rate in "${FAILURE_RATES[@]}"; do
    for base in "${BASES[@]}"; do
      for failure_type in "${FAILURE_TYPES[@]}"; do
        # cholesky, fedlearn, and mapreduce share taps310 environment
        if [ "$base" == "cholesky" ] || [ "$base" == "fedlearn" ] || [ "$base" == "mapreduce" ]; then
          conda activate taps310
          rm -rf tmp
        elif [ "$base" == "docking" ]; then
          conda activate taps-docking2
        elif [ "$base" == "moldesign" ]; then
          conda activate taps-moldesign
        elif [ "$base" == "montage" ]; then
          conda activate taps-montage
          export PATH="$CONDA_PREFIX/Montage/bin:$PATH"
        fi
        
        TOML_FILE="$base.toml"

        sed -i "s/^failure_type = \".*\"/failure_type = \"$failure_type\"/" "$TOML_FILE"
        sed -i "s/^failure_rate = .*/failure_rate = $failure_rate/" "$TOML_FILE"

        echo "Start time: $(date), Configuration: base = $base, Trail $i, failure_type = $failure_type, failure_rate = $failure_rate."

        # kill jobs that execute over 1 min
        # bjobs -noheader -o "id run_time" | sed 's/ second(s)//' | awk '$2 > 1 { system("bkill " $1) }'

        # check port
        if lsof -i :$PORT > /dev/null; then
          echo "Port $PORT is still in use, attempting to release it."

          PID=$(lsof -t -i :$PORT)
          if [ -n "$PID" ]; then
            echo "Killing process $PID that is using port $PORT."
            kill -9 $PID
          fi

          if lsof -i :$PORT > /dev/null; then
            echo "Failed to release port $PORT."
          else
            echo "Port $PORT has been successfully released."
          fi
        else
          echo "Port $PORT has been successfully released."
        fi

        # run python
        LOG_FILE="$base.log"
        if [ "$base" == "docking" ]; then
          stdbuf -oL python3 -m taps.run --config "$TOML_FILE" > "$LOG_FILE" 2>&1 &
        else
          stdbuf -oL python -m taps.run --config "$TOML_FILE" > "$LOG_FILE" 2>&1 &
        fi
        TAP_PID=$!

        # Record the start time of the task
        TASK_START_TIME=$(date +%s)

        # check the log file every 1 min, kill the process if no output for 5 minutes
        LAST_MODIFIED=$(date +%s -r "$LOG_FILE")
        TIMEOUT_INTERVAL=300 # 5 minutes
        MAX_RUNTIME=1800 # 30 minutes

        while kill -0 $TAP_PID 2>/dev/null; do
          sleep 10

          CURRENT_TIME=$(date +%s)
          MODIFIED_TIME=$(date +%s -r "$LOG_FILE")
          ELAPSED=$((CURRENT_TIME - MODIFIED_TIME))
          TOTAL_RUNTIME=$((CURRENT_TIME - TASK_START_TIME))

          # if [ $ELAPSED -gt $TIMEOUT_INTERVAL ]; then
          #   echo "$(date): No output for 5 minutes. Terminating the process."
          #   kill -9 $TAP_PID
          #   break
          # fi

          if [ $TOTAL_RUNTIME -gt $MAX_RUNTIME ]; then
            echo "$(date): Task has been running for more than 30 minutes. Terminating the process."
            kill -9 $TAP_PID
            break
          fi
        done

        mv cmd* runs/cmd

      done
    done
  done
done

echo "End time: $(date), everything is done."