#!/bin/bash

source $(conda info --base)/etc/profile.d/conda.sh

PORT=55055

BASES=("cholesky" "docking" "fedlearn" "mapreduce" "moldesign" "montage")
# BASES=("mapreduce")

# timeout, random
FAILURE_TYPES=("dependency" "failure" "import" "manager-killed" "memory" "ulimit" "worker-killed" "zero-division") 
# FAILURE_TYPES=("memory" "ulimit" "worker-killed" "zero-division")

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

    echo "Start time: $(date), Configuration: base = $base, TOML_FILE = $TOML_FILE, failure_type = $failure_type"

    # kill jobs that execute over 1 min
    bjobs -noheader -o "id run_time" | sed 's/ second(s)//' | awk '$2 > 1 { system("bkill " $1) }'

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

    # check the log file every 1 min, kill the process if no output for 5 minutes
    LAST_MODIFIED=$(date +%s -r "$LOG_FILE")
    TIMEOUT_INTERVAL=300 # 70 for test

    while kill -0 $TAP_PID 2>/dev/null; do
      sleep 60

      CURRENT_TIME=$(date +%s)
      MODIFIED_TIME=$(date +%s -r "$LOG_FILE")
      ELAPSED=$((CURRENT_TIME - MODIFIED_TIME))

      if [ $ELAPSED -gt $TIMEOUT_INTERVAL ]; then
        echo "$(date): No output for 5 minutes. Terminating the process."
        kill -9 $TAP_PID

        break
      fi

    done

  done
done
