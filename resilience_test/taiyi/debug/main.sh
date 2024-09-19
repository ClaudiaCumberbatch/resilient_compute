#!/bin/bash

source $(conda info --base)/etc/profile.d/conda.sh

BASES=("cholesky" "docking" "fedlearn" "mapreduce" "moldesign" "montage")
# BASES=("moldesign")

# timeout, random
FAILURE_TYPES=("dependency" "failure" "import" "manager-killed" "memory" "ulimit" "worker-killed" "zero-division") 
# FAILURE_TYPES=("memory" "ulimit" "worker-killed" "zero-division")

for base in "${BASES[@]}"; do
  for failure_type in "${FAILURE_TYPES[@]}"; do
    # cholesky, fedlearn, and mapreduce share taps310 environment
    if [ "$base" == "cholesky" ] || [ "$base" == "fedlearn" ] || [ "$base" == "mapreduce" ]; then
      conda activate taps310
    elif [ "$base" == "docking" ]; then
      conda activate taps-docking2
    elif [ "$base" == "moldesign" ]; then
      conda activate taps-moldesign
    elif [ "$base" == "montage" ]; then
      conda activate taps-montage
    fi
     

    TOML_FILE="$base.toml"

    sed -i "s/^failure_type = \".*\"/failure_type = \"$failure_type\"/" "$TOML_FILE"

    echo "Start time: $(date), Configuration: base = $base, TOML_FILE = $TOML_FILE, failure_type = $failure_type"

        if [ "$base" == "docking" ]; then
          timeout 1800 python3 -m taps.run --config "$TOML_FILE" > /dev/null 2>&1
          # timeout 1800 python3 -m taps.run --config "$TOML_FILE"
        else
          timeout 1800 python -m taps.run --config "$TOML_FILE" > /dev/null 2>&1
          # timeout 1800 python -m taps.run --config "$TOML_FILE"
        fi
    
    if [ $? -eq 124 ]; then
        echo "$(date): Python script timed out after 30 minutes."
    fi
  done
done
