#!/bin/bash

source $(conda info --base)/etc/profile.d/conda.sh

# BASES=("cholesky" "docking" "fedlearn" "mapreduce" "moldesign" "montage")
BASES=("mapreduce")

# timeout, random
# FAILURE_TYPES=("dependency" "failure" "import" "manager-killed" "memory" "ulimit" "worker-killed" "zero-division") 
FAILURE_TYPES=("worker-killed")

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

    echo "当前配置: base = $base, TOML_FILE = $TOML_FILE, failure_type = $failure_type"

    if [ "$base" == "docking" ]; then
      # python3 -m taps.run --config "$TOML_FILE" > /dev/null 2>&1
      python3 -m taps.run --config "$TOML_FILE"
    else
      # python -m taps.run --config "$TOML_FILE" > /dev/null 2>&1
      python -m taps.run --config "$TOML_FILE"
    fi
  done
done
