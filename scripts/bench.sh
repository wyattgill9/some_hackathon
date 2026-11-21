#!/bin/sh
# Wall time of the pipeline on a dataset, pinned to 4 cores and their SMT siblings like the scorer's 8 vCPUs.
#   scripts/bench.sh ~/data/ss20m 5
set -eu
DATA=$1
RUNS=${2:-5}
PIPE=${PIPE:-target/release/pipeline}
CPUS=${CPUS:-0-3,12-15}
for i in $(seq "$RUNS"); do
  rm -rf "$DATA.bench"
  s=$(date +%s.%N)
  NCPU=8 DATA_DIR=$DATA OUTPUT_DIR=$DATA.bench taskset -c "$CPUS" "$PIPE" 2>/dev/null
  e=$(date +%s.%N)
  awk "BEGIN { print $e - $s }"
done | sort -n | awk '{ a[NR] = $1 } END { printf "min %.3f  median %.3f  max %.3f  (n=%d)\n", a[1], a[int((NR + 1) / 2)], a[NR], NR }'
