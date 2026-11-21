#!/bin/sh
# Diff the pipeline against the frozen reference (ref/) on generated seeds with no published answer.
#   scripts/diff_vs_ref.sh "2 3 4 5 15" 1000000
# Datasets are cached under $DATA_ROOT (default ~/data) as s<seed>-<events>; the reference's output is cached too.
set -eu
SEEDS=${1:-"2 3 4 5 15"}
SIZE=${2:-1000000}
DATA=${DATA_ROOT:-$HOME/data}
REF=${REF:-ref/target/release/ref}
PIPE=${PIPE:-target/release/pipeline}
fail=0
for s in $SEEDS; do
  d=$DATA/s$s-$SIZE
  [ -d "$d" ] || generate --seed "$s" --events "$SIZE" --clicks $((SIZE / 4)) --output "$d" >/dev/null
  [ -d "$d.ref/user_daily" ] || { rm -rf "$d.ref"; DATA_DIR=$d OUTPUT_DIR=$d.ref "$REF" 2>/dev/null; }
  for cfg in "NCPU=8" "NCPU=1" "NCPU=3 SS_RANGE=4096 SS_PBITS=4"; do
    rm -rf "$d.out"
    if env $cfg DATA_DIR="$d" OUTPUT_DIR="$d.out" "$PIPE" 2>/dev/null; then
      r=$(python3 scripts/compare.py "$d.out" "$d.ref" | tail -1)
    else
      r="exit $?"
    fi
    echo "seed $s $cfg: $r"
    [ "$r" = MATCH ] || fail=1
  done
done
exit $fail
