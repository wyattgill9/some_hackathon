#!/bin/sh
# Entry point. Runs with no arguments; exit 0 means "done, check my output".
set -eu
: "${DATA_DIR:=/data}" "${OUTPUT_DIR:=/output}" "${SCRATCH_DIR:=/scratch}"
export DATA_DIR OUTPUT_DIR SCRATCH_DIR
# Read-only root filesystem: everything that wants a temp or home dir gets /scratch.
export TMPDIR="$SCRATCH_DIR" HOME="$SCRATCH_DIR" SPARK_LOCAL_DIRS="$SCRATCH_DIR"
# With --network none the container's own hostname does not resolve, and the JVM
# (Spark, log4j) dies looking it up. Give Java a hosts file that maps it to loopback.
printf '127.0.0.1 localhost %s\n::1 localhost\n' "$(hostname)" > "$SCRATCH_DIR/hosts"
export JAVA_TOOL_OPTIONS="-Djava.io.tmpdir=$SCRATCH_DIR -Djdk.net.hosts.file=$SCRATCH_DIR/hosts -Duser.home=$SCRATCH_DIR -XX:-DontCompileHugeMethods ${JAVA_TOOL_OPTIONS:-}"
exec python3 /app/pipeline/pipeline.py
