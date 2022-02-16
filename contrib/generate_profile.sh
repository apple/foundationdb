#!/bin/bash
if [ $# -eq 0 ]
  then
    echo "Please specify the path to foundation build directory"
    exit 1
fi
fdbdir=$1
export LD_LIBRARY_PATH=$fdbdir/lib:$LD_LIBRARY_PATH
export FDB_CLUSTER_FILE=$fdbdir/fdb.cluster
export LLVM_PROFILE_FILE=$fdbdir/sandbox/fdb-%p.profraw
$fdbdir/bin/fdbmonitor --conffile $fdbdir/sandbox/foundationdb.conf --lockfile $fdbdir/sandbox/fdbmonitor.pid &
# This profile will be ignored
export LLVM_PROFILE_FILE=$fdbdir/sandbox/cli-%m.profraw
$fdbdir/bin/fdbcli -C $fdbdir/fdb.cluster --exec 'configure new ssd single'
export LLVM_PROFILE_FILE=$fdbdir/sandbox/mako-build-%m.profraw
$fdbdir/bin/mako -p 64 -t 1 --keylen 32 --vallen 16 --mode build --rows 10000  --trace  --trace_format json
export LLVM_PROFILE_FILE=$fdbdir/sandbox/mako-run-%m.profraw
$fdbdir/bin/mako -p 1 -t 2 --keylen 32 --vallen 16 --mode run --rows 10000 --transaction grvg7i2gr1:48cr1:48 --seconds 60 --trace $fdbdir/sandbox/logs --trace_format json

# Shutdown fdbserver to trigger profile dumping
pid=$(ps -u $USER | grep fdbserver | fgrep -v grep | awk '{print $2}')
gdb --batch --eval-command 'call exit(0)' --pid $pid

# Clean up
fdbmonitor=$(cat $fdbdir/sandbox/fdbmonitor.pid)
kill -9 $fdbmonitor

# Profile for server
llvm-profdata merge -output=$fdbdir/fdb.profdata $fdbdir/sandbox/fdb-*.profraw
# Profile for client
llvm-profdata merge -output=$fdbdir/mako.profdata $fdbdir/sandbox/mako-*.profraw
