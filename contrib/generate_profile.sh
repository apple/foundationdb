#!/bin/bash
fdbdir=`pwd`/..
export LD_LIBRARY_PATH=$fdbdir/lib:$LD_LIBRARY_PATH
export FDB_CLUSTER_FILE=$fdbdir/fdb.cluster
export LLVM_PROFILE_FILE=$fdbdir/sandbox/fdb-%p.profraw
../bin/fdbmonitor --conffile ../sandbox/foundationdb.conf --lockfile ../sandbox/fdbmonitor.pid &
export LLVM_PROFILE_FILE=$fdbdir/sandbox/cli-%m.profraw
../bin/fdbcli -C ../fdb.cluster --exec 'configure new ssd single'
export LLVM_PROFILE_FILE=$fdbdir/sandbox/mako-build-%m.profraw
../bin/mako -p 64 -t 1 --keylen 32 --vallen 16 --mode build --rows 10000  --trace  --trace_format json
export LLVM_PROFILE_FILE=$fdbdir/sandbox/mako-run-%m.profraw
../bin/mako -p 1 -t 2 --keylen 32 --vallen 16 --mode run --rows 10000 --transaction grvg7i2gr1:48cr1:48 --seconds 60 --trace ./logs --trace_format json

# Shutdown fdbserver to trigger profile dumping
pid=$(ps -u $USER | grep fdbserver | fgrep -v grep | awk '{print $2}')
gdb --batch --eval-command 'call exit(0)' --pid $pid

# Clean up
killall -9 fdbmonitor

# Profile for server
llvm-profdata merge -output=../fdb.profdata $fdbdir/sandbox/fdb-*.profraw
# Profile for client
llvm-profdata merge -output=../mako.profdata $fdbdir/sandbox/mako-*.profraw
