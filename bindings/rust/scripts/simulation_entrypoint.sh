#!/bin/sh

# Simulation needs libc++.so at runtime
export LD_LIBRARY_PATH=/usr/local/lib/x86_64-unknown-linux-gnu

START=1
END=${1-1}
for i in $(eval echo "{$START..$END}")
do
  echo "----------------------"
  echo "----------------------"
  echo "Running iteration $i"
  echo "----------------------"
  echo "----------------------"
  if ! fdbserver -r simulation -f foundationdb-simulation/examples/atomic/test_file_73.toml -b on --trace-format json; then
      cat trace*.json | grep Rust
      exit 1;
  else
      # Removing traces about success run
      rm trace*.json
  fi

done
