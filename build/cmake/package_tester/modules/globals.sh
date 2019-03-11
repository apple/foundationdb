#!/usr/bin/env bash

# This module has to be included first and only once.
# This is because of a limitation of older bash versions
# that doesn't allow us to declare associative arrays
# globally.

if [ -z "${global_sh_included+x}"]
then
    global_sh_included=1
else
    echo "global.sh can only be included once"
    exit 1
fi

declare -A ini_name
declare -A ini_location
declare -A ini_packages
declare -A ini_format
declare -A test_start_state
declare -A test_exit_state
declare -a tests
declare -a vms
