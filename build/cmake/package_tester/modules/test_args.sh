#!/usr/bin/env bash

if [ -z ${test_args_sh_included+x} ]
then
    test_args_sh_included=1

    source ${source_dir}/modules/util.sh

    test_args_usage() {
        me=`basename "$0"`
        echo "usage: ${me} [-h] files..."
        cat <<EOF
       -n TEST: The name of the test to run

       Will execute the passed commands
       in the order they were passed
EOF
    }

    test_args_parse() {
        local __res=0
        run_deb_tests=1
        run_rpm_tests=1
        while getopts ":hn:" opt
        do
            case ${opt} in
                h )
                    test_args_usage
                    __res=2
                    break
                    ;;
                n )
                    echo "test_name=${OPTARG}"
                    test_name="${OPTARG}"
                    ;;
                \? )
                    curr_index="$((OPTIND-1))"
                    echo "Unknown option ${@:${curr_index}:1}"
                    arguments_usage
                    __res=1
                    break
                    ;;
            esac
        done
        shift $((OPTIND -1))
        package_files=("$@")
        return ${__res}
    }
fi
