#!/usr/bin/env bash

source_dir="$( cd "$( dirname "${BASH_SOURCE[0]}" )" >/dev/null && pwd )"

source ${source_dir}/modules/globals.sh
source ${source_dir}/modules/util.sh
source ${source_dir}/modules/deb.sh
source ${source_dir}/modules/tests.sh
source ${source_dir}/modules/test_args.sh

main() {
    local __res=0
    enterfun
    for _ in 1
    do
        test_args_parse "$@"
        __res=$?
        if [ ${__res} -eq 2 ]
        then
            __res=0
            break
        elif [ ${__res} -ne 0 ]
        then
            break
        fi
        tests_main
    done
    exitfun
    return ${__res}
}

main "$@"
