#!/usr/bin/env bash

source_dir="$( cd "$( dirname "${BASH_SOURCE[0]}" )" >/dev/null && pwd )"

source ${source_dir}/modules/util.sh
source ${source_dir}/modules/deb.sh
source ${source_dir}/modules/tests.sh

main() {
    local __res=0
    enterfun
    for _ in 1
    do
        package_files=( "$@" )
        if [ "${__res}" -ne 0 ]
        then
            break
        fi
        tests_main
    done
    exitfun
    return ${__res}
}

main "$@"
