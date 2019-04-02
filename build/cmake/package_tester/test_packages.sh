#!/usr/bin/env bash

source_dir="$( cd "$( dirname "${BASH_SOURCE[0]}" )" >/dev/null && pwd )"

source ${source_dir}/modules/globals.sh
source ${source_dir}/modules/config.sh
source ${source_dir}/modules/util.sh
source ${source_dir}/modules/arguments.sh
source ${source_dir}/modules/docker.sh

main() {
    local __res=0
    enterfun
    for _ in 1
    do
        arguments_parse "$@"
        if [ $? -ne 0 ]
        then
            __res=1
            break
        fi
        config_verify
        if [ $? -ne 0 ]
        then
            __res=1
            break
        fi
        docker_run
        __res=$?
    done
    exitfun
    return ${__res}
}

main "$@"
