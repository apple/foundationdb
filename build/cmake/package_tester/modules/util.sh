#!/usr/bin/env bash

if [ -z ${util_sh_included+x} ]
then
    util_sh_included=1

    # for colored output
    RED='\033[0;31m'
    GREEN='\033[0;32m'
    YELLOW='\033[1;33m'
    NC='\033[0m' # No Color


    enterfun() {
        pushd . > /dev/null
    }

    exitfun() {
        popd > /dev/null
    }

    fail() {
        false
        successOr ${@:1}
    }

    successOr() {
        local __res=$?
        if [ ${__res} -ne 0 ]
        then
            if [ "$#" -gt 1 ]
            then
                >&2 echo -e "${RED}${@:1} ${NC}"
            fi
            exit ${__res}
        fi
        return 0
    }

fi
