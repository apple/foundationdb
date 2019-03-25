#!/usr/bin/env bash

if [ -z "${testing_sh_included+x}" ]
then
    testing_sh_included=1

    source ${source_dir}/modules/util.sh

    desired_state() {
        case $1 in
            CLEAN )
                :
                ;;
            INSTALLED )
                install
                ;;
        esac
    }

    tests_healthy() {
        enterfun
        local __res=0
        for _ in 1
        do
            cd /
            fdbcli --exec status
            if [ $? -ne 0 ]
            then
                return 1
            fi
            healthy="$(fdbcli --exec status | grep HEALTHY | wc -l)"
            if [ -z "${healthy}" ]
            then
                __res=1
                break
            fi
        done
        exitfun
        return ${__res}
    }

    tests_clean() {
        uninstall purge
        successOr "FoundationDB was not uninstalled correctly"
        # systemd/initd are not running, so we have to kill manually here
        pidof fdbmonitor | xargs kill
        tests_clean_nouninstall
        rm -rf /etc/foundationdb
        rm -rf /var/lib/foundationdb
        rm -rf /var/log/foundationdb
    }

    tests_main() {
        new_state="${test_start_state[${test_name}]}"
        echo "Setting desired state ${new_state} for ${test_name}"
        desired_state "${new_state}"
        ${test_name}
        successOr "${test_name} Failed"
        echo  "======================================================================="
        echo  "Test $t successfully finished"
        echo  "======================================================================="
        current_state="${test_exit_state[${test_name}]}"
    }
fi
