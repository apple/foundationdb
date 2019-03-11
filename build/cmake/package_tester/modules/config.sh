#!/usr/bin/env bash

if [ -z ${config_sh_included+x} ]
then
    config_sh_included=1

    source ${source_dir}/modules/util.sh

    config_load_vms() {
        local __res=0
        enterfun
        for _ in 1
        do
            if [ -z "${docker_ini+x}"]
            then
                docker_file="${source_dir}/../docker.ini"
            fi
            # parse the ini file and read it into an
            # associative array
            eval "$(awk -F ' *= *' '{ if ($1 ~ /^\[/) section=$1; else if ($1 !~ /^$/) printf "ini_%s%s=\47%s\47\n", $1, section, $2  }' ${docker_file})"
            vms=( "${!ini_name[@]}" )
            if [ $? -ne 0 ]
            then
                echo "ERROR: Could not parse config-file ${docker_file}"
                __res=1
                break
            fi
        done
        exitfun
        return ${__res}
    }

    config_find_packages() {
        local __res=0
        enterfun
        for _ in 1
        do
            cd ${fdb_build}
            while read f
            do
                if [[ "${f}" =~ .*"clients".* || "${f}" =~ .*"server".* ]]
                then
                    if [ -z ${fdb_packages+x} ]
                    then
                        fdb_packages="${f}"
                    else
                        fdb_packages="${fdb_packages}:${f}"
                    fi
                fi
            done <<< "$(ls *.deb *.rpm)"
            if [ $? -ne 0 ]
            then
                __res=1
                break
            fi
        done
        exitfun
        return ${__res}
    }

    get_fdb_source() {
        local __res=0
        enterfun
        cd ${source_dir}
        while true
        do
            if [ -d .git ]
            then
                # we found the root
                pwd
                break
            fi
            if [ `pwd` = "/" ]
            then
                __res=1
                break
            fi
            cd ..
        done
        exitfun
        return ${__res}
    }

    fdb_build=0

    config_verify() {
        local __res=0
        enterfun
        for _ in 1
        do
            if [ -z ${fdb_source+x} ]
            then
                fdb_source=`get_fdb_source`
            fi
            if [ ! -d "${fdb_build}" ]
            then
                __res=1
                echo "ERROR: Could not find fdb build dir: ${fdb_build}"
                echo "        Either set the environment variable fdb_build or"
                echo "        pass it with -b <PATH_TO_BUILD>"
            fi
            if [ ! -f "${fdb_source}/flow/Net2.actor.cpp" ]
            then
                __res=1
                echo "ERROR: ${fdb_source} does not appear to be a fdb source"
                echo "       directory. Either pass it with -s or set"
                echo "       the environment variable fdb_source."
            fi
            if [ ${__res} -ne 0 ]
            then
                break
            fi
            config_load_vms
            __res=$?
            if [ ${__res} -ne 0 ]
            then
                break
            fi
        done
        exitfun
        return ${__res}
    }
fi
