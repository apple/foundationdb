#!/usr/bin/env bash

if [ -z "${docker_sh_included+x}" ]
then
    docker_sh_included=1
    source ${source_dir}/modules/util.sh
    source ${source_dir}/modules/config.sh
    source ${source_dir}/modules/tests.sh

    failed_tests=()

    docker_ids=()
    docker_threads=()
    docker_logs=()
    docker_error_logs=()

    docker_wait_any() {
        local __res=0
        enterfun
        while [ "${#docker_threads[@]}" -gt 0 ]
        do
            IFS=";" read -ra res <${pipe_file}
            docker_id=${res[0]}
            result=${res[1]}
            i=0
            for (( idx=0; idx<${#docker_ids[@]}; idx++ ))
            do
                if [ "${docker_id}" = "${docker_ids[idx]}" ]
                then
                    i=idx
                    break
                fi
            done
            if [ "${result}" -eq 0 ]
            then
                echo -e "${GREEN}Test succeeded: ${docker_threads[$i]}"
                echo -e "\tDocker-ID: ${docker_ids[$i]} "
                echo -e "\tLog-File: ${docker_logs[$i]}"
                echo -e "\tErr-File: ${docker_error_logs[$i]} ${NC}"
            else
                echo -e "${RED}Test FAILED: ${docker_threads[$i]}"
                echo -e "\tDocker-ID: ${docker_ids[$i]} "
                echo -e "\tLog-File: ${docker_logs[$i]}"
                echo -e "\tErr-File: ${docker_error_logs[$i]} ${NC}"
                failed_tests+=( "${docker_threads[$i]}" )
            fi
            n=$((i+1))
            docker_ids=( "${docker_ids[@]:0:$i}" "${docker_ids[@]:$n}" )
            docker_threads=( "${docker_threads[@]:0:$i}" "${docker_threads[@]:$n}" )
            docker_logs=( "${docker_logs[@]:0:$i}" "${docker_logs[@]:$n}" )
            docker_error_logs=( "${docker_error_logs[@]:0:$i}" "${docker_error_logs[@]:$n}" )
            break
        done
        exitfun
        return "${__res}"
    }

    docker_wait_all() {
        local __res=0
        while [ "${#docker_threads[@]}" -gt 0 ]
        do
            docker_wait_any
            if [ "$?" -ne 0 ]
            then
                __res=1
            fi
        done
        return ${__res}
    }

    docker_run() {
        local __res=0
        enterfun
        for _ in 1
        do
            echo "Testing the following:"
            echo "======================"
            for K in "${vms[@]}"
            do
                curr_packages=( $(cd ${fdb_build}/packages; ls | grep -P ${ini_packages[${K}]} ) )
                echo "Will test the following ${#curr_packages[@]} packages in docker-image ${K}:"
                for p in "${curr_packages[@]}"
                do
                    echo "    ${p}"
                done
                echo
            done
            log_dir="${fdb_build}/pkg_tester"
            pipe_file="${fdb_build}/pkg_tester.pipe"
            lock_file="${fdb_build}/pkg_tester.lock"
            if [ -p "${pipe_file}" ]
            then
                rm "${pipe_file}"
                successOr "Could not delete old pipe file"
            fi
            if [ -f "${lock_file}" ]
            then
                rm "${lock_file}"
                successOr "Could not delete old pipe file"
            fi
            touch "${lock_file}"
            successOr "Could not create lock file"
            mkfifo "${pipe_file}"
            successOr "Could not create pipe file"
            mkdir -p "${log_dir}"
            # setup the containers
            # TODO: shall we make this parallel as well?
            for vm in "${vms[@]}"
            do
                curr_name="${ini_name[$vm]}"
                curr_location="${ini_location[$vm]}"
                if [[ "$curr_location" = /* ]]
                then
                    cd "${curr_location}"
                else
                    cd ${source_dir}/../${curr_location}
                fi
                docker_buid_logs="${log_dir}/docker_build_${curr_name}"
                docker build . -t ${curr_name} 1> "${docker_buid_logs}.log" 2> "${docker_buid_logs}.err"
                successOr "Building Docker image ${curr_name} failed - see ${docker_buid_logs}.log and ${docker_buid_logs}.err"
            done
            if [ ! -z "${tests_to_run+x}"]
            then
                tests=()
                IFS=';' read -ra tests <<< "${tests_to_run}"
            fi
            for vm in "${vms[@]}"
            do
                curr_name="${ini_name[$vm]}"
                curr_format="${ini_format[$vm]}"
                curr_packages=( $(cd ${fdb_build}/packages; ls | grep -P ${ini_packages[${vm}]} ) )
                for curr_test in "${tests[@]}"
                do
                    if [ "${#docker_ids[@]}" -ge "${docker_parallelism}" ]
                    then
                        docker_wait_any
                    fi
                    log_file="${log_dir}/${curr_name}_${curr_test}.log"
                    err_file="${log_dir}/${curr_name}_${curr_test}.err"
                    docker_id=$( docker run -d -v "${fdb_source}:/foundationdb"\
                        -v "${fdb_build}:/build"\
                        ${curr_name} /sbin/init )
                    echo "Starting Test ${curr_name}/${curr_test} Docker-ID: ${docker_id}"
                    {
                        docker exec "${docker_id}" bash \
                            /foundationdb/build/cmake/package_tester/${curr_format}_tests.sh -n ${curr_test} ${curr_packages[@]}\
                            2> ${err_file} 1> ${log_file}
                        res=$?
                        if [ "${pruning_strategy}" = "ALL" ]
                        then
                            docker kill "${docker_id}" > /dev/null
                        elif [ "${res}" -eq 0 ] && [ "${pruning_strategy}" = "SUCCEEDED" ]
                        then
                            docker kill "${docker_id}" > /dev/null
                        elif [ "${res}" -ne 0 ] && [ "${pruning_strategy}" = "FAILED" ]
                        then
                            docker kill "${docker_id}" > /dev/null
                        fi
                        flock "${lock_file}" echo "${docker_id};${res}"  >> "${pipe_file}"
                    } &
                    docker_ids+=( "${docker_id}" )
                    docker_threads+=( "${curr_name}/${curr_test}" )
                    docker_logs+=( "${log_file}" )
                    docker_error_logs+=( "${err_file}" )
                done
            done
            docker_wait_all
            rm ${pipe_file}
            if [ "${#failed_tests[@]}" -eq 0 ]
            then
                echo -e "${GREEN}SUCCESS${NC}"
            else
                echo -e "${RED}FAILURE"
                echo "The following tests failed:"
                for t in "${failed_tests[@]}"
                do
                    echo "    - ${t}"
                done
                echo -e "${NC}"
                __res=1
            fi
        done
        exitfun
        return "${__res}"
    }
fi
