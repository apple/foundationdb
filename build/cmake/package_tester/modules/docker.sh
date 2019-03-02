#!/usr/bin/env bash

if [ -z "${docker_sh_included+x}" ]
then
    docker_sh_included=1
    source ${source_dir}/modules/util.sh
    source ${source_dir}/modules/config.sh
    source ${source_dir}/modules/tests.sh

    failed_tests=()

    docker_threads=()
    docker_ids=()

    docker_wait_any() {
        # docker wait waits on all containers (unlike what is documented)
        # so we need to do polling
        success=0
        while [ "${success}" -eq 0 ]
        do
            for ((i=0;i<"${#docker_ids[@]}";++i))
            do
                docker_id="${docker_ids[$i]}"
                status="$(docker ps -a -f id=${docker_id} --format '{{.Status}}' | awk '{print $1;}')"
                if [ "${status}" = "Exited" ]
                then
                    success=1
                    ret_code="$(docker wait ${docker_id})"
                    if [ "${ret_code}" -ne 0 ]
                    then
                        failed_tests+=( "${docker_threads[$i]}" )
                        echo -e "${RED}Test failed: ${docker_threads[$i]} ${NC}"
                    else
                        echo -e "${GREEN}Test succeeded: ${docker_threads[$i]} ${NC}"
                    fi
                    # remove it
                    n=$((i+1))
                    docker_ids=( "${docker_ids[@]:0:$i}" "${docker_ids[@]:$n}" )
                    docker_threads=( "${docker_threads[@]:0:$i}" "${docker_threads[@]:$n}" )
                    # prune
                    if [ "${pruning_strategy}" = "ALL" ]
                    then
                        docker container rm "${docker_id}" > /dev/null
                    elif [ "${ret_code}" -eq 0 ] && [ "${pruning_strategy}" = "SUCCEEDED" ]
                    then
                        docker container rm "${docker_id}" > /dev/null
                    elif [ "${ret_code}" -ne 0 ] && [ "${pruning_strategy}" = "FAILED" ]
                    then
                        docker container rm "${docker_id}" > /dev/null
                    fi
                fi
            done
            sleep 1
        done
    }

    docker_wait_all() {
        local __res=0
        while [ "${#docker_ids[@]}" -gt 0 ]
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
                pwd
                docker_logs="${log_dir}/docker_build_${curr_name}"
                docker build . -t ${curr_name} 1> "${docker_logs}.log" 2> "${docker_logs}.err"
                successOr "Building Docker image ${name} failed - see ${docker_logs}.log and ${docker_logs}.err"
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
                    echo "Starting Test ${curr_name}/${curr_test}"
                    docker_id=$( docker run -d -v "${fdb_source}:/foundationdb"\
                        -v "${fdb_build}:/build"\
                        ${curr_name}\
                        bash /foundationdb/build/cmake/package_tester/${curr_format}_tests.sh -n ${curr_test} ${curr_packages[@]} )
                    docker_ids+=( "${docker_id}" )
                    docker_threads+=( "${PKG} - ${t} (ID: ${docker_id})" )
                done
            done
            docker_wait_all
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
            fi
        done
        exitfun
        return "${__res}"
    }
fi
