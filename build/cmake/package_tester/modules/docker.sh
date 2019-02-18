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
                    set -x
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
                    set +x
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

    docker_build_and_run() {
        local __res=0
        enterfun
        for _ in 1
        do
            if [[ "$location" = /* ]]
            then
                cd "${location}"
            else
                cd ${source_dir}/../${location}
            fi
            docker_logs="${log_dir}/docker_build_${name}"
            docker build . -t ${name} 1> "${docker_logs}.log" 2> "${docker_logs}.err"
            successOr "Building Docker image ${name} failed - see ${docker_logs}.log and ${docker_logs}.err"
            # we start docker in interactive mode, otherwise CTRL-C won't work
            if [ ! -z "${tests_to_run+x}"]
            then
                tests=()
                IFS=';' read -ra tests <<< "${tests_to_run}"
            fi
            for t in "${tests[@]}"
            do
                if [ "${#docker_ids[@]}" -ge "${docker_parallelism}" ]
                then
                    docker_wait_any
                fi
                echo "Starting Test ${PKG,,}_${t}"
                docker_id=$( docker run -d -v "${fdb_source}:/foundationdb"\
                    -v "${fdb_build}:/build"\
                    ${name}\
                    bash /foundationdb/build/cmake/package_tester/${PKG,,}_tests.sh -n ${t} ${packages_to_test[@]} )
                docker_ids+=( "${docker_id}" )
                docker_threads+=( "${PKG} - ${t} (ID: ${docker_id})" )
            done
        done
        exitfun
        return ${__res}
    }

    docker_run_tests() {
        local __res=0
        enterfun
        counter=1
        while true
        do
            if [ -z "${ini_name[${PKG}_${counter}]+x}" ]
            then
                # we are done
                break
            fi
            name="${ini_name[${PKG}_${counter}]}"
            location="${ini_location[${PKG}_${counter}]}"
            docker_build_and_run
            __res=$?
            counter=$((counter+1))
            if [ ${__res} -ne 0 ]
            then
                break
            fi
        done
        if [ ${counter} -eq 1 ]
        then
            echo -e "${YELLOW}WARNING: No docker config found!${NC}"
        fi
        exitfun
        return ${__res}
    }

    docker_debian_tests() {
        local __res=0
        enterfun
        PKG=DEB
        packages_to_test=("${deb_packages[@]}")
        docker_run_tests
        __res=$?
        exitfun
        return ${__res}
    }

    docker_rpm_tests() {
        local __res=0
        enterfun
        PKG=RPM
        packages_to_test=("${rpm_packages[@]}")
        docker_run_tests
        __res=$?
        exitfun
        return ${__res}
    }

    docker_run() {
        local __res=0
        enterfun
        for _ in 1
        do
            log_dir="${fdb_build}/pkg_tester"
            mkdir -p "${log_dir}"
            # create list of package files to test
            IFS=':' read -ra packages <<< "${fdb_packages}"
            deb_packages=()
            rpm_packages=()
            for i in "${packages[@]}"
            do
                if [[ "${i}" =~ .*".deb" ]]
                then
                    if [ ${run_deb_tests} -ne 0 ]
                    then
                        deb_packages+=("${i}")
                    fi
                else
                    if [ ${run_rpm_tests} -ne 0 ]
                    then
                        rpm_packages+=("${i}")
                    fi
                fi
            done
            do_deb_tests=0
            do_rpm_tests=0
            if [ "${#deb_packages[@]}" -gt 0 ]
            then
                do_deb_tests=1
                echo "Will test the following debian packages:"
                echo "========================================"
                for i in "${deb_packages[@]}"
                do
                    echo " - ${i}"
                done
                echo
            fi
            if [ "${#rpm_packages[@]}" -gt 0 ]
            then
                do_rpm_tests=1
                echo "Will test the following rpm packages"
                echo "===================================="
                for i in "${rpm_packages[@]}"
                do
                    echo " - ${i}"
                done
            fi
            if [ "${do_deb_tests}" -eq 0 ] && [ "${do_rpm_tests}" -eq 0 ]
            then
                echo "nothing to do"
            fi
            if [ "${do_deb_tests}" -ne 0 ]
            then
                docker_debian_tests
            fi
            if [ "${do_rpm_tests}" -ne 0 ]
            then
                docker_rpm_tests
            fi
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
        return ${__res}
    }
fi
