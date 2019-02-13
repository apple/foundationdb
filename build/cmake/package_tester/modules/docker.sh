#!/usr/bin/env bash

if [ -z "${docker_sh_included+x}" ]
then
    docker_sh_included=1
    source ${source_dir}/modules/util.sh
    source ${source_dir}/modules/config.sh
    source ${source_dir}/modules/tests.sh

    failed_tests=()

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
            docker build . -t ${name}
            # we start docker in interactive mode, otherwise CTRL-C won't work
            if [ ! -z "${tests_to_run+x}"]
            then
                tests=()
                IFS=';' read -ra tests <<< "${tests_to_run}"
            fi
            for t in "${tests[@]}"
            do
                docker run -it -v "${fdb_source}:/foundationdb"\
                    -v "${fdb_build}:/build"\
                    ${name}\
                    bash /foundationdb/build/cmake/package_tester/${PKG,,}_tests.sh -n ${t} ${packages_to_test[@]}
                if [ "$?" -ne 0 ]
                then
                    __res=1
                    failed_tests+=( "${PKG} - ${t}" )
                fi

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
            #if [ -z "${DEB_${}}" ]
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
                echo -e "\n"
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
            else
                read -n 1 -s -r -p "Press any key to continue (or Ctrl-C to cancel)"
                echo -e "\n"
            fi
            if [ "${do_deb_tests}" -ne 0 ]
            then
                docker_debian_tests
                if [ $? -eq 0 ]
                then
                    echo -e "${GREEN}Debian tests succeeded${NC}"
                else
                    echo -e "${RED}Debian tests failed${NC}"
                    __res=1
                fi
            fi
            if [ "${do_rpm_tests}" -ne 0 ]
            then
                docker_rpm_tests
                if [ $? -eq 0 ]
                then
                    echo -e "${GREEN}RPM tests succeeded${NC}"
                else
                    echo -e "${RED}RPM tests failed${NC}"
                    __res=1
                fi
            fi
            if [ "${__res}" -eq 0 ]
            then
                echo -e "${GREEN}SUCCESS${NC}"
            else
                echo -e "${RED}FAILURE"
                echo "The following tests failed:"
                for t in "${failed_tests[@]}"
                do
                    echo -e "\t- ${t}"
                done
                echo -e "${NC}"
            fi
        done
        exitfun
        return ${__res}
    }
fi
