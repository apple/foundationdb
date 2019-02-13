#!env bash

if [ -z ${arguments_sh_included+x} ]
then
    arguments_sh_included=1

    source ${source_dir}/modules/util.sh

    arguments_usage() {
        cat <<EOF
usage: test_packages.sh [-h] [commands]
       -h:       print this help message and
                 abort execution
       -b DIR:   point set the fdb build directory
                 (this is a required argument).
       -s DIR:   Path to fdb source directory.
       -p STR:   Colon-separated list of package
                 file names (without path) to
                 test.
       -c PATH:  Path to a ini-file with the docker
                 configuration
       -t TEST:  One of DEB, RPM, ALL
       -n TESTS: Colon separated list of test names
                 to run (will run all if this option
                 is not set)

       Will execute the passed commands
       in the order they were passed
EOF
    }

    arguments_parse() {
        local __res=0
        run_deb_tests=1
        run_rpm_tests=1
        while getopts ":hb:s:t:" opt
        do
            case ${opt} in
                h )
                    arguments_usage
                    __res=2
                    break
                    ;;
                b )
                    fdb_build="${OPTARG}"
                    ;;
                s )
                    fdb_source="${OPTARG}"
                    ;;
                p )
                    fdb_packages="${OPTARG}"
                    ;;
                c )
                    docker_ini="${OPTARG}"
                    ;;
                t )
                    if [ "${OPTARG}" = "DEB" ]
                    then
                        run_rpm_tests=0
                    elif [ "${OPTARG}" = "RPM" ]
                    then
                        run_deb_tests=0
                    elif [ "${OPTARG}" != "ALL" ]
                    then
                        echo -e "${RED}No such test: ${OPTARG}${NC}"
                        echo "Note: Currently known tests are: RPM, DEB, and ALL"
                        exit 1
                    fi
                    ;;
                n )
                    tests_to_run="${OPTARG}"
                    ;;
                \? )
                    curr_index="$((OPTIND-1))"
                    echo "Unknown option ${@:${curr_index}:1}"
                    arguments_usage
                    __res=1
                    break
                    ;;
            esac
        done
        shift $((OPTIND -1))
        commands=("$@")
        return ${__res}
    }
fi
