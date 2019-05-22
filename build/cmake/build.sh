#!/usr/bin/env bash

arguments_usage() {
    cat <<EOF
usage: build.sh [-h] [commands]
       -h: print this help message and
           abort execution

       Will execute the passed commands
       in the order they were passed
EOF
}

arguments_parse() {
    local __res=0
    while getopts ":h" opt
    do
        case ${opt} in
            h )
                arguments_usage
                __res=2
                break
                ;;
            \? )
                echo "Unknown option ${opt}"
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

configure() {
    local __res=0
    for _ in 1
    do
        cmake ../foundationdb ${CMAKE_EXTRA_ARGS}
        __res=$?
        if [ ${__res} -ne 0 ]
        then
            break
        fi
    done
    return ${__res}
}

build_fast() {
    local __res=0
    for _ in 1
    do
        make -j`nproc`
        __res=$?
        if [ ${__res} -ne 0 ]
        then
            break
        fi
    done
    return ${__res}
}

build() {
    local __res=0
    for _ in 1
    do
        configure
        __res=$?
        if [ ${__res} -ne 0 ]
        then
            break
        fi
        build_fast
        __res=$?
        if [ ${__res} -ne 0 ]
        then
            break
        fi
    done
    return ${__res}
}

package_fast() {
    local __res=0
    for _ in 1
    do
        make -j`nproc` packages
        cpack
        cpack -G RPM -D GENERATE_EL6=ON
        __res=$?
        if [ ${__res} -ne 0 ]
        then
            break
        fi
    done
    return ${__res}
}

package() {
    local __res=0
    for _ in 1
    do
        build
        __res=$?
        if [ ${__res} -ne 0 ]
        then
            break
        fi
        package_fast
        __res=$?
        if [ ${__res} -ne 0 ]
        then
            break
        fi
    done
    return ${__res}
}

rpm() {
    local __res=0
    for _ in 1
    do
        configure
        __res=$?
        if [ ${__res} -ne 0 ]
        then
            break
        fi
        build_fast
        __res=$?
        if [ ${__res} -ne 0 ]
        then
            break
        fi
        fakeroot cpack -G RPM -D GENERATE_EL6=ON
        fakeroot cpack -G RPM
        __res=$?
        if [ ${__res} -ne 0 ]
        then
            break
        fi
    done
    return ${__res}
}

deb() {
    local __res=0
    for _ in 1
    do
        configure
        __res=$?
        if [ ${__res} -ne 0 ]
        then
            break
        fi
        build_fast
        __res=$?
        if [ ${__res} -ne 0 ]
        then
            break
        fi
        fakeroot cpack -G DEB
        __res=$?
        if [ ${__res} -ne 0 ]
        then
            break
        fi
    done
    return ${__res}
}

test-fast() {
    local __res=0
    for _ in 1
    do
        ctest -j`nproc` ${CTEST_EXTRA_ARGS}
        __res=$?
        if [ ${__res} -ne 0 ]
        then
            break
        fi
    done
    return ${__res}
}

test() {
    local __res=0
    for _ in 1
    do
        build
        __res=$?
        if [ ${__res} -ne 0 ]
        then
            break
        fi
        test-fast
        __res=$?
        if [ ${__res} -ne 0 ]
        then
            break
        fi
    done
    return ${__res}
}

main() {
    local __res=0
    for _ in 1
    do
        arguments_parse "$@"
        __res=$?
        if [ ${__res} -ne 0 ]
        then
            if [ ${__res} -eq 2 ]
            then
                # in this case there was no error
                # We still want to exit the script
                __res=0
            fi
            break
        fi
        echo "Num commands ${#commands[@]}"
        for command in "${commands[@]}"
        do
            echo "Command: ${command}"
            case ${command} in
                configure )
                    configure
                    __res=$?
                    ;;
                build )
                    build
                    __res=$?
                    ;;
                build/fast )
                    build_fast
                    __res=$?
                    ;;
                package )
                    package
                    __res=$?
                    ;;
                package/fast )
                    package_fast
                    __res=$?
                    ;;
                rpm )
                    rpm
                    __res=$?
                    ;;
                deb )
                    deb
                    __res=$?
                    ;;
                test-fast)
                    test-fast
                    __res=$?
                    ;;
                test)
                    test
                    __res=$?
                    ;;
                * )
                    echo "ERROR: Command not found ($command)"
                    __res=1
                    ;;
            esac
            if [ ${__res} -ne 0 ]
            then
                break
            fi
        done
    done
    return ${__res}
}

main "$@"
