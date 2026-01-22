#!/usr/bin/env bash
#
# The intent of this script is to copy various foundationdb project related
# artifacts from the build directory to a directory under "fdb-automation"
# repository. This is mainly to work around the limitations of building 
# docker images in okteto environment. This script assumes that the user
# has cloned "fdb-automation" repository under "src" in okteto environment.

set -Eeuo pipefail
script_dir=$(cd "$(dirname "${BASH_SOURCE[0]}")" &>/dev/null && pwd -P)
build_output_directory="${script_dir}/../.."
target_directory="${HOME}/src/fdb-automation/tests/scripts/fdb_artifacts"
reset=$(tput sgr0)
blue=$(tput setaf 4)

function logg () {
    printf "${blue}##### $(date +"%H:%M:%S") #  %-56.55s #####${reset}\n" "${1}"
}

function pushd () {
    command pushd "$@" > /dev/null
}

function popd () {
    command popd > /dev/null
}

function copy_binaries_and_build_files () {
    rm -rf "${target_directory}"
    mkdir -p "${target_directory}"
    pushd "${target_directory}" || exit 127

    build_files=( 'version.txt' )

    for file in "${build_files[@]}"; do
        logg "COPYING ${file}"
        cp -pr "${build_output_directory}/${file}" "${file}"
    done

    files_from_build_packages=( 'fdb.bash' 'fdb_single.bash' 'entrypoint.bash' 'sidecar.py' 'fdbkubernetesmonitor' 'fdb-aws-s3-credentials-fetcher' )

    for file in "${files_from_build_packages[@]}"; do
        logg "COPYING ${file}"
    	cp -pr "${build_output_directory}/packages/docker/${file}" "${file}"
    done

    mkdir -p "${target_directory}/stripped"
    mkdir -p "${target_directory}/unstripped"

    fdb_binaries=( 'fdbbackup' 'fdbcli' 'fdbserver' 'fdbmonitor' )

    for file in "${fdb_binaries[@]}"; do
        logg "COPYING STRIPPED ${file}"
        cp -pr "${build_output_directory}/packages/bin/${file}" "stripped/${file}"
        # cp -pr "${build_output_directory}/packages/bin/${file}" "${file}"
    done

    for file in "${fdb_binaries[@]}"; do
        logg "COPYING UNSTRIPPED ${file}"
        cp -pr "${build_output_directory}/bin/${file}" "unstripped/${file}"
    done

    client_libraries=( 'libfdb_c.so' )

    for file in "${client_libraries[@]}"; do
        logg "COPYING STRIPPED ${file}"
        cp -pr "${build_output_directory}/packages/lib/${file}" "stripped/${file}"
        # cp -pr "${build_output_directory}/packages/lib/${file}" "${file}"
    done

    for file in "${client_libraries[@]}"; do
        logg "COPYING UNSTRIPPED ${file}"
        cp -pr "${build_output_directory}/lib/${file}" "unstripped/${file}"
    done

    client_includes=( 'fdb_c_apiversion.g.h' 'fdb_c_options.g.h' )

    for file in "${client_includes[@]}"; do
        logg "COPYING ${file}"
        cp -pr "${build_output_directory}/bindings/c/foundationdb/${file}" "${file}"
    done

    client_includes=( 'fdb_c.h' 'fdb_c_types.h' )

    for file in "${client_includes[@]}"; do
        logg "COPYING ${file}"
        cp -pr "${build_output_directory}/../src/foundationdb/bindings/c/foundationdb/${file}" "${file}"
    done

    popd || exit 128
}

echo "${blue}################################################################################${reset}"
logg "STARTING ${0}"
echo "${blue}################################################################################${reset}"

if [ -n "${OKTETO_NAMESPACE+x}" ]; then
    logg "RUNNING IN OKTETO/AWS"
    copy_binaries_and_build_files
else
    logg "DO NOT RUN THIS SCRIPT OUTSIDE OF OKTETO/AWS"
    exit 1
fi

echo "${blue}################################################################################${reset}"
logg "COMPLETED ${0}"
echo "${blue}################################################################################${reset}"
