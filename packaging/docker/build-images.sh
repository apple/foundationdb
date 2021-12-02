#!/usr/bin/env bash
set -Eeuo pipefail
script_dir=$(cd "$(dirname "${BASH_SOURCE[0]}")" &>/dev/null && pwd -P)
reset=$(tput sgr0)
blue=$(tput setaf 4)

function logg() {
    printf "${blue}##### $(date +"%H:%M:%S") #  %-56.55s #####${reset}\n" "${1}"
}

function pushd () {
    command pushd "$@" > /dev/null
}

function popd () {
    command popd > /dev/null
}

function create_fake_website_directory() {
    fdb_binaries=( 'fdbbackup' 'fdbcli' 'fdbserver' 'fdbmonitor' )
    fake_fdb_binaries=( 'backup_agent' 'dr_agent' 'fastrestore_tool' 'fdbdr' 'fdbrestore' )
    logg "PREPARING WEBSITE"
    website_directory="${script_dir}/website"
    rm -rf "${website_directory}"
    mkdir -p "${website_directory}/downloads/${fdb_version}/linux/bin"
    pushd "${website_directory}/downloads/${fdb_version}/linux/bin" || exit 127
    ############################################################################
    # there are four intended paths here:
    # 1) fetch the unstripped binaries and client library from artifactory_base_url
    # 2) fetch the stripped binaries and multiple client library versions
    #    from artifactory_base_url
    # 3) copy the unstripped binaries and client library from the current local
    #    build_output of foundationdb
    # 4) copy the stripped binaries and client library from the current local
    #    build_output of foundationdb
    ############################################################################
    logg "FETCHING BINARIES"
    case "${stripped_binaries_and_from_where}" in
        "unstripped_artifactory")
            logg "DOWNLOADING BINARIES TAR FILE"
            curl -Ls "${artifactory_base_url}/${fdb_version}/release/api/foundationdb-binaries-${fdb_version}-linux.tar.gz" | tar -xzf -
            ;;
        "stripped_artifactory")
            for file in "${fdb_binaries[@]}"; do
                logg "DOWNLOADING ${file}"
                curl -Ls "${artifactory_base_url}/${fdb_version}/release/files/linux/bin/${file}" -o "${file}"
                chmod 755 "${file}"
            done
            ;;
        "unstripped_local")
            for file in "${fdb_binaries[@]}"; do
                logg "COPYING ${file}"
                cp -pr "${build_output_directory}/bin/${file}" "${file}"
                chmod 755 "${file}"
            done
            ;;
        "stripped_local")
            for file in "${fdb_binaries[@]}"; do
                logg "COPYING ${file}"
                cp -pr "${build_output_directory}/packages/bin/${file}" "${file}"
                chmod 755 "${file}"
            done
            ;;
    esac
    # dont download files that are binary duplicates of fdbbackup, recreate the
    # symlinks (to fdbbackup in the same directory)
    logg "CREATING fdbbackup SYMLINKS"
    for fake in "${fake_fdb_binaries[@]}"; do
        logg "CREATING ${fake}"
        ln -sf fdbbackup "${fake}"
    done
    popd || exit 128
    # re-create the same file that is present in the downloads path of
    # foundationdb.org such that it can be copied into the Docker image and
    # referenced with a file:// url by the container image build
    logg "CREATING BINARIES TAR FILE"
    tar -czf "${website_directory}/downloads/${fdb_version}/linux/fdb_${fdb_version}.tar.gz" --directory "${website_directory}/downloads/${fdb_version}/linux/bin" .
    rm -rf "${website_directory}/downloads/${fdb_version}/linux/bin"

    ############################################################################
    # this follows the same logic as the case statement above, they are separate
    # because it allows for the simplification of the steps that create the
    # symlinks and the binaries tarball
    ############################################################################
    logg "FETCHING CLIENT LIBRARY"
    case "${stripped_binaries_and_from_where}" in
        "unstripped_artifactory")
            for version in "${fdb_library_versions[@]}"; do
                logg "FETCHING ${version} CLIENT LIBRARY"
                destination_directory="${website_directory}/downloads/${version}/linux"
                destination_filename="libfdb_c_${version}.so"
                mkdir -p "${destination_directory}"
                pushd "${destination_directory}" || exit 127
                curl -Ls "${artifactory_base_url}/${version}/release/api/fdb-server-${version}-linux.tar.gz" | tar -xzf - ./lib/libfdb_c.so --strip-components 2
                mv "libfdb_c.so" "${destination_filename}"
                chmod 755 "${destination_filename}"
                popd || exit 128
            done
            ;;
        "stripped_artifactory")
            for version in "${fdb_library_versions[@]}"; do
                logg "FETCHING ${version} CLIENT LIBRARY"
                destination_directory="${website_directory}/downloads/${version}/linux"
                destination_filename="libfdb_c_${version}.so"
                mkdir -p "${destination_directory}"
                pushd "${destination_directory}" || exit 127
                curl -Ls "${artifactory_base_url}/${version}/release/files/linux/lib/libfdb_c.so" -o "${destination_filename}"
                chmod 755 "${destination_filename}"
                popd || exit 128
            done
            ;;
        "unstripped_local")
            logg "COPYING UNSTRIPPED CLIENT LIBRARY"
            cp -pr "${build_output_directory}/lib/libfdb_c.so" "${website_directory}/downloads/${fdb_version}/linux/libfdb_c_${fdb_version}.so"
            ;;
        "stripped_local")
            logg "COPYING STRIPPED CLIENT LIBRARY"
            cp -pr "${build_output_directory}/packages/lib/libfdb_c.so" "${website_directory}/downloads/${fdb_version}/linux/libfdb_c_${fdb_version}.so"
            ;;
    esac
    # override fdb_website variable that is passed to Docker build
    fdb_website="file:///tmp/website"
}

function compile_ycsb() {
    logg "COMPILING YCSB"
    if [ "${use_development_java_bindings}" == "true" ]; then
        logg "INSTALL JAVA BINDINGS"
        foundationdb_java_version="${fdb_version}-SNAPSHOT"
        mvn install:install-file \
        --batch-mode \
        -Dorg.slf4j.simpleLogger.log.org.apache.maven.cli.transfer.Slf4jMavenTransferListener=warn \
        -Dfile="${build_output_directory}/packages/fdb-java-${foundationdb_java_version}.jar" \
        -DgroupId=org.foundationdb \
        -DartifactId=fdb-java \
        -Dversion="${foundationdb_java_version}" \
        -Dpackaging=jar \
        -DgeneratePom=true
    else
        foundationdb_java_version="${fdb_version}"
    fi
    rm -rf "${script_dir}/YCSB"
    mkdir -p "${script_dir}/YCSB"
    pushd "${script_dir}/YCSB" || exit 127
    if [ -d "${HOME}/src/YCSB" ]; then
       rsync -av "${HOME}"/src/YCSB/. .
    else
       git clone https://github.com/FoundationDB/YCSB.git .
    fi
    sed -i "s/<foundationdb.version>[0-9]\+.[0-9]\+.[0-9]\+<\/foundationdb.version>/<foundationdb.version>${foundationdb_java_version}<\/foundationdb.version>/g" pom.xml
    mvn --batch-mode -Dorg.slf4j.simpleLogger.log.org.apache.maven.cli.transfer.Slf4jMavenTransferListener=warn -pl site.ycsb:foundationdb-binding -am clean package
    mkdir -p core/target/dependency
    # shellcheck disable=SC2046
    cp $(find "${HOME}/.m2" -name jax\*.jar) core/target/dependency/
    # shellcheck disable=SC2046
    cp $(find "${HOME}/.m2" -name htrace\*.jar) core/target/dependency/
    # shellcheck disable=SC2046
    cp $(find "${HOME}/.m2" -name HdrHistogram\*.jar) core/target/dependency/
    rm -rf .git
    popd || exit 128
}

function build_and_push_images(){
    declare -a tags_to_push=()
    for image in "${image_list[@]}"; do
        logg "BUILDING ${image}"
        image_tag="${tag_base}${image}:${fdb_version}"
        if [ "${image}" == "foundationdb-kubernetes-sidecar" ]; then
            image_tag="${image_tag}-1"
        fi
        if [ "${dockerfile_name}" == "Dockerfile.eks" ]; then
            image_tag="${image_tag}-debug"
        fi
        if [ "${image}" == "ycsb" ]; then
            compile_ycsb
        fi
        logg "TAG ${image_tag#${registry}/foundationdb/}"
        docker build \
            --label "org.foundationdb.version=${fdb_version}" \
            --label "org.foundationdb.build_date=${build_date}" \
            --label "org.foundationdb.commit=${commit_sha}" \
            --progress plain \
            --build-arg FDB_VERSION="${fdb_version}" \
            --build-arg FDB_LIBRARY_VERSIONS="${fdb_library_versions[*]}" \
            --build-arg FDB_WEBSITE="${fdb_website}" \
            --tag "${image_tag}" \
            --file "${dockerfile_name}" \
            --target "${image}" .
        if [ "${image}" == 'foundationdb' ] || [ "${image}" == 'foundationdb-kubernetes-sidecar' ] || [ "${image}" == 'ycsb' ] ; then
            tags_to_push+=("${image_tag}")
        fi
    done

    if [ "${push_docker_images}" == "true" ]; then
        for tag in "${tags_to_push[@]}"; do
            logg "PUSH ${tag}"
            docker push "${tag}"
        done
    fi
}

echo "${blue}################################################################################${reset}"
logg "STARTING ${0}"
echo "${blue}################################################################################${reset}"

################################################################################
# The intent of this script is to build the set of docker images needed to run
# FoundationDB in kubernetes from binaries that are not available the website:
# https://foundationdb.org/downloads
#
# The docker file itself will pull released binaries from the foundationdb
# website. If the intent is to build images for an already released version of
# FoundationDB, a simple docker build command will work.
#
# This script has enough stupid built into it that trying to come up with a set
# of sensible default options that will work everywhere has gotten silly. Below
# are a set of variable definitions that need to be set for this script to
# execute to completion the defaults are based on the FoundationDB development
# environment used by the team at Apple, they will not work for everyone cloning
# this project.
#
# Use this script with care.
################################################################################
artifactory_base_url="${ARTIFACTORY_URL:-https://artifactory.foundationdb.org}"
aws_region="us-west-2"
aws_account_id=$(aws --output text sts get-caller-identity --query 'Account')
build_date=$(date +"%Y-%m-%dT%H:%M:%S%z")
build_output_directory="${script_dir}/../../build_output"
commit_sha=$(git rev-parse --verify HEAD --short=10)
fdb_version=$(cat "${build_output_directory}/version.txt")
fdb_library_versions=( '5.1.7' '6.1.13' '6.2.30' '6.3.18' "${fdb_version}" )
fdb_website="https://www.foundationdb.org"
image_list=(
    'base'
    # 'go-build'
    'foundationdb-base'
    'foundationdb'
    # 'foundationdb-kubernetes-monitor'
    'foundationdb-kubernetes-sidecar'
    'ycsb'
)
registry=""
tag_base="foundationdb/"
# THESE CONTROL THE PATH OF FUNCTIONS THAT ARE CALLED BELOW
stripped_binaries_and_from_where="stripped_local" # MUST BE ONE OF ( "unstripped_artifactory" "stripped_artifactory" "unstripped_local" "stripped_local" )
dockerfile_name="Dockerfile"
use_development_java_bindings="false"
push_docker_images="false"

if [ -n "${OKTETO_NAMESPACE+x}" ]; then
    logg "RUNNING IN OKTETO/AWS"
    # these are defaults for the Apple development environment
    aws_region=$(curl -s "http://169.254.169.254/latest/meta-data/placement/region")
    aws_account_id=$(aws --output text sts get-caller-identity --query 'Account')
    build_output_directory="${HOME}/build_output"
    fdb_library_versions=( "${fdb_version}" )
    registry="${aws_account_id}.dkr.ecr.${aws_region}.amazonaws.com"
    tag_base="${registry}/foundationdb/"
    stripped_binaries_and_from_where="unstripped_local" # MUST BE ONE OF ( "unstripped_artifactory" "stripped_artifactory" "unstripped_local" "stripped_local" )
    dockerfile_name="Dockerfile.eks"
    use_development_java_bindings="true"
    push_docker_images="true"
else
    echo "Dear ${USER}, you probably need to edit this file before running it. "
    echo "${0} has a very narrow set of situations where it will be successful,"
    echo "or even useful, when executed unedited"
    exit 1
fi

create_fake_website_directory
build_and_push_images

echo "${blue}################################################################################${reset}"
logg "COMPLETED ${0}"
echo "${blue}################################################################################${reset}"
