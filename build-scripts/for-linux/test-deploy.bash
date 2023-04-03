#!/bin/bash

# Test deploying foundationdb on rpm-and deb-based linux
# $1 - full Foundationdb version, ex. 7.1.29-0.ow.1
# $2 - distr dir. Default is bld/linux/packages relative to the current dir
# $3 - a rpm-based linux docker image. Default is oraclelinux:8
# $4 - a deb-based linux docker image. Default is debian:10

set -e

BASE_DIR="$(readlink -f $(dirname $0))"
FULL_VERSION="$1"
DISTR_DIR="$(readlink -f ${2:-bld/linux/packages})"
RPM_IMAGE=${3:-oraclelinux:8}
DEB_IMAGE=${4:-debian:10}

print_usage() {
  echo >&2 "Usage: $0 FullFdbVersion [FdbDistrDir] [RpmImage] [DebImage]"
}

# testing parameters
if [[ -z "$FULL_VERSION" ]]; then
  echo >&2 "FullFdbVersion is not specified."
  print_usage
  return 1 2>/dev/null || exit 1
fi

if ! ls "$DISTR_DIR"/foundationdb-*$FULL_VERSION*.{rpm,deb}; then
  echo >&2 "No $DISTR_DIR/foundationdb-*$FULL_VERSION*.{rpm,deb} files have been found."
  echo >&2 "Possible FdbDistrDir is not correct."
  print_usage
  return 1 2>/dev/null || exit 1
fi

# check images
podman pull $RPM_IMAGE
podman pull $DEB_IMAGE

MY_ARCH_RPM=`uname -m`
MY_ARCH_DEB=`dpkg-architecture -q DEB_HOST_ARCH`

test_deploy_pkgs() {
  IMAGE=$1
  INSTALL_CMD=$2
  SERVER_FILE=$3
  CLIENT_FILE=$4
  USER_AFTER_CLIENT=${5:-Y}

  echo SERVER_FILE=$SERVER_FILE
  echo CLIENT_FILE=$CLIENT_FILE

  if [[ $USER_AFTER_CLIENT == Y ]]; then
    CLIENT_CHECK_WITH=""
    ERRMSG_CLIENT="not created"
  else
    CLIENT_CHECK_WITH="!"
    ERRMSG_CLIENT="created unexpectedly"
  fi


  echo "Trying to install the client package only..."
  set -x
  if ! podman run --rm -v "$DISTR_DIR:/mnt/distr:Z,ro" $IMAGE \
    /bin/bash -c "$INSTALL_CMD /mnt/distr/$CLIENT_FILE && $CLIENT_CHECK_WITH getent passwd foundationdb"; then
    echo >&2 "Installation of $CLIENT_FILE failed or the foundationdb user was $ERRMSG_CLIENT."
    return 3
  fi
  set +x
  echo "Installation test of the client package only is successful"
  echo ""

  echo "Trying to install the both client and server package packages..."
  set -x
  if ! podman run --rm -v "$DISTR_DIR:/mnt/distr:Z,ro" $IMAGE \
    /bin/bash -c "$INSTALL_CMD /mnt/distr/$SERVER_FILE /mnt/distr/$CLIENT_FILE && getent passwd foundationdb"
  then
    echo >&2 "Installation $SERVER_FILE and $CLIENT_FILE failed or the foundationdb user was not created."
    return 2
  fi
  set +x
  echo "Installation test of the both client and server packages is successful"
  echo ""

  echo "Trying to install the server package only..."
  set -x
  if podman run --rm -v "$DISTR_DIR:/mnt/distr:Z,ro" $IMAGE \
    /bin/bash -c "$INSTALL_CMD /mnt/distr/$SERVER_FILE"; then
    echo >&2 "Installation $SERVER_FILE without a client must fail."
    return 1
  fi
  set +x
  echo "Installation test of the server package only is successful"
  echo ""

}

RPM_INSTALL_CMD="dnf install -y"
DEB_INSTALL_CMD="apt-get update && DEBIAN_FRONTEND=noninteractive apt-get install -y"

echo "Testing DEBs deploy..."
test_deploy_pkgs \
  "$DEB_IMAGE" \
  "$DEB_INSTALL_CMD" \
  "foundationdb-server_${FULL_VERSION}_$MY_ARCH_DEB.deb" \
  "foundationdb-clients_${FULL_VERSION}_$MY_ARCH_DEB.deb" \
  Y

echo "Testing versioned DEBs deploy..."
test_deploy_pkgs \
  "$DEB_IMAGE" \
  "$DEB_INSTALL_CMD" \
  "foundationdb-$FULL_VERSION-server-versioned_${FULL_VERSION}_$MY_ARCH_DEB.deb" \
  "foundationdb-$FULL_VERSION-clients-versioned_${FULL_VERSION}_$MY_ARCH_DEB.deb" \
  N

echo "Testing RPMs deploy..."
test_deploy_pkgs \
  "$RPM_IMAGE" \
  "$RPM_INSTALL_CMD" \
  "foundationdb-server-$FULL_VERSION.$MY_ARCH_RPM.rpm" \
  "foundationdb-clients-$FULL_VERSION.$MY_ARCH_RPM.rpm" \
  Y

echo "Testing versioned RPMs deploy..."
test_deploy_pkgs \
  "$RPM_IMAGE" \
  "$RPM_INSTALL_CMD" \
  "foundationdb-$FULL_VERSION-server-versioned-$FULL_VERSION.$MY_ARCH_RPM.rpm" \
  "foundationdb-$FULL_VERSION-clients-versioned-$FULL_VERSION.$MY_ARCH_RPM.rpm" \
  N

