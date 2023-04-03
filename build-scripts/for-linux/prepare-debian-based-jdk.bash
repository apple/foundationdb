#!/bin/bash

# install openjdk of the most early longterm version

set -e

sudo apt update

find_pkg() {
  apt search -o APT::Cache::Search::Version=1 "^$1\$" | awk '{print $1;}'
}

PKG_AV=""
for P in openjdk-{8,11,17}-jdk; do
  PKG_AV=`find_pkg $P`
  [ -n "$PKG_AV" ] && break;
done

if [ -n "$PKG_AV" ]; then
  echo "Installing $PKG_AV"
  sudo DEBIAN_FRONTEND=noninteractive apt install -y $PKG_AV
else
  echo "No openjdk-*-jdk package available." >1
  return 1 2>&1 >/dev/null
  exit 1
fi
