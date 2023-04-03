#!/bin/sh

# install clang of the most recent version

set -e

sudo apt update
RECENT_PKG=`apt search -o APT::Cache::Search::Version=1 '^clang-[0-9][0-9]$' | awk '{print $1;}' | sort | tail -n 1`

sudo DEBIAN_FRONTEND=noninteractive apt install -y $RECENT_PKG

[ -e /usr/local/bin/clang ] || sudo ln -s `ls -1 /usr/bin/clang-[0-9]* | tail -n 1` /usr/local/bin/clang
[ -e /usr/local/bin/clang++ ] || sudo ln -s `ls -1 /usr/bin/clang++-[0-9]* | tail -n 1` /usr/local/bin/clang++
