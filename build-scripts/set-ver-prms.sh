#!/bin/bash

# $1 - Project version
# $2 - Build version

CMAKE_VERSION_PRMS=

[[ -n "$1" ]] && CMAKE_VERSION_PRMS="$CMAKE_VERSION_PRMS -DVERSION=$1"
[[ -n "$2" ]] && CMAKE_VERSION_PRMS="$CMAKE_VERSION_PRMS -DBUILD_VERSION=$2"

export CMAKE_VERSION_PRMS

