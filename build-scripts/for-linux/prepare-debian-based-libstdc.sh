#!/bin/sh

# install libstdc++-dev of the most recent version

set -e

sudo apt update
RECENT_VERSION=` \
  apt search -o APT::Cache::Search::Version=1 '^libstdc++.*-dev$' \
  | awk '/libstdc++/ {gsub("libstdc\+\+-","",$1); gsub("-dev","",$1);  print $1;}' \
  | sort -n |tail -n 1`

sudo DEBIAN_FRONTEND=noninteractive apt install -y libstdc++-$RECENT_VERSION-dev
