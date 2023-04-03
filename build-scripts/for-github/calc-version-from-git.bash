#!/bin/bash

# Calculate version numbers from git tags and set them to github output
# Assume that current directory is a github repository

# Do not set github variables when running outside github
[[ -z "$GITHUB_OUTPUT" ]] && GITHUB_OUTPUT=/dev/null

git fetch --prune --unshallow --tags --force
GIT_VERSION=`git describe --tags`
PROJECT_VERSION=`echo $GIT_VERSION | cut -d- -f1`
BUILD_VERSION=`echo $GIT_VERSION | cut -d- -f2-3 --output-delimiter=.`
GIT_CHANGE_NUM=`echo $GIT_VERSION | cut -d- -f3`
if [[ -n "$GIT_CHANGE_NUM" ]] || [[ "$BUILD_VERSION" < "1" ]]; then
  RELEASE_FLAG=OFF
else
  RELEASE_FLAG=ON
fi

# Display versions and set Github environment
echo "project_ver=$PROJECT_VERSION" | tee -a $GITHUB_OUTPUT
echo "build_ver=$BUILD_VERSION" | tee -a $GITHUB_OUTPUT
echo "full_ver=$PROJECT_VERSION-$BUILD_VERSION" | tee -a $GITHUB_OUTPUT
echo "release_flag=$RELEASE_FLAG" | tee -a $GITHUB_OUTPUT
