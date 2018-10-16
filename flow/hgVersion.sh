#!/bin/sh

set -e

COMMIT=$(grep GIT_HASH bazel-out/volatile-status.txt | cut -d ' ' -f 2)

echo "const char *hgVersion = \"${COMMIT}\";"
