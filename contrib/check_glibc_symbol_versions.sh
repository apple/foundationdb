#!/bin/bash
#
# Check that binaries only need glibc symbols available in the oldest glibc we support on RHEL 9 based
# distros: RHEL 9.6 glibc 2.34 (2.34-168.el9_6). Newer RHEL 9 minor releases backport some glibc 2.35+
# symbols, so a newer build host can produce binaries that fail to start on older systems with:
#   version `GLIBC_2.35' not found
#
# Usage: check_glibc_symbol_versions.sh <nm> <binary>...

set -euo pipefail

MAX_GLIBC_VERSION=2.34
# Newer symbol versions that RHEL 9.6 glibc does provide.
ALLOWED_SYMBOLS=(
  __rseq_flags@GLIBC_2.35
  __rseq_offset@GLIBC_2.35
  __rseq_size@GLIBC_2.35
)

if [[ $# -lt 2 ]]; then
  echo "Usage: $0 <nm> <binary>..." >&2
  exit 2
fi
nm=$1
shift

failed=0
for file in "$@"; do
  if ! undefined=$("$nm" -D --undefined-only "$file"); then
    echo "FAIL: $nm failed on $file"
    failed=1
    continue
  fi
  bad_symbols=()
  for symbol in $(grep -oE '[^ ]+@GLIBC_[0-9.]+$' <<<"$undefined" | sort -u); do
    version=${symbol##*@GLIBC_}
    newest=$(printf '%s\n%s\n' "$MAX_GLIBC_VERSION" "$version" | sort -V | tail -n1)
    if [[ $version != "$MAX_GLIBC_VERSION" && $newest == "$version" && ! " ${ALLOWED_SYMBOLS[*]} " =~ " $symbol " ]]; then
      bad_symbols+=("$symbol")
    fi
  done

  if [[ ${#bad_symbols[@]} -gt 0 ]]; then
    echo "FAIL: $file needs glibc symbols newer than GLIBC_${MAX_GLIBC_VERSION}:"
    printf '  %s\n' "${bad_symbols[@]}"
    failed=1
  else
    echo "OK: $file"
  fi
done

if [[ $failed -ne 0 ]]; then
  echo "Binaries would not run on RHEL 9.6. Check that clang uses gcc-toolset-13 (CLANG_GCC_INSTALL_DIR in cmake/ConfigureCompiler.cmake)."
  exit 1
fi
