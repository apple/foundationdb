#!/bin/sh

set -e

TARGET="$1"
shift

VERSION=$(cat $TARGET | grep '<Version>' | sed -e 's,^[^>]*>,,' -e 's,<.*,,')
PACKAGE_NAME=$(cat $TARGET | grep '<PackageName>' | sed -e 's,^[^>]*>,,' -e 's,<.*,,')

cat << EOF
#define FDB_VT_VERSION "${VERSION}-DROPBOX"
#define FDB_VT_PACKAGE_NAME "${PACKAGE_NAME}"
EOF

#versions.h: Makefile versions.target
#	@rm -f $@
#ifeq ($(RELEASE),true)
#	@echo "#define FDB_VT_VERSION \"$(VERSION)\"" >> $@
#else
#	@echo "#define FDB_VT_VERSION \"$(VERSION)-PRERELEASE\"" >> $@
#endif
#	@echo "#define FDB_VT_PACKAGE_NAME \"$(PACKAGE_NAME)\"" >> $@
