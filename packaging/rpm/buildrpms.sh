#!/bin/bash

VERSION=$1
RELEASE=$2

umask 0022

TEMPDIR=$(mktemp -d)
INSTDIR=$(mktemp -d)

trap "rm -rf $TEMPDIR $INSTDIR" EXIT

mkdir -p $TEMPDIR/{BUILD,BUILDROOT,RPMS,SOURCES,SPECS,SRPMS}
echo "%_topdir $TEMPDIR" > $TEMPDIR/macros

# Version-specific directory
VERSIONDIR="foundationdb-${VERSION}"

mkdir -p -m 0755 $INSTDIR/etc/foundationdb
mkdir -p -m 0755 $INSTDIR/usr/lib/$VERSIONDIR/bin
mkdir -p -m 0755 $INSTDIR/usr/lib/$VERSIONDIR/sbin
mkdir -p -m 0755 $INSTDIR/usr/lib/$VERSIONDIR/lib
mkdir -p -m 0755 $INSTDIR/usr/lib/$VERSIONDIR/include/foundationdb
mkdir -p -m 0755 $INSTDIR/usr/lib/$VERSIONDIR/lib/systemd/system
mkdir -p -m 0755 $INSTDIR/usr/lib/$VERSIONDIR/etc/foundationdb
mkdir -p -m 0755 $INSTDIR/usr/share/doc/foundationdb-clients
mkdir -p -m 0755 $INSTDIR/usr/share/doc/foundationdb-server
mkdir -p -m 0755 $INSTDIR/var/log/foundationdb
mkdir -p -m 0755 $INSTDIR/var/lib/foundationdb/data

# Config file (versioned)
install -m 0644 packaging/foundationdb.conf $INSTDIR/usr/lib/$VERSIONDIR/etc/foundationdb/foundationdb.conf

# Systemd service (versioned)
install -m 0644 packaging/rpm/foundationdb.service $INSTDIR/usr/lib/$VERSIONDIR/lib/systemd/system/foundationdb.service

# Client binaries (versioned)
install -m 0755 bin/fdbcli $INSTDIR/usr/lib/$VERSIONDIR/bin/
install -m 0755 bin/fdbbackup $INSTDIR/usr/lib/$VERSIONDIR/bin/

# Server binaries (versioned)
install -m 0755 bin/fdbserver $INSTDIR/usr/lib/$VERSIONDIR/sbin/
install -m 0755 bin/fdbmonitor $INSTDIR/usr/lib/$VERSIONDIR/sbin/

# Libraries (versioned)
install -m 0755 lib/libfdb_c.so $INSTDIR/usr/lib/$VERSIONDIR/lib/
install -m 0755 lib/libfdb_c_shim.so $INSTDIR/usr/lib/$VERSIONDIR/lib/

# Headers (versioned)
install -m 0644 bindings/c/foundationdb/fdb_c.h bindings/c/foundationdb/fdb_c_options.g.h bindings/c/foundationdb/fdb_c_types.h bindings/c/foundationdb/fdb_c_internal.h bindings/c/foundationdb/fdb_c_shim.h fdbclient/vexillographer/fdb.options $INSTDIR/usr/lib/$VERSIONDIR/include/foundationdb/

# Documentation
dos2unix -q -n README.md $INSTDIR/usr/share/doc/foundationdb-clients/README
dos2unix -q -n README.md $INSTDIR/usr/share/doc/foundationdb-server/README
chmod 0644 $INSTDIR/usr/share/doc/foundationdb-clients/README
chmod 0644 $INSTDIR/usr/share/doc/foundationdb-server/README

# Utility scripts
install -m 0755 packaging/make_public.py $INSTDIR/usr/lib/$VERSIONDIR/lib/

(cd $INSTDIR ; tar -czf $TEMPDIR/SOURCES/install-files.tar.gz *)

m4 -DFDBVERSION=$VERSION -DFDBRELEASE=$RELEASE.el9 packaging/rpm/foundationdb.spec.in > $TEMPDIR/SPECS/foundationdb.el9.spec

fakeroot rpmbuild --quiet --define "%_topdir $TEMPDIR" -bb $TEMPDIR/SPECS/foundationdb.el9.spec

cp $TEMPDIR/RPMS/x86_64/*.rpm packages
