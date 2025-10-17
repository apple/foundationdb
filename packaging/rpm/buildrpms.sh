#!/bin/bash

VERSION=$1
RELEASE=$2

umask 0022

TEMPDIR=$(mktemp -d)
INSTDIR=$(mktemp -d)

trap "rm -rf $TEMPDIR $INSTDIR" EXIT

mkdir -p $TEMPDIR/{BUILD,BUILDROOT,RPMS,SOURCES,SPECS,SRPMS}
echo "%_topdir $TEMPDIR" > $TEMPDIR/macros

mkdir -p -m 0755 $INSTDIR/etc/foundationdb
mkdir -p -m 0755 $INSTDIR/etc/rc.d/init.d
mkdir -p -m 0755 $INSTDIR/lib/systemd/system
mkdir -p -m 0755 $INSTDIR/usr/bin
mkdir -p -m 0755 $INSTDIR/usr/sbin
mkdir -p -m 0755 $INSTDIR/usr/lib64
mkdir -p -m 0755 $INSTDIR/usr/include/foundationdb
mkdir -p -m 0755 $INSTDIR/usr/share/doc/foundationdb-clients
mkdir -p -m 0755 $INSTDIR/usr/share/doc/foundationdb-server
mkdir -p -m 0755 $INSTDIR/var/log/foundationdb
mkdir -p -m 0755 $INSTDIR/var/lib/foundationdb/data

install -m 0644 packaging/foundationdb.conf $INSTDIR/etc/foundationdb
install -m 0755 packaging/rpm/foundationdb-init $INSTDIR/etc/rc.d/init.d/foundationdb
install -m 0644 packaging/rpm/foundationdb.service $INSTDIR/lib/systemd/system/foundationdb.service
install -m 0755 bin/fdbcli $INSTDIR/usr/bin
install -m 0755 bin/fdbserver $INSTDIR/usr/sbin
install -m 0755 bin/fdbmonitor $INSTDIR/usr/sbin
install -m 0755 lib/libfdb_c.so $INSTDIR/usr/lib64
install -m 0755 lib/libfdb_c_shim.so $INSTDIR/usr/lib64
install -m 0644 bindings/c/foundationdb/fdb_c.h bindings/c/foundationdb/fdb_c_options.g.h bindings/c/foundationdb/fdb_c_types.h bindings/c/foundationdb/fdb_c_internal.h bindings/c/foundationdb/fdb_c_shim.h fdbclient/vexillographer/fdb.options $INSTDIR/usr/include/foundationdb
dos2unix -q -n README.md $INSTDIR/usr/share/doc/foundationdb-clients/README
dos2unix -q -n README.md $INSTDIR/usr/share/doc/foundationdb-server/README
chmod 0644 $INSTDIR/usr/share/doc/foundationdb-clients/README
chmod 0644 $INSTDIR/usr/share/doc/foundationdb-server/README
install -m 0755 bin/fdbbackup $INSTDIR/usr/bin/backup_agent
install -m 0755 packaging/make_public.py $INSTDIR/usr/lib/foundationdb

ln -s backup_agent $INSTDIR/usr/bin/fdbbackup
ln -s backup_agent $INSTDIR/usr/bin/fdbrestore
ln -s backup_agent $INSTDIR/usr/bin/fdbdr
ln -s backup_agent $INSTDIR/usr/bin/dr_agent

(cd $INSTDIR ; tar -czf $TEMPDIR/SOURCES/install-files.tar.gz *)

m4 -DFDBVERSION=$VERSION -DFDBRELEASE=$RELEASE.el9 packaging/rpm/foundationdb.spec.in > $TEMPDIR/SPECS/foundationdb.el9.spec

fakeroot rpmbuild --quiet --define "%_topdir $TEMPDIR" -bb $TEMPDIR/SPECS/foundationdb.el9.spec

cp $TEMPDIR/RPMS/x86_64/*.rpm packages
