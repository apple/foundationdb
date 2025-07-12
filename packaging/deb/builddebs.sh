#!/bin/bash

umask 0022
status=0

# Server package

SERVERDIR=$(mktemp -d)
cp -a packaging/deb/DEBIAN-foundationdb-server $SERVERDIR/DEBIAN
chmod 0755 $SERVERDIR/DEBIAN/{pre,post}*
chmod 0644 $SERVERDIR/DEBIAN/conffiles

mkdir -p -m 0775 $SERVERDIR/etc/foundationdb
mkdir -p -m 0755 $SERVERDIR/etc/init.d
mkdir -p -m 0755 $SERVERDIR/usr/sbin
mkdir -p -m 0755 $SERVERDIR/usr/share/doc/foundationdb-server
mkdir -p -m 0755 $SERVERDIR/var/log/foundationdb
mkdir -p -m 0755 $SERVERDIR/var/lib/foundationdb/data
mkdir -p -m 0755 $SERVERDIR/usr/lib/foundationdb

install -m 0664 packaging/foundationdb.conf $SERVERDIR/etc/foundationdb
install -m 0755 packaging/deb/foundationdb-init $SERVERDIR/etc/init.d/foundationdb
install -m 0755 bin/fdbserver $SERVERDIR/usr/sbin
install -m 0755 bin/fdbmonitor $SERVERDIR/usr/sbin
install -m 0755 packaging/make_public.py $SERVERDIR/usr/lib/foundationdb
dos2unix -q -n README.md $SERVERDIR/usr/share/doc/foundationdb-server/README
chmod 0644 $SERVERDIR/usr/share/doc/foundationdb-server/README

echo "Installed-Size:" $(du -sx --exclude DEBIAN $SERVERDIR | awk '{print $1}') >> $SERVERDIR/DEBIAN/control

if ! fakeroot dpkg-deb --build "${SERVERDIR}" packages 2> /dev/null
then
	echo "Failed to create Fdb server deb package in directory: ${SERVERDIR}"
	let status="${status} + 1"
fi



rm -r "${SERVERDIR}"

# Clients package

CLIENTSDIR=$(mktemp -d)
cp -a packaging/deb/DEBIAN-foundationdb-clients $CLIENTSDIR/DEBIAN
chmod 0755 $CLIENTSDIR/DEBIAN/postinst

mkdir -p -m 0775 $CLIENTSDIR/etc/foundationdb
mkdir -p -m 0755 $CLIENTSDIR/usr/bin
mkdir -p -m 0755 $CLIENTSDIR/usr/lib
mkdir -p -m 0755 $CLIENTSDIR/usr/include/foundationdb
mkdir -p -m 0755 $CLIENTSDIR/usr/share/doc/foundationdb-clients

install -m 0755 bin/fdbcli $CLIENTSDIR/usr/bin
install -m 0644 lib/libfdb_c.so lib/libfdb_c_shim.so $CLIENTSDIR/usr/lib
install -m 0644 bindings/c/foundationdb/fdb_c.h bindings/c/foundationdb/fdb_c_types.h bindings/c/foundationdb/fdb_c_internal.h bindings/c/foundationdb/fdb_c_options.g.h fdbclient/vexillographer/fdb.options bindings/c/foundationdb/fdb_c_shim.h $CLIENTSDIR/usr/include/foundationdb
dos2unix -q -n README.md $CLIENTSDIR/usr/share/doc/foundationdb-clients/README
chmod 0644 $CLIENTSDIR/usr/share/doc/foundationdb-clients/README
install -m 0755 bin/fdbbackup $CLIENTSDIR/usr/bin/backup_agent

ln -s backup_agent $CLIENTSDIR/usr/bin/fdbbackup
ln -s backup_agent $CLIENTSDIR/usr/bin/fdbrestore
ln -s backup_agent $CLIENTSDIR/usr/bin/fdbdr
ln -s backup_agent $CLIENTSDIR/usr/bin/dr_agent

echo "Installed-Size:" $(du -sx --exclude DEBIAN $CLIENTSDIR | awk '{print $1}') >> $CLIENTSDIR/DEBIAN/control

if ! fakeroot dpkg-deb --build "${CLIENTSDIR}" packages 2> /dev/null
then
	echo "Failed to create Fdb client deb package in directory: ${CLIENTSDIR}"
	let status="${status} + 1"
fi

rm -r "${CLIENTSDIR}"

exit "${status}"
