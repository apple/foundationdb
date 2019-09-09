#!/bin/bash

MAINTAINER="Your Name <you@example.com>"
# Version must be numeric
VERSION=$(grep FDB_VT_PACKAGE_NAME versions.h | awk '{gsub(/"/,""); print $3}')

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
install -m 0755 bin/fdbmonitor $SERVERDIR/usr/lib/foundationdb
install -m 0755 packaging/make_public.py $SERVERDIR/usr/lib/foundationdb
install -m 0644 packaging/argparse.py $SERVERDIR/usr/lib/foundationdb
dos2unix -q -n README.md $SERVERDIR/usr/share/doc/foundationdb-server/README
chmod 0644 $SERVERDIR/usr/share/doc/foundationdb-server/README

echo "Package: foundationdb-server" >> $SERVERDIR/DEBIAN/control
echo "Version: $VERSION" >> $SERVERDIR/DEBIAN/control
echo "Maintainer: $MAINTAINER" >> $SERVERDIR/DEBIAN/control
echo "Description: FoundationDB server" >> $SERVERDIR/DEBIAN/control
echo "Installed-Size:" $(du -sx --exclude DEBIAN $SERVERDIR | awk '{print $1}') >> $SERVERDIR/DEBIAN/control
echo "Architecture: all" >> $SERVERDIR/DEBIAN/control

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
mkdir -p -m 0755 $CLIENTSDIR/usr/lib/foundationdb/backup_agent

install -m 0755 bin/fdbcli $CLIENTSDIR/usr/bin
install -m 0644 lib/libfdb_c.so $CLIENTSDIR/usr/lib
install -m 0644 bindings/c/foundationdb/fdb_c.h bindings/c/foundationdb/fdb_c_options.g.h fdbclient/vexillographer/fdb.options $CLIENTSDIR/usr/include/foundationdb
dos2unix -q -n README.md $CLIENTSDIR/usr/share/doc/foundationdb-clients/README
chmod 0644 $CLIENTSDIR/usr/share/doc/foundationdb-clients/README
install -m 0755 bin/fdbbackup $CLIENTSDIR/usr/lib/foundationdb/backup_agent/backup_agent
ln -s ../lib/foundationdb/backup_agent/backup_agent $CLIENTSDIR/usr/bin/fdbbackup
ln -s ../lib/foundationdb/backup_agent/backup_agent $CLIENTSDIR/usr/bin/fdbrestore
ln -s ../lib/foundationdb/backup_agent/backup_agent $CLIENTSDIR/usr/bin/fdbdr
ln -s ../lib/foundationdb/backup_agent/backup_agent $CLIENTSDIR/usr/bin/dr_agent

echo "Package: foundationdb-clients" >> $CLIENTSDIR/DEBIAN/control
echo "Version: $VERSION" >> $CLIENTSDIR/DEBIAN/control
echo "Maintainer: $MAINTAINER" >> $CLIENTSDIR/DEBIAN/control
echo "Description: FoundationDB clients and library" >> $CLIENTSDIR/DEBIAN/control
echo "Architecture: all" >> $CLIENTSDIR/DEBIAN/control
echo "Installed-Size:" $(du -sx --exclude DEBIAN $CLIENTSDIR | awk '{print $1}') >> $CLIENTSDIR/DEBIAN/control

if ! fakeroot dpkg-deb --build "${CLIENTSDIR}" packages 2> /dev/null
then
	echo "Failed to create Fdb client deb package in directory: ${CLIENTSDIR}"
	let status="${status} + 1"
fi

rm -r "${CLIENTSDIR}"

exit "${status}"
