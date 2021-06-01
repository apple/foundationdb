#!/bin/bash

set -Eeuo pipefail

umask 0022

if [ "$#" -ne 2 ] ; then
    echo "Usage: $0 <BUILDDIR> <SRCDIR>"
    exit 1
fi

# BUILDDIR is the path to the cmake build directory
# SRCDIR is the path to the source directory
#
# e.g. If your current directory is the project root and the build directory is _build, then you want to do
#     $ ./packaging/osx/buildpkg.sh _build .
#
BUILDDIR="$1"
SRCDIR="$2"

VERSION="$(grep 'CMAKE_PROJECT_VERSION[^_]' "$BUILDDIR/CMakeCache.txt" | sed -e 's/[^=]*=//')"

PKGFILE="$BUILDDIR/packages/FoundationDB-$VERSION.pkg"

CLIENTSDIR=$( mktemp -d -t fdb-clients-pkg )
SERVERDIR=$( mktemp -d -t fdb-server-pkg )

dos2unix()
{
    tr -d '\r' < $1 > $2
}

mkdir -p -m 0755 $CLIENTSDIR/usr/local/bin
mkdir -p -m 0755 $CLIENTSDIR/usr/local/lib
mkdir -p -m 0755 $CLIENTSDIR/usr/local/include/foundationdb
mkdir -p -m 0755 $CLIENTSDIR/Library/Python/2.7/site-packages/fdb
mkdir -p -m 0775 $CLIENTSDIR/usr/local/etc/foundationdb
mkdir -p -m 0755 $CLIENTSDIR/usr/local/foundationdb/backup_agent

install -m 0755 "$BUILDDIR"/bin/fdbcli $CLIENTSDIR/usr/local/bin
install -m 0644 "$SRCDIR"/bindings/c/foundationdb/fdb_c.h "$BUILDDIR"/bindings/c/foundationdb/fdb_c_options.g.h "$SRCDIR"/fdbclient/vexillographer/fdb.options $CLIENTSDIR/usr/local/include/foundationdb
install -m 0755 "$BUILDDIR"/lib/libfdb_c.dylib $CLIENTSDIR/usr/local/lib
install -m 0644 "$BUILDDIR"/bindings/python/fdb/*.py $CLIENTSDIR/Library/Python/2.7/site-packages/fdb
install -m 0755 "$BUILDDIR"/bin/fdbbackup $CLIENTSDIR/usr/local/foundationdb/backup_agent/backup_agent
install -m 0755 "$SRCDIR"/packaging/osx/uninstall-FoundationDB.sh $CLIENTSDIR/usr/local/foundationdb
dos2unix "$SRCDIR"/README.md $CLIENTSDIR/usr/local/foundationdb/README
chmod 0644 $CLIENTSDIR/usr/local/foundationdb/README
ln -s /usr/local/foundationdb/backup_agent/backup_agent $CLIENTSDIR/usr/local/bin/fdbbackup
ln -s /usr/local/foundationdb/backup_agent/backup_agent $CLIENTSDIR/usr/local/bin/fdbrestore
ln -s /usr/local/foundationdb/backup_agent/backup_agent $CLIENTSDIR/usr/local/bin/fdbdr
ln -s /usr/local/foundationdb/backup_agent/backup_agent $CLIENTSDIR/usr/local/bin/dr_agent

pkgbuild --root $CLIENTSDIR --identifier FoundationDB-clients --version $VERSION --scripts "$SRCDIR"/packaging/osx/scripts-clients FoundationDB-clients.pkg

rm -rf $CLIENTSDIR

mkdir -p -m 0775 $SERVERDIR/usr/local/etc/foundationdb
mkdir -p -m 0755 $SERVERDIR/usr/local/libexec
mkdir -p -m 0755 $SERVERDIR/Library/LaunchDaemons
mkdir -p -m 0700 $SERVERDIR/usr/local/foundationdb/data
mkdir -p -m 0700 $SERVERDIR/usr/local/foundationdb/logs

install -m 0664 "$SRCDIR"/packaging/osx/foundationdb.conf.new $SERVERDIR/usr/local/etc/foundationdb
install -m 0755 "$BUILDDIR"/bin/fdbserver "$BUILDDIR"/bin/fdbmonitor $SERVERDIR/usr/local/libexec
install -m 0644 "$SRCDIR"/packaging/osx/com.foundationdb.fdbmonitor.plist $SERVERDIR/Library/LaunchDaemons

pkgbuild --root $SERVERDIR --identifier FoundationDB-server --version "$VERSION" --scripts "$SRCDIR"/packaging/osx/scripts-server FoundationDB-server.pkg

rm -rf $SERVERDIR

productbuild --distribution "$SRCDIR"/packaging/osx/Distribution.xml --resources "$SRCDIR"/packaging/osx/resources --package-path . "$PKGFILE"

rm FoundationDB-clients.pkg FoundationDB-server.pkg
