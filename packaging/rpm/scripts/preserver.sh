# This should be ensured by the foundationdb-clients package, but it can't hurt...
getent group foundationdb >/dev/null || groupadd -r foundationdb >/dev/null
getent passwd foundationdb >/dev/null || useradd -c "FoundationDB" -g foundationdb -s /bin/false -r -d /var/lib/foundationdb foundationdb >/dev/null

if [ $1 -gt 1 ]; then
    # old versions could leave this behind
    rm -f /usr/lib/foundationdb/argparse.py /usr/lib/foundationdb/argparse.pyc
fi

CURRENTVER=$(rpm -q --queryformat %%{VERSION} foundationdb-server) || :
if [ "$CURRENTVER" = "0.1.5" ] || [ "$CURRENTVER" = "0.1.4" ]; then
    mv /etc/foundationdb/foundationdb.conf /etc/foundationdb/foundationdb.conf.rpmsave
fi
if [ $1 -eq 0 ]; then
    if pidof systemd
    then
        /usr/bin/systemctl stop foundationdb >/dev/null 2>&1
        /usr/bin/systemctl disable foundationdb >/dev/null 2>&1
    else
        /sbin/service foundationdb stop >/dev/null 2>&1
        /sbin/chkconfig --del foundationdb >/dev/null 2>&1
    fi
fi
exit 0
