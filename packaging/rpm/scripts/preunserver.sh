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
