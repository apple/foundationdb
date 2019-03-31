if [ $1 -eq 1 ]; then
    if [ ! -f /etc/foundationdb/fdb.cluster ]; then
      description=$(LC_CTYPE=C tr -dc A-Za-z0-9 < /dev/urandom | head -c 8)
      random_str=$(LC_CTYPE=C tr -dc A-Za-z0-9 < /dev/urandom | head -c 8)
      echo $description:$random_str@127.0.0.1:4500 > /etc/foundationdb/fdb.cluster
      chown foundationdb:foundationdb /etc/foundationdb/fdb.cluster
      chmod 0664 /etc/foundationdb/fdb.cluster
      NEWDB=1
    fi

    /usr/bin/systemctl enable foundationdb >/dev/null 2>&1
    /usr/bin/systemctl start foundationdb >/dev/null 2>&1

    if [ "$NEWDB" != "" ]; then
        /usr/bin/fdbcli -C /etc/foundationdb/fdb.cluster --exec "configure new single memory" --timeout 20 >/dev/null 2>&1
    fi
else
    /usr/bin/systemctl condrestart foundationdb >/dev/null 2>&1
fi
exit 0

