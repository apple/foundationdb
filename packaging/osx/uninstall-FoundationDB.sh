#!/bin/bash -x

rm -f /usr/local/libexec/{fdbserver,fdbmonitor}
rm -f /usr/local/bin/{fdbcli,fdbbackup,fdbrestore,fdbdr}
rm -f /usr/local/lib/libfdb_c.dylib
rm -rf /usr/local/include/foundationdb
rm -rf /usr/local/foundationdb/backup_agent
rm -f /usr/local/foundationdb/uninstall-FoundationDB.sh
rm -rf /Library/Python/2.7/site-packages/fdb
launchctl unload /Library/LaunchDaemons/com.foundationdb.fdbmonitor.plist >/dev/null 2>&1 || :
rm -f /Library/LaunchDaemons/com.foundationdb.fdbmonitor.plist
rm -rf /var/db/receipts/FoundationDB-{clients,server}.*

set +x

if [ -d /usr/local/foundationdb/data ]; then
    echo
    echo "Your data and configuration files have not been removed."
    echo "To remove these files, delete the following directories:"
    echo "  - /usr/local/foundationdb"
    echo "  - /usr/local/etc/foundationdb"
    echo
fi
