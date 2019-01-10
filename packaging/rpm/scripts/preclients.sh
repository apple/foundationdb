getent group foundationdb >/dev/null || groupadd -r foundationdb >/dev/null
getent passwd foundationdb >/dev/null || useradd -c "FoundationDB" -g foundationdb -s /bin/false -r -d /var/lib/foundationdb foundationdb >/dev/null
exit 0
