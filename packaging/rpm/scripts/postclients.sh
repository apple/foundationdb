# older versions of this package failed to correctly clean up their .pyc files, which is "very bad"
rm -rf /usr/lib64/python2.6/fdb
if [ -d /etc/foundationdb ]; then chown foundationdb:foundationdb /etc/foundationdb; fi
exit 0
