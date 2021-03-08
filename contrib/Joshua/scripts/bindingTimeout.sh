#!/bin/bash -u

# Look for the start cluster log file.
notstarted=0
for file in `find . -name startcluster.log` ; do
    if [ -n "$(grep 'Could not create database' "${file}")" ] ; then
        echo "${file}:"
        cat "${file}"
        echo
        notstarted=1
    fi
done

# Print information on how the server didn't start.
if [ "${notstarted}" -gt 0 ] ; then
    for file in `find . -name fdbclient.log` ; do
        echo "${file}:"
        cat "${file}"
        echo
    done
fi

# Print the test output.
for file in `find . -name console.log` ; do
    echo "${file}:"
    cat "${file}"
    echo
done
