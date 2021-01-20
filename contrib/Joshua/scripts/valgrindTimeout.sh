#!/bin/bash -u
for file in `find . -name 'trace*.xml'` ; do
    for valgrindFile in `find . -name 'valgrind*.xml'` ; do
        mono ./bin/TestHarness.exe summarize "${file}" summary.xml "${valgrindFile}" JoshuaTimeout true
    done
done
