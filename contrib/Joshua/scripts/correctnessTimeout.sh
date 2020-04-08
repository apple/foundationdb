#!/bin/bash -u
for file in `find . -name 'trace*.xml'` ; do
    mono ./bin/TestHarness.exe summarize "${file}" summary.xml "" JoshuaTimeout true
done
