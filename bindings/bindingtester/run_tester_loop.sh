#!/usr/bin/env bash

LOGGING_LEVEL=WARNING

function run() {
	echo "Running $1 api"
	./bindingtester.py $1 --test-name api --cluster-file fdb.cluster --compare --num-ops 1000 --logging-level $LOGGING_LEVEL
	echo "Running $1 concurrent api"
	./bindingtester.py $1 --test-name api --cluster-file fdb.cluster --num-ops 1000 --concurrency 5 --logging-level $LOGGING_LEVEL
	echo "Running $1 directory"
	./bindingtester.py $1 --test-name directory --cluster-file fdb.cluster --compare --num-ops 1000 --logging-level $LOGGING_LEVEL
	echo "Running $1 directory hca"
	./bindingtester.py $1 --test-name directory_hca --cluster-file fdb.cluster --num-ops 100 --concurrency 5 --logging-level $LOGGING_LEVEL
}

function scripted() {
	echo "Running $1 scripted"
	./bindingtester.py $1 --test-name scripted --cluster-file fdb.cluster --logging-level $LOGGING_LEVEL
}

function run_scripted() {
	scripted python
	scripted ruby
	scripted java
	scripted java_async
	scripted go
	scripted flow
}

run_scripted

i=1
while `true`; do
	echo "Pass $i"
	i=$((i+1))
	run python
	run ruby
	run java
	run java_async
	run go
	run flow
done
