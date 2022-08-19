## foundationdb.conf
##
## Configuration file for FoundationDB server processes
## Full documentation is available at
## https://apple.github.io/foundationdb/configuration.html#the-configuration-file

[fdbmonitor]

[general]
restart-delay = 10
## by default, restart-backoff = restart-delay-reset-interval = restart-delay
# initial-restart-delay = 0
# restart-backoff = 60
# restart-delay-reset-interval = 60
cluster-file = ${CMAKE_BINARY_DIR}/fdb.cluster
# delete-envvars =
# kill-on-configuration-change = true

## Default parameters for individual fdbserver processes
[fdbserver]
command = ${CMAKE_BINARY_DIR}/bin/fdbserver
public-address = auto:$ID
listen-address = public
datadir = ${CMAKE_BINARY_DIR}/sandbox/data/$ID
logdir = ${CMAKE_BINARY_DIR}/sandbox/logs
# logsize = 10MiB
# maxlogssize = 100MiB
# machine-id =
# datacenter-id =
# class =
# memory = 8GiB
# storage-memory = 1GiB
# cache-memory = 2GiB
# metrics-cluster =
# metrics-prefix =

## An individual fdbserver process with id 4000
## Parameters set here override defaults from the [fdbserver] section
[fdbserver.4000]
