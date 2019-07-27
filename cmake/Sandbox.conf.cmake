## foundationdb.conf
##
## Configuration file for FoundationDB server processes
## Full documentation is available at
## https://apple.github.io/foundationdb/configuration.html#the-configuration-file

[fdbmonitor]

[general]
restart_delay = 10
## by default, restart_backoff = restart_delay_reset_interval = restart_delay
# initial_restart_delay = 0
# restart_backoff = 60
# restart_delay_reset_interval = 60
cluster_file = ${CMAKE_BINARY_DIR}/fdb.cluster
# delete_envvars =
# kill_on_configuration_change = true

## Default parameters for individual fdbserver processes
[fdbserver]
command = ${CMAKE_BINARY_DIR}/bin/fdbserver
public_address = auto:$ID
listen_address = public
datadir = ${CMAKE_BINARY_DIR}/sandbox/data/$ID
logdir = ${CMAKE_BINARY_DIR}/sandbox/logs
# logsize = 10MiB
# maxlogssize = 100MiB
# machine_id =
# datacenter_id =
# class =
# memory = 8GiB
# storage_memory = 1GiB
# cache_memory = 2GiB
# metrics_cluster =
# metrics_prefix =

## An individual fdbserver process with id 4000
## Parameters set here override defaults from the [fdbserver] section
[fdbserver.4000]
