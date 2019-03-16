  {
    "client": {
      "cluster_file": {
        "path": "/etc/foundationdb/fdb.cluster",
        "up_to_date": true
      },
      "coordinators": {
        "coordinators": [
          {
            "address": "10.0.4.1:4701",
            "reachable": true
          }
        ],
        "quorum_reachable": true
      },
      "database_status": {
        "available": true,
        "healthy": true
      },
      "messages": [
        {
          "name": <name_string>,
          "description": <description_string>
        }
      ],
      "timestamp": 1415650089
    },
    "cluster": {
      "clients": {
        "count": 1,
        "supported_versions": [
          {
            "client_version": "4.2.0",
            "connected_clients": [
              {
                "address": "127.0.0.1:1234",
                "log_group": "default"
              }
            ],
            "count": 1,
            "protocol_version": "fdb00a400050001",
            "source_version": "a21e22025bafd7da5e642182683d450e7b68ca26"
          }
        ]
      },
      "cluster_controller_timestamp": 1415650089,
      "configuration": {
        "coordinators_count": 1,
        "excluded_servers": [
          {"address": "10.0.4.1"}
        ],
        "logs": 2, // this field will be absent if a value has not been explicitly set
        "policy": "zoneid^3 x 1",
        "proxies": 5, // this field will be absent if a value has not been explicitly set
        "redundancy": {
          "factor": <  "single"
                     | "double"
                     | "triple"
                     | "custom"
                     | "two_datacenter"
                     | "three_datacenter"
                     | "three_data_hall"
                    >
        },
        "resolvers": 1, // this field will be absent if a value has not been explicitly set
        "storage_engine": <  "ssd"
                           | "memory"
                           | "custom"
                          >
      },
      "data": {
        "average_partition_size_bytes": 0,
        "least_operating_space_bytes_log_server": 0,
        "least_operating_space_bytes_storage_server": 0,
        "moving_data": {
          "in_flight_bytes": 0,
          "in_queue_bytes": 0
        },
        "partitions_count": 2,
        "state": {
          "name": <  "initializing"
                   | "missing_data"
                   | "healing"
                   | "optimizing_team_collections"
                   | "healthy_repartitioning"
                   | "healthy_removing_server"
                   | "healthy_rebalancing"
                   | "healthy"
                  >,
          "description": <string>,
          "healthy": true,
          "min_replicas_remaining": 0
        },
        "total_disk_used_bytes": 0,
        "total_kv_size_bytes": 0 // estimated
      },
      "database_available": true,
      "database_locked": false,
      "fault_tolerance": {
        "max_machine_failures_without_losing_availability": 0,
        "max_machine_failures_without_losing_data": 0
      },
      "latency_probe": { // all measurements are based on running sample transactions
        "commit_seconds": 0.0, // time to commit a sample transaction
        "read_seconds": 0.0, // time to perform a single read
        "transaction_start_seconds": 0.0, // time to start a sample transaction at normal priority
        "immediate_priority_transaction_start_seconds":0.0, // time to start a sample transaction at system immediate priority
        "batch_priority_transaction_start_seconds":0.0 // time to start a sample transaction at batch priority
      },
      "machines": {
        <id_string>: {
          "address": "10.0.4.1",
          "cpu": {
            "logical_core_utilization": 0.0 // computed as cpu_seconds / elapsed_seconds; value may be capped at 0.5 due to hyper-threading
          },
          "datacenter_id": <id_string>,
          "excluded": false,
          "locality": { // This will contain any locality fields that are provided on the command line
            "machineid": <id_string>,
            "dcid": <id_string>
          },
          "machine_id": <id_string>,
          "memory": {
            "committed_bytes": 0,
            "free_bytes": 0, // an estimate of how many bytes are free to allocate to fdbservers without swapping
            "total_bytes": 0 // an estimate of total physical RAM
          },
          "network": {
            "megabits_received": {"hz": 0.0},
            "megabits_sent": {"hz": 0.0},
            "tcp_segments_retransmitted": {"hz": 0.0}
          }
        }
      },
      "messages": [
        {
          "name": <name_string>,
          "description": <description_string>,
          "issues": [
              {
                  "name": <  "incorrect_cluster_file_contents"
                           | ...
                          >,
                  "description": "Cluster file contents do not match current cluster connection string. Verify the cluster file and its parent directory are writable and that the cluster file has not been overwritten externally."
              }
          ],
          "reasons": [
              {"description": <string>}
          ],
          "unreachable_processes": [
              {"address": "10.0.4.1:4702"}
          ]
        }
      ],
      "processes": {
        <id_string>: {
          "address": "10.0.4.1:4701",
          "uptime_seconds": 1234.2345,
          "command_line": <string>,
          "cpu": {
            "usage_cores": 0.0 // average number of logical cores utilized by the process over the recent past; value may be > 1.0
          },
          "disk": {
            "busy": 0.0 // from 0.0 (idle) to 1.0 (fully busy)
          },
          "excluded": false,
          "machine_id": <id_string>,
          "fault_domain": <id_string>,
          "locality": { // This will contain any locality fields that are provided on the command line
            "machineid": <id_string>,
            "dcid": <id_string>
          },
          "memory": {
            "available_bytes": 0, //an estimate of the process' fair share of the memory available to fdbservers
            "limit_bytes": 0, // memory limit per process
            "unused_allocated_bytes": 0,
            "used_bytes": 0
          },
          "messages": [
            {
              "name": <name_string>,
              "description": <description_string>,
              "raw_log_message": <string>,
              "time": 0.0,
              "type": <string>
            }
          ],
          "network": {
            "current_connections":0,
            "connections_established": {"hz": 0.0},
            "connections_closed": {"hz": 0.0},
            "connection_errors": {"hz": 0.0},
            "megabits_received": {"hz": 0.0},
            "megabits_sent": {"hz": 0.0}
          },
          "roles": [
            {
              "id": <id_string>,
              "role": <  "master"
                       | "proxy"
                       | "log"
                       | "storage"
                       | "resolver"
                       | "cluster_controller"
                      >
            }
          ],
          "version": "3.0.0" // a process version will not be reported if it is not protocol-compatible; it will be absent from status
        }
      },
      "qos": {
        "limiting_queue_bytes_storage_server": 0,
        "limiting_version_lag_storage_server": 0,
        "performance_limited_by": {
          "name": <name_string>, // "workload" when not limiting
          "description": <description_string>,
          "reason_id": 0,
          "reason_server_id": <id_string>
        },
        "released_transactions_per_second": 0.0,
        "transactions_per_second_limit": 0.0,
        "worst_queue_bytes_log_server": 460,
        "worst_queue_bytes_storage_server": 0,
        "worst_version_lag_storage_server": 0
      },
      "recovery_state": {
        "name": <name_string>, // "fully_recovered" is the healthy state; other states are normal to transition through but not to persist in
        "description": <description_string>,
        "required_logs": 3,
        "required_proxies": 1,
        "required_resolvers": 1
      },
      "workload": {
        "bytes": { // A given counter can be reset. Roughness is a measure of the "bunching" of operations (independent of hz). Perfectly spaced operations will have a roughness of 1.0 . Randomly spaced (Poisson-distributed) operations will have a roughness of 2.0, with increased bunching resulting in increased values. Higher roughness can result in increased latency due to increased queuing.
          "written": {"counter": 0, "hz": 0.0, "roughness": 0.0}
        },
        "operations": {
          "reads": {"hz": 0.0},
          "writes": {"counter": 0, "hz": 0.0, "roughness": 0.0}
        },
        "transactions": {
          "committed": {"counter": 0, "hz": 0.0, "roughness": 0.0},
          "conflicted": {"counter": 0, "hz": 0.0, "roughness": 0.0},
          "started": {"counter": 0, "hz": 0.0, "roughness": 0.0}
        }
      },
      "layers": {
      }
    }
  }
