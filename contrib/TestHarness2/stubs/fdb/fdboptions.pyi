import enum


class StreamingMode(enum.Enum):
    want_all = -2
    iterator = -1
    exact = 0
    small = 1
    medium = 2
    large = 3
    serial = 4

class NetworkOptions(enum.Enum):
    local_address = 10
    cluster_file = 20
    trace_enable = 30
    trace_roll_size = 31
    trace_max_logs_size = 32
    trace_log_group = 33
    trace_format = 34
    trace_clock_source = 35
    trace_file_identifier = 36
    trace_partial_file_suffix = 39
    knob = 40
    TLS_plugin = 41
    TLS_cert_bytes = 42
    TLS_cert_path = 43
    TLS_key_bytes = 45
    TLS_key_path = 46
    TLS_verify_peers = 47
    Buggify_enable = 48
    Buggify_disable = 49
    Buggify_section_activated_probability = 50
    Buggify_section_fired_probability = 51
    TLS_ca_bytes = 52
    TLS_ca_path = 53
    TLS_password = 54
    disable_multi_version_client_api = 60
    callbacks_on_external_threads = 61
    external_client_library = 62
    external_client_directory = 63
    disable_local_client = 64
    client_threads_per_version = 65
    disable_client_statistics_logging = 70
    enable_slow_task_profiling = 71
    enable_run_loop_profiling = 71
    client_buggify_enable = 80
    client_buggify_disable = 81
    client_buggify_section_activated_probability = 82
    client_buggify_section_fired_probability = 83
    distributed_client_tracer = 90

class DatabaseOptions:
    location_cache_size = 10
    max_watches = 20
    machine_id = 21
    datacenter_id = 22
    snapshot_ryw_enable = 26
    snapshot_ryw_disable = 27
    transaction_logging_max_field_length = 405
    transaction_timeout = 500
    transaction_retry_limit = 501
    transaction_max_retry_delay = 502
    transaction_size_limit = 503
    transaction_causal_read_risky = 504
    transaction_include_port_in_address = 505
    transaction_bypass_unreadable = 700
    use_config_database = 800
    test_causal_read_risky = 900


class TransactionOptions:
    causal_write_risky = 10
    causal_read_risky = 20
    causal_read_disable = 21
    include_port_in_address = 23
    next_write_no_write_conflict_range = 30
    read_your_writes_disable = 51
    read_ahead_disable = 52
    durability_datacenter = 110
    durability_risky = 120
    durability_dev_null_is_web_scale = 130
    priority_system_immediate = 200
    priority_batch = 201
    initialize_new_database = 300
    access_system_keys = 301
    read_system_keys = 302
    raw_access = 303
    debug_retry_logging = 401
    transaction_logging_enable = 402
    debug_transaction_identifier = 403
    log_transaction = 404
    transaction_logging_max_field_length = 405
    server_request_tracing = 406
    timeout = 500
    retry_limit = 501
    max_retry_delay = 502
    size_limit = 503
    snapshot_ryw_enable = 600
    snapshot_ryw_disable = 601
    lock_aware = 700
    used_during_commit_protection_disable = 701
    read_lock_aware = 702
    use_provisional_proxies = 711
    report_conflicting_keys = 712
    special_key_space_relaxed = 713
    special_key_space_enable_writes = 714
    tag = 800
    auto_throttle_tag = 801
    span_parent = 900
    expensive_clear_cost_estimation_enable = 1000
    bypass_unreadable = 1100
    use_grv_cache = 1101
