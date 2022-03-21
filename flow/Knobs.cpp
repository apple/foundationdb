/*
 * Knobs.cpp
 *
 * This source file is part of the FoundationDB open source project
 *
 * Copyright 2013-2022 Apple Inc. and the FoundationDB project authors
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

#include "flow/flow.h"
#include "flow/Knobs.h"
#include "flow/BooleanParam.h"
#include <cmath>
#include <cinttypes>

FDB_BOOLEAN_PARAM(IsSimulated);
FDB_BOOLEAN_PARAM(Randomize);

FlowKnobs::FlowKnobs(Randomize randomize, IsSimulated isSimulated) {
	initialize(randomize, isSimulated);
}

FlowKnobs bootstrapGlobalFlowKnobs(Randomize::False, IsSimulated::False);
FlowKnobs const* FLOW_KNOBS = &bootstrapGlobalFlowKnobs;

#define init(...) KNOB_FN(__VA_ARGS__, INIT_ATOMIC_KNOB, INIT_KNOB)(__VA_ARGS__)

// clang-format off
void FlowKnobs::initialize(Randomize randomize, IsSimulated isSimulated) {
	init( AUTOMATIC_TRACE_DUMP,                                  1 );
	init( PREVENT_FAST_SPIN_DELAY,                             .01 );
	init( CACHE_REFRESH_INTERVAL_WHEN_ALL_ALTERNATIVES_FAILED, 1.0 );

	init( DELAY_JITTER_OFFSET,                                 0.9 );
	init( DELAY_JITTER_RANGE,                                  0.2 );
	init( BUSY_WAIT_THRESHOLD,                                   0 ); // 1e100 == never sleep
	init( CLIENT_REQUEST_INTERVAL,                             1.0 ); if( randomize && BUGGIFY ) CLIENT_REQUEST_INTERVAL = 2.0;
	init( SERVER_REQUEST_INTERVAL,                             1.0 ); if( randomize && BUGGIFY ) SERVER_REQUEST_INTERVAL = 2.0;

	init( REACTOR_FLAGS,                                         0 );

	init( DISABLE_ASSERTS,                                       0 );
	init( QUEUE_MODEL_SMOOTHING_AMOUNT,                        2.0 );

	init( RUN_LOOP_PROFILING_INTERVAL,                       0.125 ); // A value of 0 disables run loop profiling
	init( SLOWTASK_PROFILING_LOG_INTERVAL,                       0 ); // A value of 0 means use RUN_LOOP_PROFILING_INTERVAL
	init( SLOWTASK_PROFILING_MAX_LOG_INTERVAL,                 1.0 );
	init( SLOWTASK_PROFILING_LOG_BACKOFF,                      2.0 );
	init( SLOWTASK_BLOCKED_INTERVAL,                          60.0 );
	init( SATURATION_PROFILING_LOG_INTERVAL,                   0.5 ); // A value of 0 means use RUN_LOOP_PROFILING_INTERVAL
	init( SATURATION_PROFILING_MAX_LOG_INTERVAL,               5.0 );
	init( SATURATION_PROFILING_LOG_BACKOFF,                    2.0 );

	init( RANDOMSEED_RETRY_LIMIT,                                4 );
	init( FAST_ALLOC_LOGGING_BYTES,                           10e6 );
	init( HUGE_ARENA_LOGGING_BYTES,                          100e6 );
	init( HUGE_ARENA_LOGGING_INTERVAL,                         5.0 );

	// Chaos testing - enabled for simulation by default
	init( ENABLE_CHAOS_FEATURES,                       isSimulated );
	init( CHAOS_LOGGING_INTERVAL,                              5.0 );


	init( WRITE_TRACING_ENABLED,                              true ); if( randomize && BUGGIFY ) WRITE_TRACING_ENABLED = false;
	init( TRACING_SAMPLE_RATE,                                 0.0 ); // Fraction of distributed traces (not spans) to sample (0 means ignore all traces)
	init( TRACING_UDP_LISTENER_ADDR,                   "127.0.0.1" ); // Only applicable if TracerType is set to a network option
	init( TRACING_UDP_LISTENER_PORT,                          8889 ); // Only applicable if TracerType is set to a network option

	//connectionMonitor
	init( CONNECTION_MONITOR_LOOP_TIME,   isSimulated ? 0.75 : 1.0 ); if( randomize && BUGGIFY ) CONNECTION_MONITOR_LOOP_TIME = 6.0;
	init( CONNECTION_MONITOR_TIMEOUT,     isSimulated ? 1.50 : 2.0 ); if( randomize && BUGGIFY ) CONNECTION_MONITOR_TIMEOUT = 6.0;
	init( CONNECTION_MONITOR_IDLE_TIMEOUT,                   180.0 ); if( randomize && BUGGIFY ) CONNECTION_MONITOR_IDLE_TIMEOUT = 5.0;
	init( CONNECTION_MONITOR_INCOMING_IDLE_MULTIPLIER,         1.2 );
	init( CONNECTION_MONITOR_UNREFERENCED_CLOSE_DELAY,         2.0 );

	//FlowTransport
	init( CONNECTION_REJECTED_MESSAGE_DELAY,                   1.0 );
	init( CONNECTION_ID_TIMEOUT,                             600.0 ); if( randomize && BUGGIFY ) CONNECTION_ID_TIMEOUT = 60.0;
	init( CONNECTION_CLEANUP_DELAY,                          100.0 );
	init( INITIAL_RECONNECTION_TIME,                          0.05 );
	init( MAX_RECONNECTION_TIME,                               0.5 );
	init( RECONNECTION_TIME_GROWTH_RATE,                       1.2 );
	init( RECONNECTION_RESET_TIME,                             5.0 );
	init( ALWAYS_ACCEPT_DELAY,                                15.0 );
	init( ACCEPT_BATCH_SIZE,                                    10 );
	init( TOO_MANY_CONNECTIONS_CLOSED_RESET_DELAY,             5.0 );
	init( TOO_MANY_CONNECTIONS_CLOSED_TIMEOUT,                20.0 );
	init( PEER_UNAVAILABLE_FOR_LONG_TIME_TIMEOUT,           3600.0 );
	init( INCOMPATIBLE_PEER_DELAY_BEFORE_LOGGING,              5.0 );
	init( PING_LOGGING_INTERVAL,                               3.0 );
	init( PING_SAMPLE_AMOUNT,                                  100 );
	init( NETWORK_CONNECT_SAMPLE_AMOUNT,                       100 );

	init( TLS_CERT_REFRESH_DELAY_SECONDS,                 12*60*60 );
	init( TLS_SERVER_CONNECTION_THROTTLE_TIMEOUT,              9.0 );
	init( TLS_CLIENT_CONNECTION_THROTTLE_TIMEOUT,             11.0 );
	init( TLS_SERVER_CONNECTION_THROTTLE_ATTEMPTS,               1 );
	init( TLS_CLIENT_CONNECTION_THROTTLE_ATTEMPTS,               1 );
	init( TLS_CLIENT_HANDSHAKE_THREADS,                          0 );
	init( TLS_SERVER_HANDSHAKE_THREADS,                         64 );
	init( TLS_HANDSHAKE_THREAD_STACKSIZE,                64 * 1024 );
	init( TLS_MALLOC_ARENA_MAX,                                  6 );
	init( TLS_HANDSHAKE_LIMIT,                                1000 );

	init( NETWORK_TEST_CLIENT_COUNT,                            30 );
	init( NETWORK_TEST_REPLY_SIZE,                           600e3 );
	init( NETWORK_TEST_REQUEST_COUNT,                            0 ); // 0 -> run forever
	init( NETWORK_TEST_REQUEST_SIZE,                             1 );
	init( NETWORK_TEST_SCRIPT_MODE,                          false );

	//AsyncFileCached
	init( PAGE_CACHE_4K,                                   2LL<<30 );
	init( PAGE_CACHE_64K,                                200LL<<20 );
	init( SIM_PAGE_CACHE_4K,                                   1e8 );
	init( SIM_PAGE_CACHE_64K,                                  1e7 );
	init( BUGGIFY_SIM_PAGE_CACHE_4K,                           1e6 );
	init( BUGGIFY_SIM_PAGE_CACHE_64K,                          1e6 );
	init( MAX_EVICT_ATTEMPTS,                                  100 ); if( randomize && BUGGIFY ) MAX_EVICT_ATTEMPTS = 2;
	init( CACHE_EVICTION_POLICY,                          "random" );
	init( PAGE_CACHE_TRUNCATE_LOOKUP_FRACTION,                 0.1 ); if( randomize && BUGGIFY ) PAGE_CACHE_TRUNCATE_LOOKUP_FRACTION = 0.0; else if( randomize && BUGGIFY ) PAGE_CACHE_TRUNCATE_LOOKUP_FRACTION = 1.0;
	init( FLOW_CACHEDFILE_WRITE_IO_SIZE,                         0 );
	if ( randomize && BUGGIFY) {
		// Choose 16KB to 64KB as I/O size
		FLOW_CACHEDFILE_WRITE_IO_SIZE = deterministicRandom()->randomInt(16384, 65537);
	}

	//AsyncFileEIO
	init( EIO_MAX_PARALLELISM,                                  4  );
	init( EIO_USE_ODIRECT,                                      0  );

	//AsyncFileEncrypted
	init( ENCRYPTION_BLOCK_SIZE,                              4096 );
	init( MAX_DECRYPTED_BLOCKS,                                 10 );

	//AsyncFileKAIO
	init( MAX_OUTSTANDING,                                      64 );
	init( MIN_SUBMIT,                                           10 );

	init( PAGE_WRITE_CHECKSUM_HISTORY,                           0 ); if( randomize && BUGGIFY ) PAGE_WRITE_CHECKSUM_HISTORY = 10000000;
	init( DISABLE_POSIX_KERNEL_AIO,                              0 );

	//AsyncFileNonDurable
	init( NON_DURABLE_MAX_WRITE_DELAY,                         2.0 ); if( randomize && BUGGIFY ) NON_DURABLE_MAX_WRITE_DELAY = 5.0;
	init( MAX_PRIOR_MODIFICATION_DELAY,                        1.0 ); if( randomize && BUGGIFY ) MAX_PRIOR_MODIFICATION_DELAY = 10.0;

	//GenericActors
	init( BUGGIFY_FLOW_LOCK_RELEASE_DELAY,                     1.0 );
	init( LOW_PRIORITY_DELAY_COUNT,                              5 );
	init( LOW_PRIORITY_MAX_DELAY,                              5.0 );

	//IAsyncFile
	init( INCREMENTAL_DELETE_TRUNCATE_AMOUNT,                  5e8 ); //500MB
	init( INCREMENTAL_DELETE_INTERVAL,                         1.0 ); //every 1 second
		
	//Net2 and FlowTransport
	init( MIN_COALESCE_DELAY,                                10e-6 ); if( randomize && BUGGIFY ) MIN_COALESCE_DELAY = 0;
	init( MAX_COALESCE_DELAY,                                20e-6 ); if( randomize && BUGGIFY ) MAX_COALESCE_DELAY = 0;
	init( SLOW_LOOP_CUTOFF,                          15.0 / 1000.0 );
	init( SLOW_LOOP_SAMPLING_RATE,                             0.1 );
	init( TSC_YIELD_TIME,                                  1000000 );
	init( MIN_LOGGED_PRIORITY_BUSY_FRACTION,                  0.05 );
	init( CERT_FILE_MAX_SIZE,                      5 * 1024 * 1024 );
	init( READY_QUEUE_RESERVED_SIZE,                          8192 );
	init( ITERATIONS_PER_REACTOR_CHECK,                        100 );

	//Network
	init( PACKET_LIMIT,                                  100LL<<20 );
	init( PACKET_WARNING,                                  2LL<<20 );  // 2MB packet warning quietly allows for 1MB system messages
	init( TIME_OFFSET_LOGGING_INTERVAL,                       60.0 );
	init( MAX_PACKET_SEND_BYTES,                        128 * 1024 );
	init( MIN_PACKET_BUFFER_BYTES,                        4 * 1024 );
	init( MIN_PACKET_BUFFER_FREE_BYTES,                        256 );
	init( FLOW_TCP_NODELAY,                                      1 );
	init( FLOW_TCP_QUICKACK,                                     0 );

	//Sim2
	init( MIN_OPEN_TIME,                                    0.0002 );
	init( MAX_OPEN_TIME,                                    0.0012 );
	init( SIM_DISK_IOPS,                                      5000 );
	init( SIM_DISK_BANDWIDTH,                             50000000 );
	init( MIN_NETWORK_LATENCY,                              100e-6 );
	init( FAST_NETWORK_LATENCY,                             800e-6 );
	init( SLOW_NETWORK_LATENCY,                             100e-3 );
	init( MAX_CLOGGING_LATENCY,                                  0 ); if( randomize && BUGGIFY ) MAX_CLOGGING_LATENCY =  0.1 * deterministicRandom()->random01();
	init( MAX_BUGGIFIED_DELAY,                                   0 ); if( randomize && BUGGIFY ) MAX_BUGGIFIED_DELAY =  0.2 * deterministicRandom()->random01();
	init( SIM_CONNECT_ERROR_MODE, deterministicRandom()->randomInt(0,3) );

	//Tracefiles
	init( ZERO_LENGTH_FILE_PAD,                                  1 );
	init( TRACE_FLUSH_INTERVAL,                               0.25 );
	init( TRACE_RETRY_OPEN_INTERVAL,						  1.00 );
	init( MIN_TRACE_SEVERITY,                 isSimulated ? 1 : 10 ); // Related to the trace severity in Trace.h
	init( MAX_TRACE_SUPPRESSIONS,                              1e4 );
	init( TRACE_DATETIME_ENABLED,                             true ); // trace time in human readable format (always real time)
	init( TRACE_SYNC_ENABLED,                                    0 );
	init( TRACE_EVENT_METRIC_UNITS_PER_SAMPLE,                 500 );
	init( TRACE_EVENT_THROTTLER_SAMPLE_EXPIRY,              1800.0 ); // 30 mins
	init( TRACE_EVENT_THROTTLER_MSG_LIMIT,                   20000 );
	init( MAX_TRACE_FIELD_LENGTH,                              495 ); // If the value of this is changed, the corresponding default in Trace.cpp should be changed as well
	init( MAX_TRACE_EVENT_LENGTH,                             4000 ); // If the value of this is changed, the corresponding default in Trace.cpp should be changed as well
	init( ALLOCATION_TRACING_ENABLED,                         true );
	init( SIM_SPEEDUP_AFTER_SECONDS,                           450 );
	init( CODE_COV_TRACE_EVENT_SEVERITY,                        10 ); // Code coverage TraceEvent severity level

	//TDMetrics
	init( MAX_METRICS,                                         600 );
	init( MAX_METRIC_SIZE,                                    2500 );
	init( MAX_METRIC_LEVEL,                                     25 );
	init( METRIC_LEVEL_DIVISOR,                             log(4) );
	init( METRIC_LIMIT_START_QUEUE_SIZE,                        10 );  // The queue size at which to start restricting logging by disabling levels
	init( METRIC_LIMIT_RESPONSE_FACTOR,                         10 );  // The additional queue size at which to disable logging of another level (higher == less restrictive)

	//Load Balancing
	init( LOAD_BALANCE_ZONE_ID_LOCALITY_ENABLED,                 0 );
	init( LOAD_BALANCE_DC_ID_LOCALITY_ENABLED,                   1 );
	init( LOAD_BALANCE_MAX_BACKOFF,                            5.0 );
	init( LOAD_BALANCE_START_BACKOFF,                         0.01 );
	init( LOAD_BALANCE_BACKOFF_RATE,                           2.0 );
	init( MAX_LAGGING_REQUESTS_OUTSTANDING,                 100000 );
	init( INSTANT_SECOND_REQUEST_MULTIPLIER,                   2.0 );
	init( BASE_SECOND_REQUEST_TIME,                         0.0005 );
	init( SECOND_REQUEST_MULTIPLIER_GROWTH,                   0.01 );
	init( SECOND_REQUEST_MULTIPLIER_DECAY,                 0.00025 );
	init( SECOND_REQUEST_BUDGET_GROWTH,                       0.05 );
	init( SECOND_REQUEST_MAX_BUDGET,                         100.0 );
	init( ALTERNATIVES_FAILURE_RESET_TIME,                     5.0 );
	init( ALTERNATIVES_FAILURE_MIN_DELAY,                     0.05 );
	init( ALTERNATIVES_FAILURE_DELAY_RATIO,                    0.2 );
	init( ALTERNATIVES_FAILURE_MAX_DELAY,                      1.0 );
	init( ALTERNATIVES_FAILURE_SLOW_DELAY_RATIO,              0.04 );
	init( ALTERNATIVES_FAILURE_SLOW_MAX_DELAY,                30.0 );
	init( ALTERNATIVES_FAILURE_SKIP_DELAY,                     1.0 );
	init( FUTURE_VERSION_INITIAL_BACKOFF,                      1.0 );
	init( FUTURE_VERSION_MAX_BACKOFF,                          8.0 );
	init( FUTURE_VERSION_BACKOFF_GROWTH,                       2.0 );
	init( LOAD_BALANCE_MAX_BAD_OPTIONS,                          1 ); //should be the same as MAX_MACHINES_FALLING_BEHIND
	init( LOAD_BALANCE_PENALTY_IS_BAD,                        true );
	init( BASIC_LOAD_BALANCE_UPDATE_RATE,                     10.0 ); //should be longer than the rate we log network metrics
	init( BASIC_LOAD_BALANCE_MAX_CHANGE,                      0.10 );
	init( BASIC_LOAD_BALANCE_MAX_PROB,                         2.0 );
	init( BASIC_LOAD_BALANCE_MIN_REQUESTS,                      20 ); //do not adjust LB probabilities if the proxies are less than releasing less than 20 transactions per second
	init( BASIC_LOAD_BALANCE_MIN_CPU,                         0.05 ); //do not adjust LB probabilities if the proxies are less than 5% utilized
	init( BASIC_LOAD_BALANCE_BUCKETS,                           40 ); //proxies bin recent GRV requests into 40 time bins
	init( BASIC_LOAD_BALANCE_COMPUTE_PRECISION,              10000 ); //determines how much of the LB usage is holding the CPU usage of the proxy
	init( LOAD_BALANCE_TSS_TIMEOUT,                            5.0 );
	init( LOAD_BALANCE_TSS_MISMATCH_VERIFY_SS,                true ); if( randomize && BUGGIFY ) LOAD_BALANCE_TSS_MISMATCH_VERIFY_SS = false; // Whether the client should validate the SS teams all agree on TSS mismatch
	init( LOAD_BALANCE_TSS_MISMATCH_TRACE_FULL,              false ); if( randomize && BUGGIFY ) LOAD_BALANCE_TSS_MISMATCH_TRACE_FULL = true; // If true, saves the full details of the mismatch in a trace event. If false, saves them in the DB and the trace event references the DB row.
	init( TSS_LARGE_TRACE_SIZE,                              50000 );

	// Health Monitor
	init( FAILURE_DETECTION_DELAY,                             4.0 ); if( randomize && BUGGIFY ) FAILURE_DETECTION_DELAY = 1.0;
	init( HEALTH_MONITOR_MARK_FAILED_UNSTABLE_CONNECTIONS,    true );
	init( HEALTH_MONITOR_CLIENT_REQUEST_INTERVAL_SECS,          30 );
	init( HEALTH_MONITOR_CONNECTION_MAX_CLOSED,                  5 );
}
// clang-format on

static std::string toLower(std::string const& name) {
	std::string lower_name;
	for (auto c = name.begin(); c != name.end(); ++c)
		if (*c >= 'A' && *c <= 'Z')
			lower_name += *c - 'A' + 'a';
		else
			lower_name += *c;
	return lower_name;
}

ParsedKnobValue Knobs::parseKnobValue(std::string const& knob, std::string const& value) const {
	try {
		if (double_knobs.count(knob)) {
			return std::stod(value);
		} else if (bool_knobs.count(knob)) {
			if (toLower(value) == "true") {
				return true;
			} else if (toLower(value) == "false") {
				return false;
			} else {
				return (std::stoi(value) != 0);
			}
		} else if (int64_knobs.count(knob)) {
			return static_cast<int64_t>(std::stol(value, nullptr, 0));
		} else if (int_knobs.count(knob)) {
			return std::stoi(value, nullptr, 0);
		} else if (string_knobs.count(knob)) {
			return value;
		}
		return NoKnobFound{};
	} catch (...) {
		throw invalid_option_value();
	}
}

bool Knobs::setKnob(std::string const& knob, int value) {
	if (!int_knobs.count(knob)) {
		return false;
	}
	*int_knobs[knob].value = value;
	explicitlySetKnobs.insert(toLower(knob));
	return true;
}

bool Knobs::setKnob(std::string const& knob, int64_t value) {
	if (!int64_knobs.count(knob)) {
		return false;
	}
	*int64_knobs[knob].value = value;
	explicitlySetKnobs.insert(toLower(knob));
	return true;
}

bool Knobs::setKnob(std::string const& knob, bool value) {
	if (!bool_knobs.count(knob)) {
		return false;
	}
	*bool_knobs[knob].value = value;
	explicitlySetKnobs.insert(toLower(knob));
	return true;
}

bool Knobs::setKnob(std::string const& knob, double value) {
	if (!double_knobs.count(knob)) {
		return false;
	}
	*double_knobs[knob].value = value;
	explicitlySetKnobs.insert(toLower(knob));
	return true;
}

bool Knobs::setKnob(std::string const& knob, std::string const& value) {
	if (!string_knobs.count(knob)) {
		return false;
	}
	*string_knobs[knob].value = value;
	explicitlySetKnobs.insert(toLower(knob));
	return true;
}

ParsedKnobValue Knobs::getKnob(const std::string& name) const {
	if (double_knobs.count(name) > 0) {
		return ParsedKnobValue{ *double_knobs.at(name).value };
	}
	if (int64_knobs.count(name) > 0) {
		return ParsedKnobValue{ *int64_knobs.at(name).value };
	}
	if (int_knobs.count(name) > 0) {
		return ParsedKnobValue{ *int_knobs.at(name).value };
	}
	if (string_knobs.count(name) > 0) {
		return ParsedKnobValue{ *string_knobs.at(name).value };
	}
	if (bool_knobs.count(name) > 0) {
		return ParsedKnobValue{ *bool_knobs.at(name).value };
	}

	return ParsedKnobValue{ NoKnobFound() };
}

bool Knobs::isAtomic(std::string const& knob) const {
	if (double_knobs.count(knob)) {
		return double_knobs.find(knob)->second.atomic == Atomic::YES;
	} else if (int64_knobs.count(knob)) {
		return int64_knobs.find(knob)->second.atomic == Atomic::YES;
	} else if (int_knobs.count(knob)) {
		return int_knobs.find(knob)->second.atomic == Atomic::YES;
	} else if (string_knobs.count(knob)) {
		return string_knobs.find(knob)->second.atomic == Atomic::YES;
	} else if (bool_knobs.count(knob)) {
		return bool_knobs.find(knob)->second.atomic == Atomic::YES;
	}
	return false;
}

void Knobs::initKnob(double& knob, double value, std::string const& name, Atomic atomic) {
	if (!explicitlySetKnobs.count(toLower(name))) {
		knob = value;
		double_knobs[toLower(name)] = KnobValue<double>{ &knob, atomic };
	}
}

void Knobs::initKnob(int64_t& knob, int64_t value, std::string const& name, Atomic atomic) {
	if (!explicitlySetKnobs.count(toLower(name))) {
		knob = value;
		int64_knobs[toLower(name)] = KnobValue<int64_t>{ &knob, atomic };
	}
}

void Knobs::initKnob(int& knob, int value, std::string const& name, Atomic atomic) {
	if (!explicitlySetKnobs.count(toLower(name))) {
		knob = value;
		int_knobs[toLower(name)] = KnobValue<int>{ &knob, atomic };
	}
}

void Knobs::initKnob(std::string& knob, const std::string& value, const std::string& name, Atomic atomic) {
	if (!explicitlySetKnobs.count(toLower(name))) {
		knob = value;
		string_knobs[toLower(name)] = KnobValue<std::string>{ &knob, atomic };
	}
}

void Knobs::initKnob(bool& knob, bool value, std::string const& name, Atomic atomic) {
	if (!explicitlySetKnobs.count(toLower(name))) {
		knob = value;
		bool_knobs[toLower(name)] = KnobValue<bool>{ &knob, atomic };
	}
}

void Knobs::trace() const {
	for (auto& k : double_knobs)
		TraceEvent("Knob")
		    .detail("Name", k.first.c_str())
		    .detail("Value", *k.second.value)
		    .detail("Atomic", k.second.atomic);
	for (auto& k : int_knobs)
		TraceEvent("Knob")
		    .detail("Name", k.first.c_str())
		    .detail("Value", *k.second.value)
		    .detail("Atomic", k.second.atomic);
	for (auto& k : int64_knobs)
		TraceEvent("Knob")
		    .detail("Name", k.first.c_str())
		    .detail("Value", *k.second.value)
		    .detail("Atomic", k.second.atomic);
	for (auto& k : string_knobs)
		TraceEvent("Knob")
		    .detail("Name", k.first.c_str())
		    .detail("Value", *k.second.value)
		    .detail("Atomic", k.second.atomic);
	for (auto& k : bool_knobs)
		TraceEvent("Knob")
		    .detail("Name", k.first.c_str())
		    .detail("Value", *k.second.value)
		    .detail("Atomic", k.second.atomic);
}
