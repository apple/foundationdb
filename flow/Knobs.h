/*
 * Knobs.h
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

#ifndef __FLOW_KNOBS_H__
#define __FLOW_KNOBS_H__

#pragma once

#include "flow/Platform.h"

#include <map>
#include <set>
#include <string>
#include <stdint.h>
#include <variant>
#include <optional>

// Helper macros to allow the init macro to be called with an optional third
// parameter, used to explicit set atomicity of knobs.
#define KNOB_FN(_1, _2, _3, FN, ...) FN
#define INIT_KNOB(knob, value) initKnob(knob, value, #knob)
#define INIT_ATOMIC_KNOB(knob, value, atomic) initKnob(knob, value, #knob, atomic)

// NOTE: Directly using KnobValueRef as the return type for Knobs::parseKnobValue would result
// in a cyclic dependency, so we use this intermediate ParsedKnobValue type
struct NoKnobFound {};
using ParsedKnobValue = std::variant<NoKnobFound, int, double, int64_t, bool, std::string>;

enum class ConfigDBType {
	DISABLED,
	SIMPLE,
	PAXOS,
};

enum class Atomic { YES, NO };

class Knobs {
protected:
	template <class T>
	struct KnobValue {
		T* value;
		Atomic atomic;
	};

	Knobs() = default;
	Knobs(Knobs const&) = delete;
	Knobs& operator=(Knobs const&) = delete;
	void initKnob(double& knob, double value, std::string const& name, Atomic atomic = Atomic::YES);
	void initKnob(int64_t& knob, int64_t value, std::string const& name, Atomic atomic = Atomic::YES);
	void initKnob(int& knob, int value, std::string const& name, Atomic atomic = Atomic::YES);
	void initKnob(std::string& knob, const std::string& value, const std::string& name, Atomic atomic = Atomic::YES);
	void initKnob(bool& knob, bool value, std::string const& name, Atomic atomic = Atomic::YES);

	std::map<std::string, KnobValue<double>> double_knobs;
	std::map<std::string, KnobValue<int64_t>> int64_knobs;
	std::map<std::string, KnobValue<int>> int_knobs;
	std::map<std::string, KnobValue<std::string>> string_knobs;
	std::map<std::string, KnobValue<bool>> bool_knobs;
	std::set<std::string> explicitlySetKnobs;

public:
	// Sets an integer value to an integer knob, returns false if the knob does not exist or type mismatch
	bool setKnob(std::string const& name, int value);

	// Sets a boolean value to a bool knob, returns false if the knob does not exist or type mismatch
	bool setKnob(std::string const& name, bool value);

	// Sets an int64_t value to an int64_t knob, returns false if the knob does not exist or type mismatch
	bool setKnob(std::string const& name, int64_t value);

	// Sets a double value to a double knob, returns false if the knob does not exist or type mismatch
	bool setKnob(std::string const& name, double value);

	// Sets a string value to a string knob, returns false if the knob does not exist or type mismatch
	bool setKnob(std::string const& name, std::string const& value);

	// Gets the value of knob
	ParsedKnobValue getKnob(const std::string& name) const;

	ParsedKnobValue parseKnobValue(std::string const& name, std::string const& value) const;
	bool isAtomic(std::string const& knob) const;
	void trace() const;
};

template <class T>
class KnobsImpl : public Knobs {
public:
	template <class... Args>
	void reset(Args&&... args) {
		explicitlySetKnobs.clear();
		static_cast<T*>(this)->initialize(std::forward<Args>(args)...);
	}
};

class FlowKnobs : public KnobsImpl<FlowKnobs> {
public:
	int AUTOMATIC_TRACE_DUMP;
	double PREVENT_FAST_SPIN_DELAY;
	double CACHE_REFRESH_INTERVAL_WHEN_ALL_ALTERNATIVES_FAILED;

	double DELAY_JITTER_OFFSET;
	double DELAY_JITTER_RANGE;
	double BUSY_WAIT_THRESHOLD;
	double CLIENT_REQUEST_INTERVAL;
	double SERVER_REQUEST_INTERVAL;

	int DISABLE_ASSERTS;
	double QUEUE_MODEL_SMOOTHING_AMOUNT;

	int RANDOMSEED_RETRY_LIMIT;
	double FAST_ALLOC_LOGGING_BYTES;
	double HUGE_ARENA_LOGGING_BYTES;
	double HUGE_ARENA_LOGGING_INTERVAL;

	// Chaos testing
	bool ENABLE_CHAOS_FEATURES;
	double CHAOS_LOGGING_INTERVAL;

	bool WRITE_TRACING_ENABLED;
	double TRACING_SAMPLE_RATE;
	std::string TRACING_UDP_LISTENER_ADDR;
	int TRACING_UDP_LISTENER_PORT;

	// run loop profiling
	double RUN_LOOP_PROFILING_INTERVAL;
	double SLOWTASK_PROFILING_LOG_INTERVAL;
	double SLOWTASK_PROFILING_MAX_LOG_INTERVAL;
	double SLOWTASK_PROFILING_LOG_BACKOFF;
	double SLOWTASK_BLOCKED_INTERVAL;
	double SATURATION_PROFILING_LOG_INTERVAL;
	double SATURATION_PROFILING_MAX_LOG_INTERVAL;
	double SATURATION_PROFILING_LOG_BACKOFF;

	// connectionMonitor
	double CONNECTION_MONITOR_LOOP_TIME;
	double CONNECTION_MONITOR_TIMEOUT;
	double CONNECTION_MONITOR_IDLE_TIMEOUT;
	double CONNECTION_MONITOR_INCOMING_IDLE_MULTIPLIER;
	double CONNECTION_MONITOR_UNREFERENCED_CLOSE_DELAY;

	// FlowTransport
	double CONNECTION_REJECTED_MESSAGE_DELAY;
	double CONNECTION_ID_TIMEOUT;
	double CONNECTION_CLEANUP_DELAY;
	double INITIAL_RECONNECTION_TIME;
	double MAX_RECONNECTION_TIME;
	double RECONNECTION_TIME_GROWTH_RATE;
	double RECONNECTION_RESET_TIME;
	double ALWAYS_ACCEPT_DELAY;
	int ACCEPT_BATCH_SIZE;
	double INCOMPATIBLE_PEER_DELAY_BEFORE_LOGGING;
	double PING_LOGGING_INTERVAL;
	int PING_SAMPLE_AMOUNT;
	int NETWORK_CONNECT_SAMPLE_AMOUNT;

	int TLS_CERT_REFRESH_DELAY_SECONDS;
	double TLS_SERVER_CONNECTION_THROTTLE_TIMEOUT;
	double TLS_CLIENT_CONNECTION_THROTTLE_TIMEOUT;
	int TLS_SERVER_CONNECTION_THROTTLE_ATTEMPTS;
	int TLS_CLIENT_CONNECTION_THROTTLE_ATTEMPTS;
	int TLS_CLIENT_HANDSHAKE_THREADS;
	int TLS_SERVER_HANDSHAKE_THREADS;
	int TLS_HANDSHAKE_THREAD_STACKSIZE;
	int TLS_MALLOC_ARENA_MAX;
	int TLS_HANDSHAKE_LIMIT;

	int NETWORK_TEST_CLIENT_COUNT;
	int NETWORK_TEST_REPLY_SIZE;
	int NETWORK_TEST_REQUEST_COUNT;
	int NETWORK_TEST_REQUEST_SIZE;
	bool NETWORK_TEST_SCRIPT_MODE;

	// AsyncFileCached
	int64_t PAGE_CACHE_4K;
	int64_t PAGE_CACHE_64K;
	int64_t SIM_PAGE_CACHE_4K;
	int64_t SIM_PAGE_CACHE_64K;
	int64_t BUGGIFY_SIM_PAGE_CACHE_4K;
	int64_t BUGGIFY_SIM_PAGE_CACHE_64K;
	std::string CACHE_EVICTION_POLICY; // for now, "random", "lru", are supported
	int MAX_EVICT_ATTEMPTS;
	double PAGE_CACHE_TRUNCATE_LOOKUP_FRACTION;
	double TOO_MANY_CONNECTIONS_CLOSED_RESET_DELAY;
	int TOO_MANY_CONNECTIONS_CLOSED_TIMEOUT;
	int PEER_UNAVAILABLE_FOR_LONG_TIME_TIMEOUT;
	int FLOW_CACHEDFILE_WRITE_IO_SIZE;

	// AsyncFileEIO
	int EIO_MAX_PARALLELISM;
	int EIO_USE_ODIRECT;

	// AsyncFileEncrypted
	int ENCRYPTION_BLOCK_SIZE;
	int MAX_DECRYPTED_BLOCKS;

	// AsyncFileKAIO
	int MAX_OUTSTANDING;
	int MIN_SUBMIT;

	int PAGE_WRITE_CHECKSUM_HISTORY;
	int DISABLE_POSIX_KERNEL_AIO;

	// AsyncFileNonDurable
	double NON_DURABLE_MAX_WRITE_DELAY;
	double MAX_PRIOR_MODIFICATION_DELAY;

	// GenericActors
	double BUGGIFY_FLOW_LOCK_RELEASE_DELAY;
	int LOW_PRIORITY_DELAY_COUNT;
	double LOW_PRIORITY_MAX_DELAY;

	// IAsyncFile
	int64_t INCREMENTAL_DELETE_TRUNCATE_AMOUNT;
	double INCREMENTAL_DELETE_INTERVAL;

	// Net2
	double MIN_COALESCE_DELAY;
	double MAX_COALESCE_DELAY;
	double SLOW_LOOP_CUTOFF;
	double SLOW_LOOP_SAMPLING_RATE;
	int64_t TSC_YIELD_TIME;
	int64_t REACTOR_FLAGS;
	double MIN_LOGGED_PRIORITY_BUSY_FRACTION;
	int CERT_FILE_MAX_SIZE;
	int READY_QUEUE_RESERVED_SIZE;
	int ITERATIONS_PER_REACTOR_CHECK;

	// Network
	int64_t PACKET_LIMIT;
	int64_t PACKET_WARNING; // 2MB packet warning quietly allows for 1MB system messages
	double TIME_OFFSET_LOGGING_INTERVAL;
	int MAX_PACKET_SEND_BYTES;
	int MIN_PACKET_BUFFER_BYTES;
	int MIN_PACKET_BUFFER_FREE_BYTES;
	int FLOW_TCP_NODELAY;
	int FLOW_TCP_QUICKACK;

	// Sim2
	// FIMXE: more parameters could be factored out
	double MIN_OPEN_TIME;
	double MAX_OPEN_TIME;
	int64_t SIM_DISK_IOPS;
	int64_t SIM_DISK_BANDWIDTH;
	double MIN_NETWORK_LATENCY;
	double FAST_NETWORK_LATENCY;
	double SLOW_NETWORK_LATENCY;
	double MAX_CLOGGING_LATENCY;
	double MAX_BUGGIFIED_DELAY;
	int SIM_CONNECT_ERROR_MODE;
	double SIM_SPEEDUP_AFTER_SECONDS;

	// Tracefiles
	int ZERO_LENGTH_FILE_PAD;
	double TRACE_FLUSH_INTERVAL;
	double TRACE_RETRY_OPEN_INTERVAL;
	int MIN_TRACE_SEVERITY;
	int MAX_TRACE_SUPPRESSIONS;
	bool TRACE_DATETIME_ENABLED;
	int TRACE_SYNC_ENABLED;
	int TRACE_EVENT_METRIC_UNITS_PER_SAMPLE;
	int TRACE_EVENT_THROTTLER_SAMPLE_EXPIRY;
	int TRACE_EVENT_THROTTLER_MSG_LIMIT;
	int MAX_TRACE_FIELD_LENGTH;
	int MAX_TRACE_EVENT_LENGTH;
	bool ALLOCATION_TRACING_ENABLED;
	int CODE_COV_TRACE_EVENT_SEVERITY;

	// TDMetrics
	int64_t MAX_METRIC_SIZE;
	int64_t MAX_METRIC_LEVEL;
	double METRIC_LEVEL_DIVISOR;
	int METRIC_LIMIT_START_QUEUE_SIZE;
	int METRIC_LIMIT_RESPONSE_FACTOR;
	int MAX_METRICS;

	// Load Balancing
	int LOAD_BALANCE_ZONE_ID_LOCALITY_ENABLED;
	int LOAD_BALANCE_DC_ID_LOCALITY_ENABLED;
	double LOAD_BALANCE_MAX_BACKOFF;
	double LOAD_BALANCE_START_BACKOFF;
	double LOAD_BALANCE_BACKOFF_RATE;
	int64_t MAX_LAGGING_REQUESTS_OUTSTANDING;
	double INSTANT_SECOND_REQUEST_MULTIPLIER;
	double BASE_SECOND_REQUEST_TIME;
	double SECOND_REQUEST_MULTIPLIER_GROWTH;
	double SECOND_REQUEST_MULTIPLIER_DECAY;
	double SECOND_REQUEST_BUDGET_GROWTH;
	double SECOND_REQUEST_MAX_BUDGET;
	double ALTERNATIVES_FAILURE_RESET_TIME;
	double ALTERNATIVES_FAILURE_MIN_DELAY;
	double ALTERNATIVES_FAILURE_DELAY_RATIO;
	double ALTERNATIVES_FAILURE_MAX_DELAY;
	double ALTERNATIVES_FAILURE_SLOW_DELAY_RATIO;
	double ALTERNATIVES_FAILURE_SLOW_MAX_DELAY;
	double ALTERNATIVES_FAILURE_SKIP_DELAY;
	double FUTURE_VERSION_INITIAL_BACKOFF;
	double FUTURE_VERSION_MAX_BACKOFF;
	double FUTURE_VERSION_BACKOFF_GROWTH;
	int LOAD_BALANCE_MAX_BAD_OPTIONS;
	bool LOAD_BALANCE_PENALTY_IS_BAD;
	double BASIC_LOAD_BALANCE_UPDATE_RATE;
	double BASIC_LOAD_BALANCE_MAX_CHANGE;
	double BASIC_LOAD_BALANCE_MAX_PROB;
	int BASIC_LOAD_BALANCE_BUCKETS;
	int BASIC_LOAD_BALANCE_COMPUTE_PRECISION;
	double BASIC_LOAD_BALANCE_MIN_REQUESTS;
	double BASIC_LOAD_BALANCE_MIN_CPU;
	double LOAD_BALANCE_TSS_TIMEOUT;
	bool LOAD_BALANCE_TSS_MISMATCH_VERIFY_SS;
	bool LOAD_BALANCE_TSS_MISMATCH_TRACE_FULL;
	int TSS_LARGE_TRACE_SIZE;

	// Health Monitor
	int FAILURE_DETECTION_DELAY;
	bool HEALTH_MONITOR_MARK_FAILED_UNSTABLE_CONNECTIONS;
	int HEALTH_MONITOR_CLIENT_REQUEST_INTERVAL_SECS;
	int HEALTH_MONITOR_CONNECTION_MAX_CLOSED;
	FlowKnobs(class Randomize, class IsSimulated);
	void initialize(class Randomize, class IsSimulated);
};

// Flow knobs are needed before the knob collections are available, so a global FlowKnobs object is used to bootstrap
extern FlowKnobs bootstrapGlobalFlowKnobs;
extern FlowKnobs const* FLOW_KNOBS;

#endif
