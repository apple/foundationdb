/*
 * Knobs.h
 *
 * This source file is part of the FoundationDB open source project
 *
 * Copyright 2013-2018 Apple Inc. and the FoundationDB project authors
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

#ifndef FLOW_KNOBS_H
#define FLOW_KNOBS_H
#pragma once

#include "Platform.h"

#include <map>
#include <string>
#include <stdint.h>

class Knobs {
public:
	bool setKnob( std::string const& name, std::string const& value ); // Returns true if the knob name is known, false if it is unknown
	void trace();

protected:
	void initKnob( double& knob, double value, std::string const& name );
	void initKnob( int64_t& knob, int64_t value, std::string const& name );
	void initKnob( int& knob, int value, std::string const& name );
	void initKnob( std::string& knob, const std::string& value, const std::string& name );

	std::map<std::string, double*> double_knobs;
	std::map<std::string, int64_t*> int64_knobs;
	std::map<std::string, int*> int_knobs;
	std::map<std::string, std::string*> string_knobs;
};

class FlowKnobs : public Knobs {
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

	//slow task profiling
	double SLOWTASK_PROFILING_INTERVAL;
	double SLOWTASK_PROFILING_MAX_LOG_INTERVAL;
	double SLOWTASK_PROFILING_LOG_BACKOFF;

	//connectionMonitor
	double CONNECTION_MONITOR_LOOP_TIME;
	double CONNECTION_MONITOR_TIMEOUT;

	//FlowTransport
	double CONNECTION_REJECTED_MESSAGE_DELAY;
	double CONNECTION_ID_TIMEOUT;
	double CONNECTION_CLEANUP_DELAY;
	double INITIAL_RECONNECTION_TIME;
	double MAX_RECONNECTION_TIME;
	double RECONNECTION_TIME_GROWTH_RATE;
	double RECONNECTION_RESET_TIME;

	//AsyncFileCached
	int64_t PAGE_CACHE_4K;
	int64_t PAGE_CACHE_64K;
	int64_t SIM_PAGE_CACHE_4K;
	int64_t SIM_PAGE_CACHE_64K;
	int64_t BUGGIFY_SIM_PAGE_CACHE_4K;
	int64_t BUGGIFY_SIM_PAGE_CACHE_64K;
	int MAX_EVICT_ATTEMPTS;

	//AsyncFileKAIO
	int MAX_OUTSTANDING;
	int MIN_SUBMIT;

	int PAGE_WRITE_CHECKSUM_HISTORY;

	//AsyncFileNonDurable
	double MAX_PRIOR_MODIFICATION_DELAY;

	//GenericActors
	double MAX_DELIVER_DUPLICATE_DELAY;
	double BUGGIFY_FLOW_LOCK_RELEASE_DELAY;

	//IAsyncFile
	int64_t INCREMENTAL_DELETE_TRUNCATE_AMOUNT;
	double INCREMENTAL_DELETE_INTERVAL;

	//Net2
	double MIN_COALESCE_DELAY;
	double MAX_COALESCE_DELAY;
	double SLOW_LOOP_CUTOFF;
	double SLOW_LOOP_SAMPLING_RATE;
	int64_t TSC_YIELD_TIME;
	int64_t REACTOR_FLAGS;

	//Network
	int64_t PACKET_LIMIT;
	int64_t PACKET_WARNING;  // 2MB packet warning quietly allows for 1MB system messages
	double TIME_OFFSET_LOGGING_INTERVAL;

	//Sim2
	//FIMXE: more parameters could be factored out
	double MIN_OPEN_TIME;
	double MAX_OPEN_TIME;
	int64_t SIM_DISK_IOPS;
	int64_t SIM_DISK_BANDWIDTH;
	double MIN_NETWORK_LATENCY;
	double FAST_NETWORK_LATENCY;
	double SLOW_NETWORK_LATENCY;
	double MAX_CLOGGING_LATENCY;
	double MAX_BUGGIFIED_DELAY;

	//Tracefiles
	int ZERO_LENGTH_FILE_PAD;
	double TRACE_FLUSH_INTERVAL;
	double TRACE_RETRY_OPEN_INTERVAL;
	int MIN_TRACE_SEVERITY;
	int MAX_TRACE_SUPPRESSIONS;
	int TRACE_FSYNC_ENABLED;
	int TRACE_EVENT_METRIC_UNITS_PER_SAMPLE;
	int TRACE_EVENT_THROTLLER_SAMPLE_EXPIRY;
	int TRACE_EVENT_THROTTLER_MSG_LIMIT;

	//TDMetrics
	int64_t MAX_METRIC_SIZE;
	int64_t MAX_METRIC_LEVEL;
	double METRIC_LEVEL_DIVISOR;
	int METRIC_LIMIT_START_QUEUE_SIZE;
	int METRIC_LIMIT_RESPONSE_FACTOR;
	int MAX_METRICS;

	//Load Balancing
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
	double ALTERNATIVES_FAILURE_MAX_DELAY;
	double ALTERNATIVES_FAILURE_MIN_DELAY;
	double ALTERNATIVES_FAILURE_DELAY_RATIO;

	FlowKnobs(bool randomize = false, bool isSimulated = false);
};

extern FlowKnobs const* FLOW_KNOBS;

#endif
