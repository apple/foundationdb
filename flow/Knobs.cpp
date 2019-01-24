/*
 * Knobs.cpp
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

#include "flow/Knobs.h"
#include "flow/flow.h"
#include <cmath>

FlowKnobs const* FLOW_KNOBS = new FlowKnobs();

#define init( knob, value ) initKnob( knob, value, #knob )

FlowKnobs::FlowKnobs(bool randomize, bool isSimulated) {
	init( AUTOMATIC_TRACE_DUMP,                                  1 );
	init( PREVENT_FAST_SPIN_DELAY,                             .01 );
	init( CACHE_REFRESH_INTERVAL_WHEN_ALL_ALTERNATIVES_FAILED, 1.0 );

	init( DELAY_JITTER_OFFSET,                                 0.9 );
	init( DELAY_JITTER_RANGE,                                  0.2 );
	init( BUSY_WAIT_THRESHOLD,                                   0 ); // 1e100 == never sleep
	init( CLIENT_REQUEST_INTERVAL,                             0.1 ); if( randomize && BUGGIFY ) CLIENT_REQUEST_INTERVAL = 1.0;
	init( SERVER_REQUEST_INTERVAL,                             0.1 ); if( randomize && BUGGIFY ) SERVER_REQUEST_INTERVAL = 1.0;

	init( REACTOR_FLAGS,                                         0 );

	init( DISABLE_ASSERTS,                                       0 );
	init( QUEUE_MODEL_SMOOTHING_AMOUNT,                        2.0 );

	init( SLOWTASK_PROFILING_INTERVAL,                       0.125 ); // A value of 0 disables SlowTask profiling
	init( SLOWTASK_PROFILING_MAX_LOG_INTERVAL,                 1.0 );
	init( SLOWTASK_PROFILING_LOG_BACKOFF,                      2.0 );

	init( RANDOMSEED_RETRY_LIMIT,                                4 );

	//connectionMonitor
	init( CONNECTION_MONITOR_LOOP_TIME,   isSimulated ? 0.75 : 1.0 ); if( randomize && BUGGIFY ) CONNECTION_MONITOR_LOOP_TIME = 6.0;
	init( CONNECTION_MONITOR_TIMEOUT,     isSimulated ? 1.50 : 2.0 ); if( randomize && BUGGIFY ) CONNECTION_MONITOR_TIMEOUT = 6.0;

	//FlowTransport
	init( CONNECTION_REJECTED_MESSAGE_DELAY,                   1.0 );
	init( CONNECTION_ID_TIMEOUT,                             600.0 ); if( randomize && BUGGIFY ) CONNECTION_ID_TIMEOUT = 60.0;
	init( CONNECTION_CLEANUP_DELAY,                          100.0 );
	init( INITIAL_RECONNECTION_TIME,                          0.05 );
	init( MAX_RECONNECTION_TIME,                               0.5 );
	init( RECONNECTION_TIME_GROWTH_RATE,                       1.2 );
	init( RECONNECTION_RESET_TIME,                             5.0 );
	init( CONNECTION_ACCEPT_DELAY,                            0.01 );

	init( TLS_CERT_REFRESH_DELAY_SECONDS,                 12*60*60 );

	//AsyncFileCached
	init( PAGE_CACHE_4K,                                2000LL<<20 );
	init( PAGE_CACHE_64K,                                200LL<<20 );
	init( SIM_PAGE_CACHE_4K,                                   1e8 );
	init( SIM_PAGE_CACHE_64K,                                  1e7 );
	init( BUGGIFY_SIM_PAGE_CACHE_4K,                           1e6 );
	init( BUGGIFY_SIM_PAGE_CACHE_64K,                          1e6 );
	init( MAX_EVICT_ATTEMPTS,                                  100 ); if( randomize && BUGGIFY ) MAX_EVICT_ATTEMPTS = 2;
	init( PAGE_CACHE_TRUNCATE_LOOKUP_FRACTION,                 0.1 ); if( randomize && BUGGIFY ) PAGE_CACHE_TRUNCATE_LOOKUP_FRACTION = 0.0; else if( randomize && BUGGIFY ) PAGE_CACHE_TRUNCATE_LOOKUP_FRACTION = 1.0;

	//AsyncFileKAIO
	init( MAX_OUTSTANDING,                                      64 );
	init( MIN_SUBMIT,                                           10 );

	init( PAGE_WRITE_CHECKSUM_HISTORY,                           0 ); if( randomize && BUGGIFY ) PAGE_WRITE_CHECKSUM_HISTORY = 10000000;

	//AsyncFileNonDurable
	init( MAX_PRIOR_MODIFICATION_DELAY,                        1.0 ); if( randomize && BUGGIFY ) MAX_PRIOR_MODIFICATION_DELAY = 10.0;

	//GenericActors
	init( MAX_DELIVER_DUPLICATE_DELAY,                         1.0 ); if( randomize && BUGGIFY ) MAX_DELIVER_DUPLICATE_DELAY = 10.0;
	init( BUGGIFY_FLOW_LOCK_RELEASE_DELAY,                     1.0 );

	//IAsyncFile
	init( INCREMENTAL_DELETE_TRUNCATE_AMOUNT,                  5e8 ); //500MB
	init( INCREMENTAL_DELETE_INTERVAL,                         1.0 ); //every 1 second
		
	//Net2 and FlowTransport
	init( MIN_COALESCE_DELAY,                                10e-6 ); if( randomize && BUGGIFY ) MIN_COALESCE_DELAY = 0;
	init( MAX_COALESCE_DELAY,                                20e-6 ); if( randomize && BUGGIFY ) MAX_COALESCE_DELAY = 0;
	init( SLOW_LOOP_CUTOFF,                          15.0 / 1000.0 );
	init( SLOW_LOOP_SAMPLING_RATE,                             0.1 );
	init( TSC_YIELD_TIME,                                  1000000 );

	//Network
	init( PACKET_LIMIT,                                  100LL<<20 );
	init( PACKET_WARNING,                                  2LL<<20 );  // 2MB packet warning quietly allows for 1MB system messages
	init( TIME_OFFSET_LOGGING_INTERVAL,                       60.0 );

	//Sim2
	init( MIN_OPEN_TIME,                                    0.0002 );
	init( MAX_OPEN_TIME,                                    0.0012 );
	init( SIM_DISK_IOPS,                                      5000 );
	init( SIM_DISK_BANDWIDTH,                             50000000 );
	init( MIN_NETWORK_LATENCY,                              100e-6 );
	init( FAST_NETWORK_LATENCY,                             800e-6 );
	init( SLOW_NETWORK_LATENCY,                             100e-3 );
	init( MAX_CLOGGING_LATENCY,                                  0 ); if( randomize && BUGGIFY ) MAX_CLOGGING_LATENCY =  0.1 * g_random->random01();
	init( MAX_BUGGIFIED_DELAY,                                   0 ); if( randomize && BUGGIFY ) MAX_BUGGIFIED_DELAY =  0.2 * g_random->random01();

	//Tracefiles
	init( ZERO_LENGTH_FILE_PAD,                                  1 );
	init( TRACE_FLUSH_INTERVAL,                               0.25 );
	init( TRACE_RETRY_OPEN_INTERVAL,						  1.00 );
	init( MIN_TRACE_SEVERITY,                 isSimulated ? 0 : 10 ); // Related to the trace severity in Trace.h
	init( MAX_TRACE_SUPPRESSIONS,                              1e4 );
	init( TRACE_SYNC_ENABLED,                                    0 );
	init( TRACE_EVENT_METRIC_UNITS_PER_SAMPLE,                 500 );
	init( TRACE_EVENT_THROTTLER_SAMPLE_EXPIRY,              1800.0 ); // 30 mins
	init( TRACE_EVENT_THROTTLER_MSG_LIMIT,                   20000 );

	//TDMetrics
	init( MAX_METRICS,                                         600 );
	init( MAX_METRIC_SIZE,                                    2500 );
	init( MAX_METRIC_LEVEL,                                     25 );
	init( METRIC_LEVEL_DIVISOR,                             log(4) );
	init( METRIC_LIMIT_START_QUEUE_SIZE,                        10 );  // The queue size at which to start restricting logging by disabling levels
	init( METRIC_LIMIT_RESPONSE_FACTOR,                         10 );  // The additional queue size at which to disable logging of another level (higher == less restrictive)

	//Load Balancing
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
	init( ALTERNATIVES_FAILURE_MAX_DELAY,                      1.0 );
	init( ALTERNATIVES_FAILURE_MIN_DELAY,                     0.05 );
	init( ALTERNATIVES_FAILURE_DELAY_RATIO,                    0.2 );
	init( FUTURE_VERSION_INITIAL_BACKOFF,                      1.0 );
	init( FUTURE_VERSION_MAX_BACKOFF,                          8.0 );
	init( FUTURE_VERSION_BACKOFF_GROWTH,                       2.0 );
}

bool Knobs::setKnob( std::string const& knob, std::string const& value ) {
	if (double_knobs.count(knob)) {
		double v;
		int n=0;
		if (sscanf(value.c_str(), "%lf%n", &v, &n) != 1 || n != value.size())
			throw invalid_option_value();
		*double_knobs[knob] = v;
		return true;
	}
	if (int64_knobs.count(knob) || int_knobs.count(knob)) {
		int64_t v;
		int n=0;
		if (StringRef(value).startsWith(LiteralStringRef("0x"))) {
			if (sscanf(value.c_str(), "0x%llx%n", &v, &n) != 1 || n != value.size())
				throw invalid_option_value();
		} else {
			if (sscanf(value.c_str(), "%lld%n", &v, &n) != 1 || n != value.size())
				throw invalid_option_value();
		}
		if (int64_knobs.count(knob))
			*int64_knobs[knob] = v;
		else {
			if ( v < std::numeric_limits<int>::min() || v > std::numeric_limits<int>::max() )
				throw invalid_option_value();
			*int_knobs[knob] = v;
		}
		return true;
	}
	if (string_knobs.count(knob)) {
		*string_knobs[knob] = value;
		return true;
	}
	return false;
}

static std::string toLower( std::string const& name ) {
	std::string lower_name;
	for(auto c = name.begin(); c != name.end(); ++c)
		if (*c >= 'A' && *c <= 'Z')
			lower_name += *c - 'A' + 'a';
		else
			lower_name += *c;
	return lower_name;
}

void Knobs::initKnob( double& knob, double value, std::string const& name ) {
	knob = value;
	double_knobs[toLower(name)] = &knob;
}

void Knobs::initKnob( int64_t& knob, int64_t value, std::string const& name ) {
	knob = value;
	int64_knobs[toLower(name)] = &knob;
}

void Knobs::initKnob( int& knob, int value, std::string const& name ) {
	knob = value;
	int_knobs[toLower(name)] = &knob;
}

void Knobs::initKnob( std::string& knob, const std::string& value, const std::string& name ) {
	knob = value;
	string_knobs[toLower(name)] = &knob;
}

void Knobs::trace() {
	for(auto &k : double_knobs)
		TraceEvent("Knob").detail("Name", k.first.c_str()).detail("Value", *k.second);
	for(auto &k : int_knobs)
		TraceEvent("Knob").detail("Name", k.first.c_str()).detail("Value", *k.second);
	for(auto &k : int64_knobs)
		TraceEvent("Knob").detail("Name", k.first.c_str()).detail("Value", *k.second);
	for(auto &k : string_knobs)
		TraceEvent("Knob").detail("Name", k.first.c_str()).detail("Value", *k.second);
}
