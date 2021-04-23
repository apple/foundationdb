/*
 * ActorLineageProfiler.h
 *
 * This source file is part of the FoundationDB open source project
 *
 * Copyright 2013-2021 Apple Inc. and the FoundationDB project authors
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

#pragma once

#include "fdbclient/AnnotateActor.h"

#include <optional>
#include <string>
#include <any>
#include <vector>
#include <mutex>
#include <condition_variable>
#include "flow/singleton.h"
#include "flow/flow.h"

void samplingProfilerUpdateFrequency(std::optional<std::any> freq);
void samplingProfilerUpdateWindow(std::optional<std::any> window);

struct IALPCollectorBase {
	virtual std::optional<std::any> collect(ActorLineage*) = 0;
	virtual const std::string_view& name() = 0;
	IALPCollectorBase();
};

template <class T>
struct IALPCollector : IALPCollectorBase {
	const std::string_view& name() override { return T::name; }
};

struct Sample : std::enable_shared_from_this<Sample> {
	double time = 0.0;
	unsigned size = 0u;
	char* data = nullptr;
	~Sample() { ::free(data); }
};

class SampleCollectorT {
public: // Types
	friend struct crossbow::create_static<SampleCollectorT>;
	using Getter = std::function<std::vector<Reference<ActorLineage>>()>;

private:
	std::vector<IALPCollectorBase*> collectors;
	std::map<WaitState, Getter> getSamples;
	SampleCollectorT() {}
	std::map<std::string_view, std::any> collect(ActorLineage* lineage);

public:
	void addCollector(IALPCollectorBase* collector) { collectors.push_back(collector); }
	std::shared_ptr<Sample> collect();
	void addGetter(WaitState waitState, Getter const& getter) { getSamples[waitState] = getter; };
};

using SampleCollector = crossbow::singleton<SampleCollectorT>;

class SampleCollection_t {
	friend struct crossbow::create_static<SampleCollection_t>;
	using Lock = std::unique_lock<std::mutex>;
	SampleCollection_t() {}

	SampleCollector _collector;
	mutable std::mutex mutex;
	std::atomic<double> windowSize = 5.0;
	std::deque<std::shared_ptr<Sample>> data;

public:
	/**
	 * Define how many samples the collection shoul keep. The window size is defined by time dimension.
	 *
	 * \param duration How long a sample should be kept in the collection.
	 */
	void setWindowSize(double duration) { windowSize.store(duration); }
	/**
	 * By default returns reference counted pointers of all samples. A window can be defined in terms of absolute time.
	 *
	 * \param from The minimal age of all returned samples.
	 * \param to The max age of all returned samples.
	 */
	std::vector<std::shared_ptr<Sample>> get(double from = 0.0, double to = std::numeric_limits<double>::max()) const;
	/**
	 * Collects all new samples from the sample collector and stores them in the collection.
	 */
	void refresh();
	const SampleCollector& collector() const { return _collector; }
	SampleCollector& collector() { return _collector; }
};

using SampleCollection = crossbow::singleton<SampleCollection_t>;

class ActorLineageProfilerT {
	friend struct crossbow::create_static<ActorLineageProfilerT>;
	ActorLineageProfilerT();
	SampleCollection collection;
	std::thread profilerThread;
	std::atomic<unsigned> frequency = 0;
	std::mutex mutex;
	std::condition_variable cond;
	void profile();

public:
	~ActorLineageProfilerT();
	void setFrequency(unsigned frequency);
	void stop();
};

using ActorLineageProfiler = crossbow::singleton<ActorLineageProfilerT>;
