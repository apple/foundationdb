/*
 * ActorLineageProfiler.h
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
	Sample() {}
	Sample(Sample const&) = delete;
	Sample& operator=(Sample const&) = delete;
	std::unordered_map<WaitState, std::pair<char*, unsigned>> data;
	~Sample() {
		std::for_each(data.begin(), data.end(), [](std::pair<WaitState, std::pair<char*, unsigned>> entry) {
			::free(entry.second.first);
		});
	}
};

class SampleIngestor : std::enable_shared_from_this<SampleIngestor> {
public:
	virtual ~SampleIngestor();
	virtual void ingest(std::shared_ptr<Sample> const& sample) = 0;
	virtual void getConfig(std::map<std::string, std::string>&) const = 0;
};

class NoneIngestor : public SampleIngestor {
public:
	void ingest(std::shared_ptr<Sample> const& sample) override {}
	void getConfig(std::map<std::string, std::string>& res) const override { res["ingestor"] = "none"; }
};

// The FluentD ingestor uses the pimpl idiom. This is to make compilation less heavy weight as this implementation has
// dependencies to boost::asio
struct FluentDIngestorImpl;

class FluentDIngestor : public SampleIngestor {
public: // Public Types
	enum class Protocol { TCP, UDP };

private: // members
	FluentDIngestorImpl* impl;

public: // interface
	void ingest(std::shared_ptr<Sample> const& sample) override;
	FluentDIngestor(Protocol protocol, NetworkAddress& endpoint);
	void getConfig(std::map<std::string, std::string>& res) const override;
	~FluentDIngestor();
};

struct ConfigError {
	std::string description;
};

class ProfilerConfigT {
private: // private types
	using Lock = std::unique_lock<std::mutex>;
	friend struct crossbow::create_static<ProfilerConfigT>;

private: // members
	std::shared_ptr<SampleIngestor> ingestor = std::make_shared<NoneIngestor>();

private: // construction
	ProfilerConfigT() {}
	ProfilerConfigT(ProfilerConfigT const&) = delete;
	ProfilerConfigT& operator=(ProfilerConfigT const&) = delete;
	void setBackend(std::shared_ptr<SampleIngestor> ingestor) { this->ingestor = ingestor; }

public:
	void ingest(std::shared_ptr<Sample> sample) { ingestor->ingest(sample); }
	void reset(std::map<std::string, std::string> const& config);
	std::map<std::string, std::string> getConfig() const;
};

using ProfilerConfig = crossbow::singleton<ProfilerConfigT>;

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
	std::atomic<double> windowSize = 0.0;
	std::deque<std::shared_ptr<Sample>> data;
	ProfilerConfig config;
	Reference<ActorLineage> _currentLineage;

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
	void collect(const Reference<ActorLineage>& lineage);
	const SampleCollector& collector() const { return _collector; }
	SampleCollector& collector() { return _collector; }
	Reference<ActorLineage> getLineage() { return _currentLineage; }
};

using SampleCollection = crossbow::singleton<SampleCollection_t>;

struct ProfilerImpl;

namespace boost {
namespace asio {
// forward declare io_context because including boost asio is super expensive
class io_context;
} // namespace asio
} // namespace boost

class ActorLineageProfilerT {
	friend struct crossbow::create_static<ActorLineageProfilerT>;
	ProfilerImpl* impl;
	SampleCollection collection;
	ActorLineageProfilerT();

public:
	~ActorLineageProfilerT();
	void setFrequency(unsigned frequency);
	boost::asio::io_context& context();
};

using ActorLineageProfiler = crossbow::singleton<ActorLineageProfilerT>;
