/*
 * ActorLineageProfiler.cpp
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
#include "flow/singleton.h"
#include "fdbrpc/IAsyncFile.h"
#include "fdbclient/ActorLineageProfiler.h"
#include "fdbclient/NameLineage.h"
#include <msgpack.hpp>
#include <memory>
#include <typeindex>
#include <boost/endian/conversion.hpp>
#include <boost/asio.hpp>

using namespace std::literals;

class Packer : public msgpack::packer<msgpack::sbuffer> {
	struct visitor_t {
		using VisitorMap = std::unordered_map<std::type_index, std::function<void(std::any const&, Packer& packer)>>;
		VisitorMap visitorMap;

		template <class T>
		static void any_visitor(std::any const& val, Packer& packer) {
			const T& v = std::any_cast<const T&>(val);
			packer.pack(v);
		}

		template <class... Args>
		struct populate_visitor_map;
		template <class Head, class... Tail>
		struct populate_visitor_map<Head, Tail...> {
			static void populate(VisitorMap& map) {
				map.emplace(std::type_index(typeid(Head)), any_visitor<Head>);
				populate_visitor_map<Tail...>::populate(map);
			}
		};
		template <class Head>
		struct populate_visitor_map<Head> {
			static void populate(VisitorMap&) {}
		};

		visitor_t() {
			populate_visitor_map<int64_t,
			                     uint64_t,
			                     bool,
			                     float,
			                     double,
			                     std::string,
			                     std::string_view,
			                     std::vector<std::any>,
			                     std::vector<std::string>,
			                     std::vector<std::string_view>,
			                     std::map<std::string, std::any>,
			                     std::map<std::string_view, std::any>,
			                     std::vector<std::map<std::string_view, std::any>>>::populate(visitorMap);
		}

		void visit(const std::any& val, Packer& packer) {
			auto iter = visitorMap.find(val.type());
			if (iter == visitorMap.end()) {
				TraceEvent(SevError, "PackerTypeNotFound").detail("Type", val.type().name());
			} else {
				iter->second(val, packer);
			}
		}
	};
	msgpack::sbuffer sbuffer;
	// Initializing visitor_t involves building a type-map. As this is a relatively expensive operation, we don't want
	// to do this each time we create a Packer object. So visitor_t is a stateless class and we only use it as a
	// visitor.
	crossbow::singleton<visitor_t> visitor;

public:
	Packer() : msgpack::packer<msgpack::sbuffer>(sbuffer) {}

	void pack(std::any const& val) { visitor->visit(val, *this); }

	void pack(bool val) {
		if (val) {
			pack_true();
		} else {
			pack_false();
		}
	}

	void pack(uint64_t val) {
		if (val <= std::numeric_limits<uint8_t>::max()) {
			pack_uint8(uint8_t(val));
		} else if (val <= std::numeric_limits<uint16_t>::max()) {
			pack_uint16(uint16_t(val));
		} else if (val <= std::numeric_limits<uint32_t>::max()) {
			pack_uint32(uint32_t(val));
		} else {
			pack_uint64(val);
		}
	}

	void pack(int64_t val) {
		if (val >= 0) {
			this->pack(uint64_t(val));
		} else if (val >= std::numeric_limits<uint8_t>::min()) {
			pack_int8(int8_t(val));
		} else if (val >= std::numeric_limits<int16_t>::min()) {
			pack_int16(int16_t(val));
		} else if (val >= std::numeric_limits<int32_t>::min()) {
			pack_int32(int32_t(val));
		} else if (val >= std::numeric_limits<int64_t>::min()) {
			pack_int64(int64_t(val));
		}
	}

	void pack(float val) { pack_float(val); }
	void pack(double val) { pack_double(val); }
	void pack(std::string const& str) {
		pack_str(str.size());
		pack_str_body(str.data(), str.size());
	}

	void pack(std::string_view val) {
		pack_str(val.size());
		pack_str_body(val.data(), val.size());
	}

	template <class K, class V>
	void pack(std::map<K, V> const& map) {
		pack_map(map.size());
		for (const auto& p : map) {
			pack(p.first);
			pack(p.second);
		}
	}

	template <class T>
	void pack(std::vector<T> const& val) {
		pack_array(val.size());
		for (const auto& v : val) {
			pack(v);
		}
	}

	std::pair<char*, unsigned> getbuf() {
		unsigned size = sbuffer.size();
		return std::make_pair(sbuffer.release(), size);
	}
};

IALPCollectorBase::IALPCollectorBase() {
	SampleCollector::instance().addCollector(this);
}

std::map<std::string_view, std::any> SampleCollectorT::collect(ActorLineage* lineage) {
	ASSERT(lineage != nullptr);
	std::map<std::string_view, std::any> out;
	for (auto& collector : collectors) {
		auto val = collector->collect(lineage);
		if (val.has_value()) {
			out[collector->name()] = val.value();
		}
	}
	return out;
}

std::shared_ptr<Sample> SampleCollectorT::collect() {
	auto sample = std::make_shared<Sample>();
	double time = g_network->now();
	sample->time = time;
	for (auto& p : getSamples) {
		Packer packer;
		std::vector<std::map<std::string_view, std::any>> samples;
		auto sampleVec = p.second();
		for (auto& val : sampleVec) {
			auto m = collect(val.getPtr());
			if (!m.empty()) {
				samples.emplace_back(std::move(m));
			}
		}
		if (!samples.empty()) {
			packer.pack(samples);
			sample->data[p.first] = packer.getbuf();
		}
	}
	return sample;
}

void SampleCollection_t::collect(const Reference<ActorLineage>& lineage) {
	ASSERT(lineage.isValid());
	_currentLineage = lineage;
	auto sample = _collector->collect();
	ASSERT(sample);
	{
		Lock _{ mutex };
		data.emplace_back(sample);
	}
	auto min = std::min(data.back()->time - windowSize, data.back()->time);
	double oldest = data.front()->time;
	// we don't need to check for data.empty() in this loop (or the inner loop) as we know that we will end
	// up with at least one entry which is the most recent sample
	while (oldest < min) {
		Lock _{ mutex };
		// we remove at most 10 elements at a time. This is so we don't block the main thread for too long.
		for (int i = 0; i < 10 && oldest < min; ++i) {
			data.pop_front();
			oldest = data.front()->time;
		}
	}
	// TODO: Should only call ingest when deleting from memory
	config->ingest(sample);
}

std::vector<std::shared_ptr<Sample>> SampleCollection_t::get(double from /*= 0.0*/,
                                                             double to /*= std::numeric_limits<double>::max()*/) const {
	Lock _{ mutex };
	std::vector<std::shared_ptr<Sample>> res;
	for (const auto& sample : data) {
		if (sample->time > to) {
			break;
		} else if (sample->time >= from) {
			res.push_back(sample);
		}
	}
	return res;
}

void sample(LineageReference* lineagePtr) {
	if (!lineagePtr->isValid()) {
		return;
	}
	if (!lineagePtr->isAllocated()) {
		lineagePtr->allocate();
	}
	(*lineagePtr)->modify(&NameLineage::actorName) = lineagePtr->actorName();
	boost::asio::post(ActorLineageProfiler::instance().context(),
	                  [lineage = LineageReference::addRef(lineagePtr->getPtr())]() {
		                  SampleCollection::instance().collect(lineage);
	                  });
}

struct ProfilerImpl {
	boost::asio::io_context context;
	boost::asio::executor_work_guard<decltype(context.get_executor())> workGuard;
	boost::asio::steady_timer timer;
	std::thread mainThread;
	unsigned frequency;

	SampleCollection collection;

	ProfilerImpl() : workGuard(context.get_executor()), timer(context) {
		mainThread = std::thread([this]() { context.run(); });
	}
	~ProfilerImpl() {
		setFrequency(0);
		workGuard.reset();
		mainThread.join();
	}

	void profileHandler(boost::system::error_code const& ec) {
		if (ec) {
			return;
		}
		startSampling = true;
		timer = boost::asio::steady_timer(context, std::chrono::microseconds(1000000 / frequency));
		timer.async_wait([this](auto const& ec) { profileHandler(ec); });
	}

	void setFrequency(unsigned frequency) {
		boost::asio::post(context, [this, frequency]() {
			this->frequency = frequency;
			timer.cancel();
			if (frequency > 0) {
				profileHandler(boost::system::error_code{});
			}
		});
	}
};

ActorLineageProfilerT::ActorLineageProfilerT() : impl(new ProfilerImpl()) {
	// collection->collector()->addGetter(WaitState::Network,
	//                                    std::bind(&ActorLineageSet::copy, std::ref(g_network->getActorLineageSet())));
	// collection->collector()->addGetter(
	//     WaitState::Disk,
	//     std::bind(&ActorLineageSet::copy, std::ref(IAsyncFileSystem::filesystem()->getActorLineageSet())));
	collection->collector()->addGetter(WaitState::Running, []() {
		return std::vector<Reference<ActorLineage>>({ SampleCollection::instance().getLineage() });
	});
}

ActorLineageProfilerT::~ActorLineageProfilerT() {
	delete impl;
}

void ActorLineageProfilerT::setFrequency(unsigned frequency) {
	impl->setFrequency(frequency);
}

boost::asio::io_context& ActorLineageProfilerT::context() {
	return impl->context;
}

SampleIngestor::~SampleIngestor() {}

void ProfilerConfigT::reset(std::map<std::string, std::string> const& config) {
	bool expectNoMore = false, useFluentD = false, useTCP = false;
	std::string endpoint;
	ConfigError err;
	for (auto& kv : config) {
		if (expectNoMore) {
			err.description = format("Unexpected option %s", kv.first.c_str());
			throw err;
		}
		if (kv.first == "ingestor") {
			std::string val = kv.second;
			std::for_each(val.begin(), val.end(), [](auto c) { return std::tolower(c); });
			if (val == "none") {
				setBackend(std::make_shared<NoneIngestor>());
			} else if (val == "fluentd") {
				useFluentD = true;
			} else {
				err.description = format("Unsupported ingestor: %s", val.c_str());
				throw err;
			}
		} else if (kv.first == "ingestor_endpoint") {
			endpoint = kv.second;
		} else if (kv.first == "ingestor_protocol") {
			auto val = kv.second;
			std::for_each(val.begin(), val.end(), [](auto c) { return std::tolower(c); });
			if (val == "tcp") {
				useTCP = true;
			} else if (val == "udp") {
				useTCP = false;
			} else {
				err.description = format("Unsupported protocol for fluentd: %s", kv.second.c_str());
				throw err;
			}
		} else {
			err.description = format("Unknown option %s", kv.first.c_str());
			throw err;
		}
	}
	if (useFluentD) {
		if (endpoint.empty()) {
			err.description = "Endpoint is required for fluentd ingestor";
			throw err;
		}
		NetworkAddress address;
		try {
			address = NetworkAddress::parse(endpoint);
		} catch (Error& e) {
			err.description = format("Can't parse address %s", endpoint.c_str());
			throw err;
		}
		setBackend(std::make_shared<FluentDIngestor>(
		    useTCP ? FluentDIngestor::Protocol::TCP : FluentDIngestor::Protocol::UDP, address));
	}
}

std::map<std::string, std::string> ProfilerConfigT::getConfig() const {
	std::map<std::string, std::string> res;
	if (ingestor) {
		ingestor->getConfig(res);
	}
	return res;
}

// Callback used to update the sampling profilers run frequency whenever the
// frequency changes.
void samplingProfilerUpdateFrequency(std::optional<std::any> freq) {
	double frequency = 0;
	if (freq.has_value()) {
		frequency = std::any_cast<double>(freq.value());
	}
	TraceEvent(SevInfo, "SamplingProfilerUpdateFrequency").detail("Frequency", frequency);
	ActorLineageProfiler::instance().setFrequency(frequency);
}

// Callback used to update the sample collector window size.
void samplingProfilerUpdateWindow(std::optional<std::any> window) {
	double duration = 0;
	if (window.has_value()) {
		duration = std::any_cast<double>(window.value());
	}
	TraceEvent(SevInfo, "SamplingProfilerUpdateWindow").detail("Duration", duration);
	SampleCollection::instance().setWindowSize(duration);
}
