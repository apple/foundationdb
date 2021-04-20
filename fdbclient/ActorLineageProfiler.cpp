/*
 * ActorLineageProfiler.cpp
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

#include "flow/flow.h"
#include "flow/singleton.h"
#include "fdbrpc/IAsyncFile.h"
#include "fdbclient/ActorLineageProfiler.h"
#include <msgpack.hpp>
#include <memory>
#include <boost/endian/conversion.hpp>

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
		template <>
		struct populate_visitor_map<> {
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
			                     std::map<std::string, std::any>,
			                     std::map<std::string_view, std::any>>::populate(visitorMap);
		}

		void visit(const std::any& val, Packer& packer) {
			auto iter = visitorMap.find(val.type());
			if (iter == visitorMap.end()) {
				// TODO: trace error
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

	std::shared_ptr<Sample> done(double time) {
		auto res = std::make_shared<Sample>();
		res->time = time;
		res->size = sbuffer.size();
		res->data = sbuffer.release();
		return res;
	}
};

IALPCollectorBase::IALPCollectorBase() {
	SampleCollector::instance().addCollector(this);
}

std::map<std::string_view, std::any> SampleCollectorT::collect(ActorLineage* lineage) {
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
	Packer packer;
	std::map<std::string_view, std::any> res;
	double time = g_network->now();
	res["time"sv] = time;
	for (auto& p : getSamples) {
		std::vector<std::map<std::string_view, std::any>> samples;
		auto sampleVec = p.second();
		for (auto& val : sampleVec) {
			auto m = collect(val.getPtr());
			if (!m.empty()) {
				samples.emplace_back(std::move(m));
			}
		}
		if (!samples.empty()) {
			res[to_string(p.first)] = samples;
		}
	}
	packer.pack(res);
	return packer.done(time);
}

void SampleCollection_t::refresh() {
	auto sample = _collector->collect();
	auto min = std::max(sample->time - windowSize, sample->time);
	{
		Lock _{ mutex };
		data.emplace_back(std::move(sample));
	}
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

ActorLineageProfilerT::ActorLineageProfilerT() {
	collection->collector()->addGetter(WaitState::Network,
	                                   std::bind(&ActorLineageSet::copy, std::ref(g_network->getActorLineageSet())));
	collection->collector()->addGetter(
	    WaitState::Disk,
	    std::bind(&ActorLineageSet::copy, std::ref(IAsyncFileSystem::filesystem()->getActorLineageSet())));
	collection->collector()->addGetter(WaitState::Running, []() {
		auto res = currentLineageThreadSafe.get();
		return std::vector<Reference<ActorLineage>>({ currentLineageThreadSafe.get() });
	});
}

ActorLineageProfilerT::~ActorLineageProfilerT() {
	stop();
}

void ActorLineageProfilerT::stop() {
	setFrequency(0);
}

void ActorLineageProfilerT::setFrequency(unsigned frequency) {
	bool change = this->frequency != frequency;
	this->frequency = frequency;
	if (frequency != 0 && !profilerThread.joinable()) {
		profilerThread = std::thread(std::bind(&ActorLineageProfilerT::profile, this));
	} else if (change) {
		cond.notify_all();
	}

	if (frequency == 0) {
		profilerThread.join();
	}
}

void ActorLineageProfilerT::profile() {
	for (;;) {
		collection->refresh();
		if (frequency == 0) {
			return;
		}
		{
			std::unique_lock<std::mutex> lock{ mutex };
			cond.wait_for(lock, std::chrono::microseconds(1000000 / frequency));
			// cond.wait_until(lock, lastSample + std::chrono::milliseconds)
		}
		if (frequency == 0) {
			return;
		}
	}
}
