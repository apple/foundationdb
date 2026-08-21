/*
 * MetricLogger.cpp
 *
 * This source file is part of the FoundationDB open source project
 *
 * Copyright 2013-2026 Apple Inc. and the FoundationDB project authors
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

#include <cmath>
#include <cstddef>
#include <memory>
#include "msgpack.hpp"
#include <msgpack/v3/unpack_decl.hpp>
#include <string>
#include "fdbrpc/Stats.h"
#include "flow/Msgpack.h"
#include "flow/ApiVersion.h"
#include "flow/IRandom.h"
#include "flow/Knobs.h"
#include "flow/OTELMetrics.h"
#include "flow/SystemMonitor.h"
#include "flow/UnitTest.h"
#include "flow/TDMetric.h"
#include "fdbclient/DatabaseContext.h"
#include "fdbclient/ReadYourWrites.h"
#include "fdbclient/KeyBackedTypes.h"
#include "MetricLogger.h"
#include "MetricClient.h"
#include "flow/flow.h"
#include "flow/network.h"
#include "flow/IUDPSocket.h"
#include "flow/IConnection.h"

namespace {
struct MetricsRule {
	explicit(false) MetricsRule(bool enabled = false, int minLevel = 0, StringRef const& name = StringRef())
	  : namePattern(name), enabled(enabled), minLevel(minLevel) {}

	Standalone<StringRef> typePattern;
	Standalone<StringRef> namePattern;
	Standalone<StringRef> addressPattern;
	Standalone<StringRef> idPattern;

	bool enabled;
	int minLevel;

	Tuple pack() const {
		return Tuple::makeTuple(namePattern, typePattern, addressPattern, idPattern, enabled ? 1 : 0, minLevel);
	}

	static inline MetricsRule unpack(Tuple const& t) {
		MetricsRule r;
		int i = 0;
		if (i < t.size())
			r.namePattern = t.getString(i++);
		if (i < t.size())
			r.typePattern = t.getString(i++);
		if (i < t.size())
			r.addressPattern = t.getString(i++);
		if (i < t.size())
			r.idPattern = t.getString(i++);
		if (i < t.size())
			r.enabled = t.getInt(i++) != 0;
		if (i < t.size())
			r.minLevel = (int)t.getInt(i++);
		return r;
	}

	// For now this just returns true if pat is in subject.  Returns true if pat is empty.
	// TODO:  Support more complex patterns?
	static inline bool patternMatch(StringRef const& pat, StringRef const& subject) {
		if (pat.size() == 0)
			return true;
		for (int i = 0, iend = subject.size() - pat.size() + 1; i < iend; ++i)
			if (subject.substr(i, pat.size()) == pat)
				return true;
		return false;
	}

	bool applyTo(BaseMetric* m, StringRef const& address) const {
		if (!patternMatch(addressPattern, address))
			return false;
		if (!patternMatch(namePattern, m->metricName.name))
			return false;
		if (!patternMatch(typePattern, m->metricName.type))
			return false;
		if (!patternMatch(idPattern, m->metricName.id))
			return false;

		m->setConfig(enabled, minLevel);
		return true;
	}
};

struct MetricsConfig {
	explicit MetricsConfig(Key prefix = KeyRef())
	  : space(prefix), ruleMap(space.get("Rules"_sr).key()), addressMap(space.get("Enum"_sr).get("Address"_sr).key()),
	    nameAndTypeMap(space.get("Enum"_sr).get("NameType"_sr).key()),
	    ruleChangeKey(space.get("RulesChanged"_sr).key()), enumsChangeKey(space.get("EnumsChanged"_sr).key()),
	    fieldChangeKey(space.get("FieldsChanged"_sr).key()) {}

	Subspace space;

	using RuleMapT = KeyBackedMap<int64_t, MetricsRule>;
	RuleMapT ruleMap;
	RuleMapT::RangeResultType rules;

	KeyBackedMap<Key, int64_t> addressMap;
	KeyBackedMap<std::pair<Key, Key>, int64_t> nameAndTypeMap;

	Key ruleChangeKey;
	Key enumsChangeKey;
	Key fieldChangeKey;
};

/*
  Rule Updater:

  For now:
    Read and store all rules locally.
    For each metric
      Disable the metric
      For each rule in reverse order, apply the rule to the metric and stop if it returns true
    Wait for rule change, repeat.

  If this gets too slow, yields can be added but at the cost of potentially missing a few data points
  because a metric was disabled and not yet re-enabled before it was logged.

  Or, rules and metrics can be stored for more efficient matching and rule updates can be applied
  differentially.
    Read all rules, store latest version
    Clear all configs for each registered metric
    For each rule in order,
       Efficiently select matching metrics and set config
    Loop
      Wait for rule change
      If rule version skipped, go back to start of rule updater
      Read only new rules.
      For each new rule, in order
        Remove any old rules that new rule completely covers
        Efficiently select matching metrics and set config
      Go back to wait for rule change
*/
Future<Void> metricRuleUpdater(Database cx, MetricsConfig* config, TDMetricCollection* collection) {
	Reference<ReadYourWritesTransaction> tr(new ReadYourWritesTransaction(cx));

	while (true) {
		Future<Void> newMetric = collection->metricAdded.onTrigger();
		Error err;
		try {
			tr->setOption(FDBTransactionOptions::ACCESS_SYSTEM_KEYS);
			MetricsConfig::RuleMapT::RangeResultType rules = co_await config->ruleMap.getRange(tr, 0, {}, 1e6);

			for (auto& it : collection->metricMap) {
				it.value->setConfig(false);
				for (auto i = rules.results.rbegin(); !(i == rules.results.rend()); ++i)
					if (i->second.applyTo(it.value.getPtr(), collection->address))
						break;
			}
			config->rules = std::move(rules);

			Future<Void> rulesChanged = tr->watch(config->ruleChangeKey);
			co_await tr->commit();
			co_await (rulesChanged || newMetric);
			tr->reset();
			continue;
		} catch (Error& e) {
			err = e;
		}
		co_await tr->onError(err);
	}
}

// Implementation of IMetricDB
class MetricDB : public IMetricDB {
public:
	explicit MetricDB(ReadYourWritesTransaction* tr = nullptr) : tr(tr) {}
	~MetricDB() override {}

	// levelKey is the prefix for the entire level, no timestamp at the end
	static Future<Optional<Standalone<StringRef>>> getLastBlock_impl(ReadYourWritesTransaction* tr,
	                                                                 Standalone<StringRef> levelKey) {
		RangeResult results = co_await tr->getRange(normalKeys.withPrefix(levelKey), 1, Snapshot::True, Reverse::True);
		if (results.size() == 1)
			co_return results[0].value;
		co_return Optional<Standalone<StringRef>>();
	}

	Future<Optional<Standalone<StringRef>>> getLastBlock(Standalone<StringRef> key) override {
		return getLastBlock_impl(tr, key);
	}

	ReadYourWritesTransaction* tr;
};

Future<Void> dumpMetrics(Database cx, MetricsConfig* config, TDMetricCollection* collection) {
	MetricBatch batch;
	Standalone<MetricKeyRef> mk;
	ASSERT(collection != nullptr);
	mk.prefix = StringRef(mk.arena(), config->space.key());
	mk.address = StringRef(mk.arena(), collection->address);

	while (true) {
		batch.clear();
		uint64_t rollTime = std::numeric_limits<uint64_t>::max();
		if (collection->rollTimes.size()) {
			rollTime = collection->rollTimes.front();
			collection->rollTimes.pop_front();
		}

		// Are any metrics enabled?
		bool enabled = false;

		// Flush data for each metric, track if any are enabled.
		for (auto& it : collection->metricMap) {
			// If this metric was ever enabled at all then flush it
			if (it.value->pCollection != nullptr) {
				mk.name = it.value->metricName;
				it.value->flushData(mk, rollTime, batch);
			}
			enabled = enabled || it.value->enabled;
		}

		if (rollTime == std::numeric_limits<uint64_t>::max()) {
			collection->currentTimeBytes = 0;
		}

		ReadYourWritesTransaction cbtr(cx);
		MetricDB mdb(&cbtr);

		std::map<int, Future<Void>> results;
		// Call all of the callbacks, map each index to its resulting future
		for (int i = 0, iend = batch.scope.callbacks.size(); i < iend; ++i)
			results[i] = batch.scope.callbacks[i](&mdb, &batch.scope);

		while (true) {
			auto cb = results.begin();
			// Wait for each future, return the ones that succeeded
			Error lastError;
			while (cb != results.end()) {
				try {
					co_await cb->second;
					cb = results.erase(cb);
				} catch (Error& e) {
					++cb;
					lastError = e;
				}
			}

			// If all the callbacks completed then we're done.
			if (results.empty())
				break;

			// Otherwise, wait to retry
			co_await cbtr.onError(lastError);
			for (auto& cb : results)
				cb.second = batch.scope.callbacks[cb.first](&mdb, &batch.scope);
		}

		// If there are more rolltimes then next dump is now, otherwise if no metrics are enabled then it is
		// whenever the next metric is enabled but if there are metrics enabled then it is in 1 second.
		Future<Void> nextDump;
		if (collection->rollTimes.size() > 0)
			nextDump = Void();
		else {
			nextDump = collection->metricEnabled.onTrigger();
			if (enabled)
				nextDump = nextDump || delay(1.0);
		}

		Transaction tr(cx);
		while (true) {
			tr.setOption(FDBTransactionOptions::ACCESS_SYSTEM_KEYS);
			Error err;
			try {
				for (auto& i : batch.scope.inserts) {
					// fprintf(stderr, "%s: dump insert: %s\n", collection->address.toString().c_str(),
					// printable(allInsertions[i].key).c_str());
					tr.set(i.key, i.value());
				}

				for (auto& a : batch.scope.appends) {
					// fprintf(stderr, "%s: dump append: %s\n", collection->address.toString().c_str(),
					// printable(allAppends[i].key).c_str());
					tr.atomicOp(a.key, a.value(), MutationRef::AppendIfFits);
				}

				for (auto& u : batch.scope.updates) {
					// fprintf(stderr, "%s: dump update: %s\n", collection->address.toString().c_str(),
					// printable(allUpdates[i].first).c_str());
					tr.set(u.first, u.second);
				}

				co_await tr.commit();
				break;
			} catch (Error& e) {
				err = e;
			}
			co_await tr.onError(err);
		}
		co_await nextDump;
	}
}

// Push metric field registrations to database.
Future<Void> updateMetricRegistration(Database cx, MetricsConfig* config, TDMetricCollection* collection) {
	Standalone<MetricKeyRef> mk;
	mk.prefix = StringRef(mk.arena(), config->space.key());
	mk.address = StringRef(mk.arena(), collection->address);

	bool addressRegistered = false;

	while (true) {
		Future<Void> registrationChange = collection->metricRegistrationChanged.onTrigger();
		Future<Void> newMetric = collection->metricAdded.onTrigger();
		std::vector<Standalone<StringRef>> keys;
		bool fieldsChanged = false;
		bool enumsChanged = false;

		// Register each metric that isn't already registered
		for (auto& it : collection->metricMap) {
			if (!it.value->registered) {
				// Register metric so it can create its field keys
				mk.name = it.value->metricName;
				it.value->registerFields(mk, keys);

				// Also set keys for the metric's (name,type) pair in the type-and-name map
				keys.push_back(
				    config->nameAndTypeMap.getProperty({ it.value->metricName.name, it.value->metricName.type }).key);

				it.value->registered = true;
				fieldsChanged = true;
				enumsChanged = true;
			}
		}

		// Set a key for this collection's address in the address map if it hasn't been done.
		if (!addressRegistered) {
			keys.push_back(config->addressMap.getProperty(collection->address).key);
			addressRegistered = true;
			enumsChanged = true;
		}

		if (enumsChanged)
			keys.push_back(config->enumsChangeKey);
		if (fieldsChanged)
			keys.push_back(config->fieldChangeKey);

		// Write keys collected to database
		Transaction tr(cx);
		while (true) {
			tr.setOption(FDBTransactionOptions::ACCESS_SYSTEM_KEYS);
			Error err;
			try {
				Value timestamp =
				    BinaryWriter::toValue(CompressedInt<int64_t>(now()), AssumeVersion(g_network->protocolVersion()));
				for (auto& key : keys) {
					// fprintf(stderr, "%s: register: %s\n", collection->address.toString().c_str(),
					// printable(key).c_str());
					tr.set(key, timestamp);
				}

				co_await tr.commit();
				break;
			} catch (Error& e) {
				err = e;
			}
			co_await tr.onError(err);
		}

		// Wait for a metric to require registration or a new metric to be added
		co_await (registrationChange || newMetric);
	}
}
} // namespace

Future<Void> runMetrics(Future<Database> fcx, Key prefix) {
	// Never log to an empty prefix, it's pretty much always a bad idea.
	if (prefix.size() == 0) {
		TraceEvent(SevWarnAlways, "TDMetricsRefusingEmptyPrefix").log();
		co_return;
	}

	// Wait until the collection has been created and initialized.
	TDMetricCollection* metrics = nullptr;
	while (true) {
		metrics = TDMetricCollection::getTDMetrics();
		if (metrics != nullptr)
			if (metrics->init())
				break;
		co_await delay(1.0);
	}

	MetricsConfig config(prefix);

	try {
		Database cx = co_await fcx;
		Future<Void> conf = metricRuleUpdater(cx, &config, metrics);
		Future<Void> dump = dumpMetrics(cx, &config, metrics);
		Future<Void> reg = updateMetricRegistration(cx, &config, metrics);

		co_await (conf || dump || reg);
	} catch (Error& e) {
		if (e.code() != error_code_actor_cancelled) {
			// Disable all metrics
			for (auto& it : metrics->metricMap)
				it.value->setConfig(false);
		}

		TraceEvent(SevWarnAlways, "TDMetricsStopped").error(e);
		throw e;
	}
}

namespace {
Future<Void> startMetricsSimulationServer(MetricsDataModel model) {
	if (model == MetricsDataModel::NONE) {
		co_return;
	}
	uint32_t port = 0;
	switch (model) {
	case MetricsDataModel::STATSD:
		port = FLOW_KNOBS->STATSD_UDP_EMISSION_PORT;
	case MetricsDataModel::OTLP:
		port = FLOW_KNOBS->OTEL_UDP_EMISSION_PORT;
	case MetricsDataModel::NONE:
		port = 0;
	}
	TraceEvent(SevInfo, "MetricsUDPServerStarted").detail("Address", "127.0.0.1").detail("Port", port);
	NetworkAddress localAddress = NetworkAddress::parse("127.0.0.1:" + std::to_string(port));
	Reference<IUDPSocket> serverSocket = co_await INetworkConnections::net()->createUDPSocket(localAddress);
	serverSocket->bind(localAddress);
	Standalone<StringRef> packetString = makeString(IUDPSocket::MAX_PACKET_SIZE);
	uint8_t* packet = mutateString(packetString);

	while (true) {
		int size = co_await serverSocket->receive(packet, packet + IUDPSocket::MAX_PACKET_SIZE);
		auto message = packetString.substr(0, size);

		// Let's just focus on statsd for now. For statsd, the message is expected to be separated by newlines. We need
		// to break each statsd metric and verify them individually.
		if (model == MetricsDataModel::STATSD) {
			std::string statsd_message = message.toString();
			auto metrics = splitString(statsd_message, "\n");
			for (const auto& metric : metrics) {
				ASSERT(verifyStatsdMessage(metric));
			}
		} else if (model == MetricsDataModel::OTLP) {
			msgpack::object_handle result;
			msgpack::unpack(result, reinterpret_cast<const char*>(packet), size);
		}
	}
}
} // namespace

Future<Void> runMetrics() {
	MetricCollection* metrics = nullptr;
	MetricsDataModel model = knobToMetricModel(FLOW_KNOBS->METRICS_DATA_MODEL);
	if (model == MetricsDataModel::NONE) {
		co_return;
	}
	UDPMetricClient metricClient;
	Future<Void> metricsActor;
	if (g_network->isSimulated()) {
		metricsActor = startMetricsSimulationServer(model);
	}
	while (true) {
		metrics = MetricCollection::getMetricCollection();
		if (metrics != nullptr) {

			metricClient.send(metrics);
		}
		co_await delay(FLOW_KNOBS->METRICS_EMISSION_INTERVAL);
	}
}

TEST_CASE("/fdbserver/metrics/TraceEvents") {
	auto getenv2 = [](const char* s) -> const char* {
		s = getenv(s);
		return s ? s : "";
	};
	std::string metricsConnFile = getenv2("METRICS_CONNFILE");
	std::string metricsPrefix = getenv2("METRICS_PREFIX");
	if (metricsConnFile == "") {
		fprintf(stdout, "Metrics cluster file must be specified in environment variable METRICS_CONNFILE\n");
		co_return;
	}
	fprintf(stdout, "Using environment variables METRICS_CONNFILE and METRICS_PREFIX.\n");

	Database metricsDb = Database::createDatabase(metricsConnFile, ApiVersion::LATEST_VERSION);
	TDMetricCollection::getTDMetrics()->address = "0.0.0.0:0"_sr;
	// Keep the metrics task alive while this test emits trace events.
	[[maybe_unused]] Future<Void> metrics = runMetrics(metricsDb, KeyRef(metricsPrefix));
	int64_t x = 0;

	double w = 0.5;
	int chunk = 4000;
	int total = 200000;

	fprintf(stdout, "Writing trace event named Dummy with fields a, b, c, d, j, k, s, x, y, z.\n");
	fprintf(stdout, "  There is a %f second pause every %d events\n", w, chunk);
	fprintf(stdout, "  %d events will be logged.\n", total);
	fprintf(stdout, "  a is always present.  It starts with = 0 and increments by 1 with each event.\n");
	fprintf(stdout, "  b, if present, is always a*2.\n");
	fprintf(stdout, "  c, if present, is always a*3.\n");
	fprintf(stdout, "  b and c are never present in the same event.\n");
	fprintf(stdout, "  x, y, and z, if present, are doubles and equal to 1.5 * a, b, and c, respectively\n");
	fprintf(stdout, "  d is always present, is a string, and rotates through the values 'one', 'two', and ''.\n");
	fprintf(stdout, "  Plotting j on the x axis and k on the y axis should look like x=sin(2t), y=sin(3t)\n");

	Int64MetricHandle intMetric = Int64MetricHandle("DummyInt"_sr);
	BoolMetricHandle boolMetric = BoolMetricHandle("DummyBool"_sr);
	StringMetricHandle stringMetric = StringMetricHandle("DummyString"_sr);

	static const char* dStrings[] = { "one", "two", "" };
	const char** d = dStrings;
	Arena arena;

	while (true) {
		{
			double sstart = x;
			for (int i = 0; i < chunk; ++i, ++x) {
				intMetric = x;
				boolMetric = (x % 2) > 0;
				const char* s = d[x % 3];
				// s doesn't actually require an arena
				stringMetric = Standalone<StringRef>(StringRef((uint8_t*)s, strlen(s)), arena);

				TraceEvent("Dummy")
				    .detail("A", x)
				    .detail("X", 1.5 * x)
				    .detail("D", s)
				    .detail("J", sin(2.0 * x))
				    .detail("K", sin(3.0 * x))
				    .detail("S", sstart + (double)chunk * sin(10.0 * i / chunk));
			}
			co_await delay(w);
		}

		{
			double sstart = x;
			for (int i = 0; i < chunk; ++i, ++x) {
				intMetric = x;
				boolMetric = x % 2 > 0;
				TraceEvent("Dummy")
				    .detail("A", x)
				    .detail("X", 1.5 * x)
				    .detail("B", x * 2)
				    .detail("Y", 3.0 * x)
				    .detail("D", d[x % 3])
				    .detail("J", sin(2.0 * x))
				    .detail("K", sin(3.0 * x))
				    .detail("S", sstart + (double)chunk * sin(40.0 * i / chunk));
			}
			co_await delay(w);
		}

		{
			double sstart = x;
			for (int i = 0; i < chunk; ++i, ++x) {
				intMetric = x;
				boolMetric = x % 2 > 0;
				TraceEvent("Dummy")
				    .detail("A", x)
				    .detail("X", 1.5 * x)
				    .detail("C", x * 3)
				    .detail("Z", 4.5 * x)
				    .detail("D", d[x % 3])
				    .detail("J", sin(2.0 * x))
				    .detail("K", sin(3.0 * x))
				    .detail("S", sstart + (double)chunk * sin(160.0 * i / chunk));
			}
			co_await delay(w);

			if (x >= total)
				co_return;
		}
	}
}
