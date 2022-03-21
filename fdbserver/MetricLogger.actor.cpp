/*
 * MetricLogger.actor.cpp
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

#include <cmath>
#include "flow/UnitTest.h"
#include "flow/TDMetric.actor.h"
#include "fdbclient/DatabaseContext.h"
#include "fdbclient/ReadYourWrites.h"
#include "fdbclient/KeyBackedTypes.h"
#include "fdbserver/MetricLogger.actor.h"
#include "flow/actorcompiler.h" // This must be the last #include.

struct MetricsRule {
	MetricsRule(bool enabled = false, int minLevel = 0, StringRef const& name = StringRef())
	  : namePattern(name), enabled(enabled), minLevel(minLevel) {}

	Standalone<StringRef> typePattern;
	Standalone<StringRef> namePattern;
	Standalone<StringRef> addressPattern;
	Standalone<StringRef> idPattern;

	bool enabled;
	int minLevel;

	Tuple pack() const {
		return Tuple()
		    .append(namePattern)
		    .append(typePattern)
		    .append(addressPattern)
		    .append(idPattern)
		    .append(enabled ? 1 : 0)
		    .append(minLevel);
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
	MetricsConfig(Key prefix = KeyRef())
	  : space(prefix), ruleMap(space.get(LiteralStringRef("Rules")).key()),
	    addressMap(space.get(LiteralStringRef("Enum")).get(LiteralStringRef("Address")).key()),
	    nameAndTypeMap(space.get(LiteralStringRef("Enum")).get(LiteralStringRef("NameType")).key()),
	    ruleChangeKey(space.get(LiteralStringRef("RulesChanged")).key()),
	    enumsChangeKey(space.get(LiteralStringRef("EnumsChanged")).key()),
	    fieldChangeKey(space.get(LiteralStringRef("FieldsChanged")).key()) {}

	Subspace space;

	typedef KeyBackedMap<int64_t, MetricsRule> RuleMapT;
	RuleMapT ruleMap;
	RuleMapT::PairsType rules;

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
ACTOR Future<Void> metricRuleUpdater(Database cx, MetricsConfig* config, TDMetricCollection* collection) {

	state Reference<ReadYourWritesTransaction> tr(new ReadYourWritesTransaction(cx));

	loop {
		state Future<Void> newMetric = collection->metricAdded.onTrigger();
		try {
			tr->setOption(FDBTransactionOptions::ACCESS_SYSTEM_KEYS);
			MetricsConfig::RuleMapT::PairsType rules = wait(config->ruleMap.getRange(tr, 0, {}, 1e6));

			for (auto& it : collection->metricMap) {
				it.value->setConfig(false);
				for (auto i = rules.rbegin(); !(i == rules.rend()); ++i)
					if (i->second.applyTo(it.value.getPtr(), collection->address))
						break;
			}
			config->rules = std::move(rules);

			state Future<Void> rulesChanged = tr->watch(config->ruleChangeKey);
			wait(tr->commit());
			wait(rulesChanged || newMetric);
			tr->reset();

		} catch (Error& e) {
			wait(tr->onError(e));
		}
	}
}

// Implementation of IMetricDB
class MetricDB : public IMetricDB {
public:
	MetricDB(ReadYourWritesTransaction* tr = nullptr) : tr(tr) {}
	~MetricDB() override {}

	// levelKey is the prefix for the entire level, no timestamp at the end
	ACTOR static Future<Optional<Standalone<StringRef>>> getLastBlock_impl(ReadYourWritesTransaction* tr,
	                                                                       Standalone<StringRef> levelKey) {
		RangeResult results = wait(tr->getRange(normalKeys.withPrefix(levelKey), 1, Snapshot::True, Reverse::True));
		if (results.size() == 1)
			return results[0].value;
		return Optional<Standalone<StringRef>>();
	}

	Future<Optional<Standalone<StringRef>>> getLastBlock(Standalone<StringRef> key) override {
		return getLastBlock_impl(tr, key);
	}

	ReadYourWritesTransaction* tr;
};

ACTOR Future<Void> dumpMetrics(Database cx, MetricsConfig* config, TDMetricCollection* collection) {
	state MetricUpdateBatch batch;
	state Standalone<MetricKeyRef> mk;
	ASSERT(collection != nullptr);
	mk.prefix = StringRef(mk.arena(), config->space.key());
	mk.address = StringRef(mk.arena(), collection->address);

	loop {
		batch.clear();
		uint64_t rollTime = std::numeric_limits<uint64_t>::max();
		if (collection->rollTimes.size()) {
			rollTime = collection->rollTimes.front();
			collection->rollTimes.pop_front();
		}

		// Are any metrics enabled?
		state bool enabled = false;

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

		state ReadYourWritesTransaction cbtr(cx);
		state MetricDB mdb(&cbtr);

		state std::map<int, Future<Void>> results;
		// Call all of the callbacks, map each index to its resulting future
		for (int i = 0, iend = batch.callbacks.size(); i < iend; ++i)
			results[i] = batch.callbacks[i](&mdb, &batch);

		loop {
			state std::map<int, Future<Void>>::iterator cb = results.begin();
			// Wait for each future, return the ones that succeeded
			state Error lastError;
			while (cb != results.end()) {
				try {
					wait(cb->second);
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
			wait(cbtr.onError(lastError));
			for (auto& cb : results)
				cb.second = batch.callbacks[cb.first](&mdb, &batch);
		}

		// If there are more rolltimes then next dump is now, otherwise if no metrics are enabled then it is
		// whenever the next metric is enabled but if there are metrics enabled then it is in 1 second.
		state Future<Void> nextDump;
		if (collection->rollTimes.size() > 0)
			nextDump = Void();
		else {
			nextDump = collection->metricEnabled.onTrigger();
			if (enabled)
				nextDump = nextDump || delay(1.0);
		}

		state Transaction tr(cx);
		loop {
			tr.setOption(FDBTransactionOptions::ACCESS_SYSTEM_KEYS);
			try {
				for (auto& i : batch.inserts) {
					// fprintf(stderr, "%s: dump insert: %s\n", collection->address.toString().c_str(),
					// printable(allInsertions[i].key).c_str());
					tr.set(i.key, i.value());
				}

				for (auto& a : batch.appends) {
					// fprintf(stderr, "%s: dump append: %s\n", collection->address.toString().c_str(),
					// printable(allAppends[i].key).c_str());
					tr.atomicOp(a.key, a.value(), MutationRef::AppendIfFits);
				}

				for (auto& u : batch.updates) {
					// fprintf(stderr, "%s: dump update: %s\n", collection->address.toString().c_str(),
					// printable(allUpdates[i].first).c_str());
					tr.set(u.first, u.second);
				}

				wait(tr.commit());
				break;
			} catch (Error& e) {
				wait(tr.onError(e));
			}
		}
		wait(nextDump);
	}
}

// Push metric field registrations to database.
ACTOR Future<Void> updateMetricRegistration(Database cx, MetricsConfig* config, TDMetricCollection* collection) {
	state Standalone<MetricKeyRef> mk;
	mk.prefix = StringRef(mk.arena(), config->space.key());
	mk.address = StringRef(mk.arena(), collection->address);

	state bool addressRegistered = false;

	loop {
		state Future<Void> registrationChange = collection->metricRegistrationChanged.onTrigger();
		state Future<Void> newMetric = collection->metricAdded.onTrigger();
		state std::vector<Standalone<StringRef>> keys;
		state bool fieldsChanged = false;
		state bool enumsChanged = false;

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
		state Transaction tr(cx);
		loop {
			tr.setOption(FDBTransactionOptions::ACCESS_SYSTEM_KEYS);
			try {
				Value timestamp =
				    BinaryWriter::toValue(CompressedInt<int64_t>(now()), AssumeVersion(g_network->protocolVersion()));
				for (auto& key : keys) {
					// fprintf(stderr, "%s: register: %s\n", collection->address.toString().c_str(),
					// printable(key).c_str());
					tr.set(key, timestamp);
				}

				wait(tr.commit());
				break;
			} catch (Error& e) {
				wait(tr.onError(e));
			}
		}

		// Wait for a metric to require registration or a new metric to be added
		wait(registrationChange || newMetric);
	}
}

ACTOR Future<Void> runMetrics(Future<Database> fcx, Key prefix) {
	// Never log to an empty prefix, it's pretty much always a bad idea.
	if (prefix.size() == 0) {
		TraceEvent(SevWarnAlways, "TDMetricsRefusingEmptyPrefix").log();
		return Void();
	}

	// Wait until the collection has been created and initialized.
	state TDMetricCollection* metrics = nullptr;
	loop {
		metrics = TDMetricCollection::getTDMetrics();
		if (metrics != nullptr)
			if (metrics->init())
				break;
		wait(delay(1.0));
	}

	state MetricsConfig config(prefix);

	try {
		Database cx = wait(fcx);
		Future<Void> conf = metricRuleUpdater(cx, &config, metrics);
		Future<Void> dump = dumpMetrics(cx, &config, metrics);
		Future<Void> reg = updateMetricRegistration(cx, &config, metrics);

		wait(conf || dump || reg);
	} catch (Error& e) {
		if (e.code() != error_code_actor_cancelled) {
			// Disable all metrics
			for (auto& it : metrics->metricMap)
				it.value->setConfig(false);
		}

		TraceEvent(SevWarnAlways, "TDMetricsStopped").error(e);
		throw e;
	}
	return Void();
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
		return Void();
	}
	fprintf(stdout, "Using environment variables METRICS_CONNFILE and METRICS_PREFIX.\n");

	state Database metricsDb = Database::createDatabase(metricsConnFile, Database::API_VERSION_LATEST);
	TDMetricCollection::getTDMetrics()->address = LiteralStringRef("0.0.0.0:0");
	state Future<Void> metrics = runMetrics(metricsDb, KeyRef(metricsPrefix));
	state int64_t x = 0;

	state double w = 0.5;
	state int chunk = 4000;
	state int total = 200000;

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

	state Int64MetricHandle intMetric = Int64MetricHandle(LiteralStringRef("DummyInt"));
	state BoolMetricHandle boolMetric = BoolMetricHandle(LiteralStringRef("DummyBool"));
	state StringMetricHandle stringMetric = StringMetricHandle(LiteralStringRef("DummyString"));

	static const char* dStrings[] = { "one", "two", "" };
	state const char** d = dStrings;
	state Arena arena;

	loop {
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
			wait(delay(w));
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
			wait(delay(w));
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
			wait(delay(w));

			if (x >= total)
				return Void();
		}
	}
}
