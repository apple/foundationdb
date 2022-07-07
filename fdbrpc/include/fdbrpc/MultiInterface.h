/*
 * MultiInterface.h
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

#ifndef FLOW_MULTIINTERFACE_H
#define FLOW_MULTIINTERFACE_H
#pragma once

#include "flow/FastRef.h"
#include "fdbrpc/Locality.h"

#include <vector>

extern uint64_t debug_lastLoadBalanceResultEndpointToken;

template <class K, class V>
struct KVPair {
	// KVPair<K,V> is ordered only by K and described by V
	K k;
	V v;
	KVPair() {}
	KVPair(K const& k, V const& v) : k(k), v(v) {}
	KVPair(K&& k, V&& v) : k(std::move(k)), v(std::move(v)) {}
};
template <class K, class V>
bool operator<(KVPair<K, V> const& l, KVPair<K, V> const& r) {
	return l.k < r.k;
}
template <class K, class V>
bool operator<(KVPair<K, V> const& l, K const& r) {
	return l.k < r;
}
template <class K, class V>
bool operator<(K const& l, KVPair<K, V> const& r) {
	return l < r.k;
}

template <class K, class V>
std::string describe(KVPair<K, V> const& p) {
	return format("%d ", p.k) + describe(p.v);
}

template <class T>
struct ReferencedInterface : public ReferenceCounted<ReferencedInterface<T>> {
	T interf;
	int8_t distance; // one of enum values in struct LBDistance
	std::string toString() const { return interf.toString(); }
	ReferencedInterface(T const& interf, LocalityData const& locality = LocalityData()) : interf(interf) {
		distance = LBLocalityData<T>::Present ? loadBalanceDistance(locality,
		                                                            LBLocalityData<T>::getLocality(interf),
		                                                            LBLocalityData<T>::getAddress(interf))
		                                      : LBDistance::DISTANT;
	}
	virtual ~ReferencedInterface() {}

	static bool sort_by_distance(Reference<ReferencedInterface<T>> r1, Reference<ReferencedInterface<T>> r2) {
		return r1->distance < r2->distance;
	}
};

template <class T>
struct AlternativeInfo {
	T interf;
	double probability;
	double cumulativeProbability;
	int processBusyTime;
	double lastUpdate;

	AlternativeInfo(T const& interf, double probability, double cumulativeProbability)
	  : interf(interf), probability(probability), cumulativeProbability(cumulativeProbability), processBusyTime(-1),
	    lastUpdate(0) {}

	bool operator<(double const& r) const { return cumulativeProbability < r; }
	bool operator<=(double const& r) const { return cumulativeProbability <= r; }
	bool operator==(double const& r) const { return cumulativeProbability == r; }
};

FDB_DECLARE_BOOLEAN_PARAM(BalanceOnRequests);

template <class T>
class ModelInterface : public ReferenceCounted<ModelInterface<T>> {
public:
	// If balanceOnRequests is true, the client will load balance based on the number of GRVs released by each proxy
	// If balanceOnRequests is false, the client will load balance based on the CPU usage of each proxy
	// Only requests which take from the GRV budget on the proxy should set balanceOnRequests to true
	explicit ModelInterface(const std::vector<T>& v, BalanceOnRequests balanceOnRequests = BalanceOnRequests::False)
	  : balanceOnRequests(balanceOnRequests) {
		for (int i = 0; i < v.size(); i++) {
			alternatives.push_back(AlternativeInfo(v[i], 1.0 / v.size(), (i + 1.0) / v.size()));
		}
		if (v.size()) {
			updater = recurring([this]() { updateProbabilities(); }, FLOW_KNOBS->BASIC_LOAD_BALANCE_UPDATE_RATE);
		}
	}

	int size() const { return alternatives.size(); }

	bool alwaysFresh() const { return LBLocalityData<T>::alwaysFresh(); }

	int getBest() const {
		return std::lower_bound(alternatives.begin(), alternatives.end(), deterministicRandom()->random01()) -
		       alternatives.begin();
	}

	void updateRecent(int index, int processBusyTime) {
		alternatives[index].processBusyTime = processBusyTime;
		alternatives[index].lastUpdate = now();
	}

	void updateProbabilities() {
		double totalBusy = 0;
		for (auto& it : alternatives) {
			int busyMetric = balanceOnRequests ? it.processBusyTime / FLOW_KNOBS->BASIC_LOAD_BALANCE_COMPUTE_PRECISION
			                                   : it.processBusyTime % FLOW_KNOBS->BASIC_LOAD_BALANCE_COMPUTE_PRECISION;
			totalBusy += busyMetric;
			if (now() - it.lastUpdate > FLOW_KNOBS->BASIC_LOAD_BALANCE_UPDATE_RATE / 2.0) {
				return;
			}
		}

		if ((balanceOnRequests && totalBusy < FLOW_KNOBS->BASIC_LOAD_BALANCE_MIN_REQUESTS * alternatives.size()) ||
		    (!balanceOnRequests && totalBusy < FLOW_KNOBS->BASIC_LOAD_BALANCE_COMPUTE_PRECISION *
		                                           FLOW_KNOBS->BASIC_LOAD_BALANCE_MIN_CPU * alternatives.size())) {
			return;
		}

		double totalProbability = 0;
		for (auto& it : alternatives) {
			int busyMetric = balanceOnRequests ? it.processBusyTime / FLOW_KNOBS->BASIC_LOAD_BALANCE_COMPUTE_PRECISION
			                                   : it.processBusyTime % FLOW_KNOBS->BASIC_LOAD_BALANCE_COMPUTE_PRECISION;
			it.probability +=
			    (1.0 / alternatives.size() - (busyMetric / totalBusy)) * FLOW_KNOBS->BASIC_LOAD_BALANCE_MAX_CHANGE;
			it.probability =
			    std::max(it.probability, 1 / (FLOW_KNOBS->BASIC_LOAD_BALANCE_MAX_PROB * alternatives.size()));
			it.probability = std::min(it.probability, FLOW_KNOBS->BASIC_LOAD_BALANCE_MAX_PROB / alternatives.size());
			totalProbability += it.probability;
		}

		for (auto& it : alternatives) {
			it.probability = it.probability / totalProbability;
		}

		totalProbability = 0;
		for (auto& it : alternatives) {
			totalProbability += it.probability;
			it.cumulativeProbability = totalProbability;
		}
		alternatives.back().cumulativeProbability = 1.0;
	}

	template <class F>
	F const& get(int index, F T::*member) const {
		return alternatives[index].interf.*member;
	}

	T const& getInterface(int index) { return alternatives[index].interf; }
	UID getId(int index) const { return alternatives[index].interf.id(); }

	virtual ~ModelInterface() {}

	std::string description() { return describe(alternatives); }

private:
	std::vector<AlternativeInfo<T>> alternatives;
	Future<Void> updater;
	bool balanceOnRequests;
};

template <class T>
class MultiInterface : public ReferenceCounted<MultiInterface<T>> {
	MultiInterface(const std::vector<T>& v, LocalityData const& locality = LocalityData()) {
		// This version of MultInterface is no longer used, but was kept around because of templating
		ASSERT(false);
	}

	virtual ~MultiInterface() {}
};

template <class T>
class MultiInterface<ReferencedInterface<T>> : public ReferenceCounted<MultiInterface<ReferencedInterface<T>>> {
public:
	MultiInterface(const std::vector<Reference<ReferencedInterface<T>>>& v) : alternatives(v), bestCount(0) {
		deterministicRandom()->randomShuffle(alternatives);
		if (LBLocalityData<T>::Present) {
			std::stable_sort(alternatives.begin(), alternatives.end(), ReferencedInterface<T>::sort_by_distance);
		}
		if (size()) {
			for (int i = 1; i < alternatives.size(); i++) {
				if (alternatives[i]->distance > alternatives[0]->distance) {
					bestCount = i;
					return;
				}
			}
			bestCount = size();
		}
	}

	int size() const { return alternatives.size(); }
	int countBest() const { return bestCount; }
	LBDistance::Type bestDistance() const {
		if (!size())
			return LBDistance::DISTANT;
		return (LBDistance::Type)alternatives[0]->distance;
	}
	bool alwaysFresh() const { return LBLocalityData<T>::alwaysFresh(); }

	template <class F>
	F const& get(int index, F T::*member) const {
		return alternatives[index]->interf.*member;
	}

	T const& getInterface(int index) { return alternatives[index]->interf; }
	UID getId(int index) const { return alternatives[index]->interf.id(); }
	bool hasInterface(UID id) const {
		for (const auto& ref : alternatives) {
			if (ref->interf.id() == id) {
				return true;
			}
		}
		return false;
	}

	Reference<ReferencedInterface<T>>& operator[](int i) { return alternatives[i]; }

	const Reference<ReferencedInterface<T>>& operator[](int i) const { return alternatives[i]; }

	virtual ~MultiInterface() {}

	std::string description() { return describe(alternatives); }

private:
	std::vector<Reference<ReferencedInterface<T>>> alternatives;
	int16_t bestCount; // The number of interfaces in the same location as alternatives[0]. The same location means
	                   // DC by default and machine if more than one alternatives are on the same machine).
};

template <class Ar, class T>
void load(Ar& ar, Reference<MultiInterface<T>>&) {
	ASSERT(false);
} //< required for Future<T>
template <class Ar, class T>
void load(Ar& ar, Reference<ModelInterface<T>>&) {
	ASSERT(false);
} //< required for Future<T>

#endif
