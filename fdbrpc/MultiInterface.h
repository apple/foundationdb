/*
 * MultiInterface.h
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

#ifndef FLOW_MULTIINTERFACE_H
#define FLOW_MULTIINTERFACE_H
#pragma once

extern uint64_t debug_lastLoadBalanceResultEndpointToken;

template <class K, class V>
struct KVPair {
	// KVPair<K,V> is ordered only by K and described by V
	K k;
	V v;
	KVPair() {}
	KVPair( K const& k, V const& v ) : k(k), v(v) {}
	KVPair(K && k, V && v) : k(std::move(k)), v(std::move(v)) {}
};
template <class K, class V> bool operator < ( KVPair<K,V> const& l, KVPair<K,V> const& r ) { return l.k < r.k; }
template <class K, class V> bool operator < ( KVPair<K,V> const& l, K const& r ) { return l.k < r; }
template <class K, class V> bool operator < ( K const& l, KVPair<K,V> const& r ) { return l < r.k; }

template <class K, class V>
std::string describe( KVPair<K,V> const& p ) { return format("%d ", p.k) + describe(p.v); }

template <class T>
struct ReferencedInterface : public ReferenceCounted<ReferencedInterface<T>> {
	T interf;
	int8_t distance;
	std::string toString() const {
		return interf.toString();
	}
	ReferencedInterface(T const& interf, LocalityData const& locality = LocalityData()) : interf(interf) {
		distance = LBLocalityData<T>::Present ? loadBalanceDistance( locality, LBLocalityData<T>::getLocality( interf ), LBLocalityData<T>::getAddress( interf ) ) : LBDistance::DISTANT;
	}
	virtual ~ReferencedInterface() {}

	static bool sort_by_distance(Reference<ReferencedInterface<T>> r1, Reference<ReferencedInterface<T>> r2) {
		return r1->distance < r2->distance;
	}
};

template <class T>
class MultiInterface : public ReferenceCounted<MultiInterface<T>> {
public:
	MultiInterface( const vector<T>& v, LocalityData const& locality = LocalityData() ) : bestCount(0) {
		for(int i=0; i<v.size(); i++)
			alternatives.push_back(KVPair<int,T>(LBDistance::DISTANT,v[i]));
		g_random->randomShuffle(alternatives);
		if ( LBLocalityData<T>::Present ) {
			for(int a=0; a<alternatives.size(); a++)
				alternatives[a].k = loadBalanceDistance( locality, LBLocalityData<T>::getLocality( alternatives[a].v ), LBLocalityData<T>::getAddress( alternatives[a].v ) );
			std::stable_sort( alternatives.begin(), alternatives.end() );
		}
		if(size())
			bestCount = std::lower_bound( alternatives.begin()+1, alternatives.end(), alternatives[0].k+1 ) - alternatives.begin();
	}

	int size() const { return alternatives.size(); }
	int countBest() const {
		return bestCount;
	}
	LBDistance::Type bestDistance() const {
		if( !size() )
			return LBDistance::DISTANT;
		return (LBDistance::Type) alternatives[0].k;
	}
	bool alwaysFresh() const {
		return LBLocalityData<T>::alwaysFresh();
	}

	template <class F>
	F const& get( int index, F T::*member ) const {
		return alternatives[index].v.*member;
	}

	T const& getInterface(int index) { return alternatives[index].v; }
	UID getId( int index ) const { return alternatives[index].v.id(); }

	virtual ~MultiInterface() {}

	std::string description() {
		return describe( alternatives );
	}
private:
	vector<KVPair<int,T>> alternatives;
	int16_t bestCount;
};

template <class T>
class MultiInterface<ReferencedInterface<T>> : public ReferenceCounted<MultiInterface<ReferencedInterface<T>>> {
public:
	MultiInterface( const vector<Reference<ReferencedInterface<T>>>& v ) : alternatives(v), bestCount(0) {
		g_random->randomShuffle(alternatives);
		if ( LBLocalityData<T>::Present ) {
			std::stable_sort( alternatives.begin(), alternatives.end(), ReferencedInterface<T>::sort_by_distance );
		}
		if(size()) {
			for(int i = 1; i < alternatives.size(); i++) {
				if(alternatives[i]->distance > alternatives[0]->distance) {
					bestCount = i;
					return;
				}
			}
			bestCount = size();
		}
	}

	int size() const { return alternatives.size(); }
	int countBest() const {
		return bestCount;
	}
	LBDistance::Type bestDistance() const {
		if( !size() )
			return LBDistance::DISTANT;
		return (LBDistance::Type) alternatives[0]->distance;
	}
	bool alwaysFresh() const {
		return LBLocalityData<T>::alwaysFresh();
	}

	template <class F>
	F const& get( int index, F T::*member ) const {
		return alternatives[index]->interf.*member;
	}

	T const& getInterface(int index) { return alternatives[index]->interf; }
	UID getId( int index ) const { return alternatives[index]->interf.id(); }

	virtual ~MultiInterface() {}

	std::string description() {
		return describe( alternatives );
	}
private:
	vector<Reference<ReferencedInterface<T>>> alternatives;
	int16_t bestCount;
};

template <class Ar, class T> void load(Ar& ar, Reference<MultiInterface<T>>&) { ASSERT(false); }	//< required for Future<T>

#endif
