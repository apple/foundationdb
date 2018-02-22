/*
 * LogSystemConfig.h
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

#ifndef FDBSERVER_LOGSYSTEMCONFIG_H
#define FDBSERVER_LOGSYSTEMCONFIG_H
#pragma once

#include "TLogInterface.h"
#include "fdbrpc/ReplicationPolicy.h"
#include "DatabaseConfiguration.h"

template <class Interface>
struct OptionalInterface {
	// Represents an interface with a known id() and possibly known actual endpoints.
	// For example, an OptionalInterface<TLogInterface> represents a particular tlog by id, which you might or might not presently know how to communicate with

	UID id() const { return ident; }
	bool present() const { return iface.present(); }
	Interface const& interf() const { return iface.get(); }

	explicit OptionalInterface( UID id ) : ident(id) {}
	explicit OptionalInterface( Interface const& i ) : ident(i.id()), iface(i) {}
	OptionalInterface() {}

	std::string toString() const { return ident.toString(); }

	bool operator==(UID const& r) const { return ident == r; }

	template <class Ar>
	void serialize( Ar& ar ) {
		ar & iface;
		if( !iface.present() ) ar & ident;
		else ident = iface.get().id();
	}

protected:
	UID ident;
	Optional<Interface> iface;
};

struct OldTLogConf {
	vector<OptionalInterface<TLogInterface>> tLogs;
	int32_t tLogWriteAntiQuorum, tLogReplicationFactor;
	std::vector< LocalityData > tLogLocalities; // Stores the localities of the log servers
	IRepPolicyRef	tLogPolicy;
	Version epochEnd;

	OldTLogConf() : tLogWriteAntiQuorum(0), tLogReplicationFactor(0), epochEnd(0) {}

	std::string toString() const {
		return format("anti: %d replication: %d tLogs: %s  tlocalities: %s",
		tLogWriteAntiQuorum, tLogReplicationFactor, epochEnd, describe(tLogs).c_str(), describe(tLogLocalities).c_str()); }

	bool operator == ( const OldTLogConf& rhs ) const {
		bool bIsEqual = true;
		if (tLogWriteAntiQuorum != rhs.tLogWriteAntiQuorum) {
			bIsEqual = false;
		}
		else if (tLogReplicationFactor != rhs.tLogReplicationFactor) {
			bIsEqual = false;
		}
		else if (tLogs.size() != rhs.tLogs.size()) {
			bIsEqual = false;
		}
		if (bIsEqual) {
			for(int j = 0; j < tLogs.size(); j++ ) {
				if (tLogs[j].id() != rhs.tLogs[j].id()) {
					bIsEqual = false;
					break;
				}
				else if (tLogs[j].present() != rhs.tLogs[j].present()) {
					bIsEqual = false;
					break;
				}
				else if (tLogs[j].present() && tLogs[j].interf().commit.getEndpoint().token != rhs.tLogs[j].interf().commit.getEndpoint().token ) {
					bIsEqual = false;
					break;
				}
			}
		}
		return bIsEqual;
	}

	template <class Ar>
	void serialize( Ar& ar ) {
		ar & tLogs & tLogWriteAntiQuorum & tLogReplicationFactor & tLogPolicy & tLogLocalities & epochEnd;
	}
};

struct LogSystemConfig {
	int logSystemType;
	std::vector<OptionalInterface<TLogInterface>> tLogs;
	std::vector< LocalityData > tLogLocalities;
	std::vector<OldTLogConf> oldTLogs;
	int32_t tLogWriteAntiQuorum, tLogReplicationFactor;
	IRepPolicyRef	tLogPolicy;
	//LogEpoch epoch;

	LogSystemConfig() : tLogWriteAntiQuorum(0), tLogReplicationFactor(0), logSystemType(0) {}

	std::string toString() const { return format("type: %d anti: %d replication: %d tLogs: %s oldGenerations: %d tlocalities: %s",
		logSystemType, tLogWriteAntiQuorum, tLogReplicationFactor, describe(tLogs).c_str(), oldTLogs.size(), describe(tLogLocalities).c_str()); }

	std::vector<TLogInterface> allPresentLogs() const {
		std::vector<TLogInterface> results;
		for( int i = 0; i < tLogs.size(); i++ )
			if( tLogs[i].present() )
				results.push_back(tLogs[i].interf());
		return results;
	}

	bool operator == ( const LogSystemConfig& rhs ) const { return isEqual(rhs); }

	bool isEqual(LogSystemConfig const& r) const {
		if (logSystemType != r.logSystemType || tLogWriteAntiQuorum != r.tLogWriteAntiQuorum || tLogReplicationFactor != r.tLogReplicationFactor || tLogs.size() != r.tLogs.size() || oldTLogs.size() != r.oldTLogs.size())
			return false;
		else if ((tLogPolicy && !r.tLogPolicy) || (!tLogPolicy && r.tLogPolicy) || (tLogPolicy && (tLogPolicy->info() != r.tLogPolicy->info())))
			return false;
		for(int i = 0; i < tLogs.size(); i++ ) {
			if( tLogs[i].id() != r.tLogs[i].id() || tLogs[i].present() != r.tLogs[i].present() )
				return false;
			if( tLogs[i].present() && tLogs[i].interf().commit.getEndpoint().token != r.tLogs[i].interf().commit.getEndpoint().token )
				return false;
		}
		for(int i = 0; i < oldTLogs.size(); i++ ) {
			if (oldTLogs[i] != r.oldTLogs[i])
				return false;
		}
		return true;
	}

	bool isEqualIds(LogSystemConfig const& r) const {
		if(logSystemType!=r.logSystemType || tLogWriteAntiQuorum!=r.tLogWriteAntiQuorum || tLogReplicationFactor!=r.tLogReplicationFactor ||
		tLogs.size() != r.tLogs.size() || oldTLogs.size() != r.oldTLogs.size())
			return false;
		else if ((tLogPolicy && !r.tLogPolicy) || (!tLogPolicy && r.tLogPolicy) || (tLogPolicy && (tLogPolicy->info() != r.tLogPolicy->info()))) {
			return false;
		}
		for(int i = 0; i < tLogs.size(); i++ ) {
			if( tLogs[i].id() != r.tLogs[i].id() )
				return false;
		}

		for(int i = 0; i < oldTLogs.size(); i++ ) {
			if (oldTLogs[i].tLogWriteAntiQuorum != r.oldTLogs[i].tLogWriteAntiQuorum || oldTLogs[i].tLogReplicationFactor != r.oldTLogs[i].tLogReplicationFactor || oldTLogs[i].tLogs.size() != r.oldTLogs[i].tLogs.size())
				return false;

			for(int j = 0; j < oldTLogs[i].tLogs.size(); j++ ) {
				if( oldTLogs[i].tLogs[j].id() != r.oldTLogs[i].tLogs[j].id() )
					return false;
			}
		}
		return true;
	}

	bool isNextGenerationOf(LogSystemConfig const& r) const {
		if( !oldTLogs.size() || oldTLogs[0].tLogWriteAntiQuorum!=r.tLogWriteAntiQuorum || oldTLogs[0].tLogReplicationFactor!=r.tLogReplicationFactor || oldTLogs[0].tLogs.size() != r.tLogs.size())
			return false;
		for(int i = 0; i < oldTLogs[0].tLogs.size(); i++ ) {
			if( oldTLogs[0].tLogs[i].id() != r.tLogs[i].id() )
				return false;
		}
		return true;
	}

	template <class Ar>
	void serialize( Ar& ar ) {
		ar & logSystemType & tLogs & oldTLogs & tLogWriteAntiQuorum & tLogReplicationFactor & tLogPolicy & tLogLocalities;
	}
};

#endif
