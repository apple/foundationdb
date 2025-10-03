/*
 * SingletonRoles.h
 *
 * This source file is part of the FoundationDB open source project
 *
 * Copyright 2013-2024 Apple Inc. and the FoundationDB project authors
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

#include "fdbserver/ClusterController.actor.h"
#include "fdbserver/RatekeeperInterface.h"
#include "fdbserver/DataDistributorInterface.h"

// This is used to artificially amplify the used count for processes
// occupied by non-singletons. This ultimately makes it less desirable
// for singletons to use those processes as well. This constant should
// be increased if we ever have more than 100 singletons (unlikely).
static constexpr int PID_USED_AMP_FOR_NON_SINGLETON = 100;

// Wrapper for singleton interfaces
template <class Interface>
class Singleton {
protected:
	const Optional<Interface>& interface;

	explicit Singleton(const Optional<Interface>& interface) : interface(interface) {}

public:
	Interface const& getInterface() const { return interface.get(); }
	bool isPresent() const { return interface.present(); }
};

struct RatekeeperSingleton : Singleton<RatekeeperInterface> {

	RatekeeperSingleton(const Optional<RatekeeperInterface>& interface) : Singleton(interface) {}

	Role getRole() const { return Role::RATEKEEPER; }
	ProcessClass::ClusterRole getClusterRole() const { return ProcessClass::Ratekeeper; }

	void setInterfaceToDbInfo(ClusterControllerData& cc) const {
		if (interface.present()) {
			TraceEvent("CCRK_SetInf", cc.id).detail("Id", interface.get().id());
			cc.db.setRatekeeper(interface.get());
		}
	}
	void halt(ClusterControllerData& cc, Optional<Standalone<StringRef>> pid) const {
		if (interface.present() && cc.id_worker.contains(pid)) {
			cc.id_worker[pid].haltRatekeeper =
			    brokenPromiseToNever(interface.get().haltRatekeeper.getReply(HaltRatekeeperRequest(cc.id)));
		}
	}
	void recruit(ClusterControllerData& cc) const {
		cc.lastRecruitTime = now();
		cc.recruitRatekeeper.set(true);
	}
};

struct DataDistributorSingleton : Singleton<DataDistributorInterface> {

	DataDistributorSingleton(const Optional<DataDistributorInterface>& interface) : Singleton(interface) {}

	Role getRole() const { return Role::DATA_DISTRIBUTOR; }
	ProcessClass::ClusterRole getClusterRole() const { return ProcessClass::DataDistributor; }

	void setInterfaceToDbInfo(ClusterControllerData& cc) const {
		if (interface.present()) {
			TraceEvent("CCDD_SetInf", cc.id).detail("Id", interface.get().id());
			cc.db.setDistributor(interface.get());
		}
	}
	void halt(ClusterControllerData& cc, Optional<Standalone<StringRef>> pid) const {
		if (interface.present() && cc.id_worker.contains(pid)) {
			cc.id_worker[pid].haltDistributor =
			    brokenPromiseToNever(interface.get().haltDataDistributor.getReply(HaltDataDistributorRequest(cc.id)));
		}
	}
	void recruit(ClusterControllerData& cc) const {
		cc.lastRecruitTime = now();
		cc.recruitDistributor.set(true);
	}
};

struct ConsistencyScanSingleton : Singleton<ConsistencyScanInterface> {

	ConsistencyScanSingleton(const Optional<ConsistencyScanInterface>& interface) : Singleton(interface) {}

	Role getRole() const { return Role::CONSISTENCYSCAN; }
	ProcessClass::ClusterRole getClusterRole() const { return ProcessClass::ConsistencyScan; }

	void setInterfaceToDbInfo(ClusterControllerData& cc) const {
		if (interface.present()) {
			TraceEvent("CCCK_SetInf", cc.id).detail("Id", interface.get().id());
			cc.db.setConsistencyScan(interface.get());
		}
	}
	void halt(ClusterControllerData& cc, Optional<Standalone<StringRef>> pid) const {
		if (interface.present()) {
			cc.id_worker[pid].haltConsistencyScan =
			    brokenPromiseToNever(interface.get().haltConsistencyScan.getReply(HaltConsistencyScanRequest(cc.id)));
		}
	}
	void recruit(ClusterControllerData& cc) const {
		cc.lastRecruitTime = now();
		cc.recruitConsistencyScan.set(true);
	}
};

struct EncryptKeyProxySingleton : Singleton<EncryptKeyProxyInterface> {

	EncryptKeyProxySingleton(const Optional<EncryptKeyProxyInterface>& interface) : Singleton(interface) {}

	Role getRole() const { return Role::ENCRYPT_KEY_PROXY; }
	ProcessClass::ClusterRole getClusterRole() const { return ProcessClass::EncryptKeyProxy; }

	void setInterfaceToDbInfo(ClusterControllerData& cc) const {
		if (interface.present()) {
			TraceEvent("CCEKP_SetInf", cc.id).detail("Id", interface.get().id());
			cc.db.setEncryptKeyProxy(interface.get());
		}
	}
	void halt(ClusterControllerData& cc, Optional<Standalone<StringRef>> pid) const {
		if (interface.present() && cc.id_worker.contains(pid)) {
			cc.id_worker[pid].haltEncryptKeyProxy =
			    brokenPromiseToNever(interface.get().haltEncryptKeyProxy.getReply(HaltEncryptKeyProxyRequest(cc.id)));
		}
	}
	void recruit(ClusterControllerData& cc) const {
		cc.lastRecruitTime = now();
		cc.recruitEncryptKeyProxy.set(true);
	}
};

struct SingletonRecruitThrottler {
	double lastRecruitStart;

	SingletonRecruitThrottler() : lastRecruitStart(-1) {}

	double newRecruitment() {
		double n = now();
		double waitTime =
		    std::max(0.0, (lastRecruitStart + SERVER_KNOBS->CC_THROTTLE_SINGLETON_RERECRUIT_INTERVAL - n));
		lastRecruitStart = n;
		return waitTime;
	}
};
