/*
 * SuspendProcesses.actor.cpp
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

#include <boost/algorithm/string/predicate.hpp>

#include "fdbserver/workloads/workloads.actor.h"
#include "fdbserver/ServerDBInfo.h"
#include "fdbclient/Status.h"
#include "fdbclient/StatusClient.h"
#include "fdbclient/ManagementAPI.actor.h"
#include "fdbclient/RunRYWTransaction.actor.h"
#include "flow/actorcompiler.h" // has to be last include

struct SuspendProcessesWorkload : TestWorkload {
	static constexpr auto NAME = "SuspendProcesses";
	std::vector<std::string> prefixSuspendProcesses;
	double suspendTimeDuration;
	double waitTimeDuration;

	SuspendProcessesWorkload(WorkloadContext const& wcx) : TestWorkload(wcx) {
		prefixSuspendProcesses = getOption(options, "prefixesSuspendProcesses"_sr, std::vector<std::string>());
		waitTimeDuration = getOption(options, "waitTimeDuration"_sr, 0);
		suspendTimeDuration = getOption(options, "suspendTimeDuration"_sr, 0);
	}

	Future<Void> setup(Database const& cx) override { return Void(); }

	ACTOR Future<Void> _start(Database cx, SuspendProcessesWorkload* self) {
		wait(delay(self->waitTimeDuration));
		state ReadYourWritesTransaction tr(cx);
		loop {
			try {
				tr.setOption(FDBTransactionOptions::ACCESS_SYSTEM_KEYS);
				tr.setOption(FDBTransactionOptions::LOCK_AWARE);
				RangeResult kvs =
				    wait(tr.getRange(KeyRangeRef("\xff\xff/worker_interfaces/"_sr, "\xff\xff/worker_interfaces0"_sr),
				                     CLIENT_KNOBS->TOO_MANY));
				ASSERT(!kvs.more);
				std::vector<Standalone<StringRef>> suspendProcessInterfaces;
				for (auto it : kvs) {
					auto ip_port = (it.key.endsWith(":tls"_sr) ? it.key.removeSuffix(":tls"_sr) : it.key)
					                   .removePrefix("\xff\xff/worker_interfaces/"_sr);
					for (auto& killProcess : self->prefixSuspendProcesses) {
						if (boost::starts_with(ip_port.toString().c_str(), killProcess.c_str())) {
							suspendProcessInterfaces.push_back(it.value);
							TraceEvent("SuspendProcessSelectedProcess").detail("IpPort", printable(ip_port));
						}
					}
				}
				for (auto& interf : suspendProcessInterfaces) {
					BinaryReader::fromStringRef<ClientWorkerInterface>(interf, IncludeVersion())
					    .reboot.send(RebootRequest(false, false, self->suspendTimeDuration));
				}
				return Void();
			} catch (Error& e) {
				wait(tr.onError(e));
			}
		}
	}

	Future<Void> start(Database const& cx) override {
		if (clientId != 0)
			return Void();
		return _start(cx, this);
	}

	Future<bool> check(Database const& cx) override { return true; }

	void getMetrics(std::vector<PerfMetric>& m) override {}
};

WorkloadFactory<SuspendProcessesWorkload> SuspendProcessesWorkloadFactory;
