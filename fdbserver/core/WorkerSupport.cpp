#include <algorithm>
#include <set>
#include <string>

#include "fdbrpc/simulator.h"
#include "fdbserver/core/Knobs.h"
#include "fdbserver/core/ServerDBInfo.actor.h"
#include "flow/TDMetric.actor.h"
#include "flow/Trace.h"
#include "flow/genericactors.actor.h"
#include "flow/network.h"

template class RequestStream<RecruitMasterRequest, false>;
template struct NetNotifiedQueue<RecruitMasterRequest, false>;

template class RequestStream<InitializeCommitProxyRequest, false>;
template struct NetNotifiedQueue<InitializeCommitProxyRequest, false>;

template class RequestStream<InitializeGrvProxyRequest, false>;
template struct NetNotifiedQueue<InitializeGrvProxyRequest, false>;

template class RequestStream<GetServerDBInfoRequest, false>;
template struct NetNotifiedQueue<GetServerDBInfoRequest, false>;

namespace {

Future<std::vector<Endpoint>> tryDBInfoBroadcast(RequestStream<UpdateServerDBInfoRequest> stream,
                                                 UpdateServerDBInfoRequest req) {
	ErrorOr<std::vector<Endpoint>> rep =
	    co_await stream.getReplyUnlessFailedFor(req, SERVER_KNOBS->DBINFO_FAILED_DELAY, 0);
	if (rep.present()) {
		co_return rep.get();
	}
	req.broadcastInfo.push_back(stream.getEndpoint());
	co_return req.broadcastInfo;
}

std::set<std::pair<std::string, std::string>> g_roles;

Standalone<StringRef> roleString(std::set<std::pair<std::string, std::string>> roles, bool with_ids) {
	std::string result;
	for (auto& r : roles) {
		if (!result.empty()) {
			result.append(",");
		}
		result.append(r.first);
		if (with_ids) {
			result.append(":");
			result.append(r.second);
		}
	}
	return StringRef(result);
}

} // namespace

Future<std::vector<Endpoint>> broadcastDBInfoRequest(UpdateServerDBInfoRequest req,
                                                     int sendAmount,
                                                     Optional<Endpoint> sender,
                                                     bool sendReply) {
	std::vector<Future<std::vector<Endpoint>>> replies;
	ReplyPromise<std::vector<Endpoint>> reply = req.reply;
	resetReply(req);
	int currentStream = 0;
	std::vector<Endpoint> broadcastEndpoints = req.broadcastInfo;
	for (int i = 0; i < sendAmount && currentStream < broadcastEndpoints.size(); i++) {
		std::vector<Endpoint> endpoints;
		RequestStream<UpdateServerDBInfoRequest> cur(broadcastEndpoints[currentStream++]);
		while (currentStream < broadcastEndpoints.size() * (i + 1) / sendAmount) {
			endpoints.push_back(broadcastEndpoints[currentStream++]);
		}
		req.broadcastInfo = endpoints;
		replies.push_back(tryDBInfoBroadcast(cur, req));
		resetReply(req);
	}
	co_await waitForAll(replies);
	std::vector<Endpoint> notUpdated;
	if (sender.present()) {
		notUpdated.push_back(sender.get());
	}
	for (auto& it : replies) {
		notUpdated.insert(notUpdated.end(), it.get().begin(), it.get().end());
	}
	if (sendReply) {
		reply.send(notUpdated);
	}
	co_return notUpdated;
}

Future<Void> broadcastTxnRequest(TxnStateRequest req, int sendAmount, bool sendReply) {
	ReplyPromise<Void> reply = req.reply;
	resetReply(req);
	std::vector<Future<Void>> replies;
	int currentStream = 0;
	std::vector<Endpoint> broadcastEndpoints = req.broadcastInfo;
	for (int i = 0; i < sendAmount && currentStream < broadcastEndpoints.size(); i++) {
		std::vector<Endpoint> endpoints;
		RequestStream<TxnStateRequest> cur(broadcastEndpoints[currentStream++]);
		while (currentStream < broadcastEndpoints.size() * (i + 1) / sendAmount) {
			endpoints.push_back(broadcastEndpoints[currentStream++]);
		}
		req.broadcastInfo = endpoints;
		replies.push_back(brokenPromiseToNever(cur.getReply(req)));
		resetReply(req);
	}
	co_await waitForAll(replies);
	if (sendReply) {
		reply.send(Void());
	}
}

bool addressInDbAndPrimarySatelliteDc(const NetworkAddress& address, Reference<AsyncVar<ServerDBInfo> const> dbInfo) {
	for (const auto& logSet : dbInfo->get().logSystemConfig.tLogs) {
		if (logSet.isLocal && logSet.locality == tagLocalitySatellite) {
			for (const auto& tlog : logSet.tLogs) {
				if (tlog.present() && tlog.interf().addresses().contains(address)) {
					return true;
				}
			}
		}
	}

	return false;
}

bool addressInDbAndRemoteDc(const NetworkAddress& address,
                            Reference<AsyncVar<ServerDBInfo> const> dbInfo,
                            Optional<std::vector<NetworkAddress>> storageServers) {
	const auto& dbi = dbInfo->get();

	for (const auto& logSet : dbi.logSystemConfig.tLogs) {
		if (logSet.isLocal || logSet.locality == tagLocalitySatellite) {
			continue;
		}
		for (const auto& tlog : logSet.tLogs) {
			if (tlog.present() && tlog.interf().addresses().contains(address)) {
				return true;
			}
		}

		for (const auto& logRouter : logSet.logRouters) {
			if (logRouter.present() && logRouter.interf().addresses().contains(address)) {
				return true;
			}
		}
	}

	if (storageServers.present() &&
	    std::find(storageServers.get().begin(), storageServers.get().end(), address) != storageServers.get().end()) {
		return true;
	}

	return false;
}

void startRole(const Role& role,
               UID roleId,
               UID workerId,
               const std::map<std::string, std::string>& details,
               const std::string& origination) {
	if (role.includeInTraceRoles) {
		addTraceRole(role.abbreviation);
	}

	TraceEvent ev("Role", roleId);
	ev.detail("As", role.roleName)
	    .detail("Transition", "Begin")
	    .detail("Origination", origination)
	    .detail("OnWorker", workerId);
	for (auto it = details.begin(); it != details.end(); it++) {
		ev.detail(it->first.c_str(), it->second);
	}

	ev.trackLatest(roleId.shortString() + ".Role");

	g_roles.insert({ role.roleName, roleId.shortString() });
	StringMetricHandle("Roles"_sr) = roleString(g_roles, false);
	StringMetricHandle("RolesWithIDs"_sr) = roleString(g_roles, true);
	if (g_network->isSimulated()) {
		g_simulator->addRole(g_network->getLocalAddress(), role.roleName);
	}
}

void endRole(const Role& role, UID id, std::string reason, bool ok, Error e) {
	{
		TraceEvent ev("Role", id);
		if (e.code() != invalid_error_code) {
			ev.errorUnsuppressed(e);
		}
		ev.detail("Transition", "End").detail("As", role.roleName).detail("Reason", reason);

		ev.trackLatest(id.shortString() + ".Role");
	}

	if (!ok) {
		std::string type = role.roleName + "Failed";

		TraceEvent err(SevError, type.c_str(), id);
		if (e.code() != invalid_error_code) {
			err.errorUnsuppressed(e);
		}
		err.detail("Reason", reason);
	}

	latestEventCache.clear(id.shortString());

	g_roles.erase({ role.roleName, id.shortString() });
	StringMetricHandle("Roles"_sr) = roleString(g_roles, false);
	StringMetricHandle("RolesWithIDs"_sr) = roleString(g_roles, true);
	if (g_network->isSimulated()) {
		g_simulator->removeRole(g_network->getLocalAddress(), role.roleName);
	}

	if (role.includeInTraceRoles) {
		removeTraceRole(role.abbreviation);
	}
}

Future<Void> traceRole(Role role, UID roleId) {
	while (true) {
		co_await delay(SERVER_KNOBS->WORKER_LOGGING_INTERVAL);
		TraceEvent("Role", roleId).detail("Transition", "Refresh").detail("As", role.roleName);
	}
}

const Role Role::WORKER("Worker", "WK", false);
const Role Role::STORAGE_SERVER("StorageServer", "SS");
const Role Role::TESTING_STORAGE_SERVER("TestingStorageServer", "ST");
const Role Role::TRANSACTION_LOG("TLog", "TL");
const Role Role::SHARED_TRANSACTION_LOG("SharedTLog", "SL", false);
const Role Role::COMMIT_PROXY("CommitProxyServer", "CP");
const Role Role::GRV_PROXY("GrvProxyServer", "GP");
const Role Role::MASTER("MasterServer", "MS");
const Role Role::RESOLVER("Resolver", "RV");
const Role Role::CLUSTER_CONTROLLER("ClusterController", "CC");
const Role Role::TESTER("Tester", "TS");
const Role Role::LOG_ROUTER("LogRouter", "LR");
const Role Role::DATA_DISTRIBUTOR("DataDistributor", "DD");
const Role Role::RATEKEEPER("Ratekeeper", "RK");
const Role Role::COORDINATOR("Coordinator", "CD");
const Role Role::BACKUP("Backup", "BK");
const Role Role::ENCRYPT_KEY_PROXY("EncryptKeyProxy", "EP");
const Role Role::CONSISTENCYSCAN("ConsistencyScan", "CS");
