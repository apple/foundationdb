/*
 * StatusCommand.actor.cpp
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

#include "fdbcli/fdbcli.actor.h"

#include "fdbclient/FDBOptions.g.h"
#include "fdbclient/IClientApi.h"
#include "fdbclient/Knobs.h"
#include "fdbclient/StatusClient.h"

#include "flow/Arena.h"
#include "flow/FastRef.h"
#include "flow/ThreadHelper.actor.h"
#include "flow/actorcompiler.h" // This must be the last #include.

namespace {

std::string getCoordinatorsInfoString(StatusObjectReader statusObj) {
	std::string outputString;
	try {
		StatusArray coordinatorsArr = statusObj["client.coordinators.coordinators"].get_array();
		for (StatusObjectReader coor : coordinatorsArr)
			outputString += format("\n  %s  (%s)",
			                       coor["address"].get_str().c_str(),
			                       coor["reachable"].get_bool() ? "reachable" : "unreachable");
	} catch (std::runtime_error&) {
		outputString = "\n  Unable to retrieve list of coordination servers";
	}

	return outputString;
}

std::string lineWrap(const char* text, int col) {
	const char* iter = text;
	const char* start = text;
	const char* space = nullptr;
	std::string out = "";
	do {
		iter++;
		if (*iter == '\n' || *iter == ' ' || *iter == '\0')
			space = iter;
		if (*iter == '\n' || *iter == '\0' || (iter - start == col)) {
			if (!space)
				space = iter;
			out += format("%.*s\n", (int)(space - start), start);
			start = space;
			if (*start == ' ' /* || *start == '\n'*/)
				start++;
			space = nullptr;
		}
	} while (*iter);
	return out;
}

std::pair<int, int> getNumOfNonExcludedProcessAndZones(StatusObjectReader statusObjCluster) {
	StatusObjectReader processesMap;
	std::set<std::string> zones;
	int numOfNonExcludedProcesses = 0;
	if (statusObjCluster.get("processes", processesMap)) {
		for (auto proc : processesMap.obj()) {
			StatusObjectReader process(proc.second);
			if (process.has("excluded") && process.last().get_bool())
				continue;
			numOfNonExcludedProcesses++;
			std::string zoneId;
			if (process.get("locality.zoneid", zoneId)) {
				zones.insert(zoneId);
			}
		}
	}
	return { numOfNonExcludedProcesses, zones.size() };
}

int getNumofNonExcludedMachines(StatusObjectReader statusObjCluster) {
	StatusObjectReader machineMap;
	int numOfNonExcludedMachines = 0;
	if (statusObjCluster.get("machines", machineMap)) {
		for (auto mach : machineMap.obj()) {
			StatusObjectReader machine(mach.second);
			if (machine.has("excluded") && !machine.last().get_bool())
				numOfNonExcludedMachines++;
		}
	}
	return numOfNonExcludedMachines;
}

std::string getDateInfoString(StatusObjectReader statusObj, std::string key) {
	time_t curTime;
	if (!statusObj.has(key)) {
		return "";
	}
	curTime = statusObj.last().get_int64();
	char buffer[128];
	struct tm* timeinfo;
	timeinfo = localtime(&curTime);
	strftime(buffer, 128, "%m/%d/%y %H:%M:%S", timeinfo);
	return std::string(buffer);
}

std::string getProcessAddressByServerID(StatusObjectReader processesMap, std::string serverID) {
	if (serverID == "")
		return "unknown";

	for (auto proc : processesMap.obj()) {
		try {
			StatusArray rolesArray = proc.second.get_obj()["roles"].get_array();
			for (StatusObjectReader role : rolesArray) {
				if (role["id"].get_str().find(serverID) == 0) {
					// If this next line throws, then we found the serverID but the role has no address, so the role is
					// skipped.
					return proc.second.get_obj()["address"].get_str();
				}
			}
		} catch (std::exception&) {
			// If an entry in the process map is badly formed then something will throw. Since we are
			// looking for a positive match, just ignore any read execeptions and move on to the next proc
		}
	}
	return "unknown";
}

std::string getWorkloadRates(StatusObjectReader statusObj,
                             bool unknown,
                             std::string first,
                             std::string second,
                             bool transactionSection = false) {
	// Re-point statusObj at either the transactions sub-doc or the operations sub-doc depending on transactionSection
	// flag
	if (transactionSection) {
		if (!statusObj.get("transactions", statusObj))
			return "unknown";
	} else {
		if (!statusObj.get("operations", statusObj))
			return "unknown";
	}

	std::string path = first + "." + second;
	double value;
	if (!unknown && statusObj.get(path, value)) {
		return format("%d Hz", (int)round(value));
	}
	return "unknown";
}

void getBackupDRTags(StatusObjectReader& statusObjCluster,
                     const char* context,
                     std::map<std::string, std::string>& tagMap) {
	std::string path = format("layers.%s.tags", context);
	StatusObjectReader tags;
	if (statusObjCluster.tryGet(path, tags)) {
		for (auto itr : tags.obj()) {
			JSONDoc tag(itr.second);
			bool running = false;
			tag.tryGet("running_backup", running);
			if (running) {
				std::string uid;
				if (tag.tryGet("mutation_stream_id", uid)) {
					tagMap[itr.first] = uid;
				} else {
					tagMap[itr.first] = "";
				}
			}
		}
	}
}

std::string logBackupDR(const char* context, std::map<std::string, std::string> const& tagMap) {
	std::string outputString = "";
	if (tagMap.size() > 0) {
		outputString += format("\n\n%s:", context);
		for (auto itr : tagMap) {
			outputString += format("\n  %-22s", itr.first.c_str());
			if (itr.second.size() > 0) {
				outputString += format(" - %s", itr.second.c_str());
			}
		}
	}

	return outputString;
}

} // namespace

namespace fdb_cli {

void printStatus(StatusObjectReader statusObj,
                 StatusClient::StatusLevel level,
                 bool displayDatabaseAvailable,
                 bool hideErrorMessages) {
	if (FlowTransport::transport().incompatibleOutgoingConnectionsPresent()) {
		fprintf(
		    stderr,
		    "WARNING: One or more of the processes in the cluster is incompatible with this version of fdbcli.\n\n");
	}

	try {
		bool printedCoordinators = false;

		// status or status details
		if (level == StatusClient::NORMAL || level == StatusClient::DETAILED) {

			StatusObjectReader statusObjClient;
			statusObj.get("client", statusObjClient);

			// The way the output string is assembled is to add new line character before addition to the string rather
			// than after
			std::string outputString = "";
			std::string clusterFilePath;
			if (statusObjClient.get("cluster_file.path", clusterFilePath))
				outputString = format("Using cluster file `%s'.\n", clusterFilePath.c_str());
			else
				outputString = "Using unknown cluster file.\n";

			StatusObjectReader statusObjCoordinators;
			StatusArray coordinatorsArr;

			if (statusObjClient.get("coordinators", statusObjCoordinators)) {
				// Look for a second "coordinators", under the first one.
				if (statusObjCoordinators.has("coordinators"))
					coordinatorsArr = statusObjCoordinators.last().get_array();
			}

			// Check if any coordination servers are unreachable
			bool quorum_reachable;
			if (statusObjCoordinators.get("quorum_reachable", quorum_reachable) && !quorum_reachable) {
				outputString += "\nCould not communicate with a quorum of coordination servers:";
				outputString += getCoordinatorsInfoString(statusObj);

				printf("%s\n", outputString.c_str());
				return;
			} else {
				for (StatusObjectReader coor : coordinatorsArr) {
					bool reachable;
					if (coor.get("reachable", reachable) && !reachable) {
						outputString += "\nCould not communicate with all of the coordination servers."
						                "\n  The database will remain operational as long as we"
						                "\n  can connect to a quorum of servers, however the fault"
						                "\n  tolerance of the system is reduced as long as the"
						                "\n  servers remain disconnected.\n";
						outputString += getCoordinatorsInfoString(statusObj);
						outputString += "\n";
						printedCoordinators = true;
						break;
					}
				}
			}

			// print any client messages
			if (statusObjClient.has("messages")) {
				for (StatusObjectReader message : statusObjClient.last().get_array()) {
					std::string desc;
					if (message.get("description", desc))
						outputString += "\n" + lineWrap(desc.c_str(), 80);
				}
			}

			bool fatalRecoveryState = false;
			StatusObjectReader statusObjCluster;
			try {
				if (statusObj.get("cluster", statusObjCluster)) {

					StatusObjectReader recoveryState;
					if (statusObjCluster.get("recovery_state", recoveryState)) {
						std::string name;
						std::string description;
						if (recoveryState.get("name", name) && recoveryState.get("description", description) &&
						    name != "accepting_commits" && name != "all_logs_recruited" &&
						    name != "storage_recovered" && name != "fully_recovered") {
							fatalRecoveryState = true;

							if (name == "recruiting_transaction_servers") {
								description +=
								    format("\nNeed at least %d log servers across unique zones, %d commit proxies, "
								           "%d GRV proxies and %d resolvers.",
								           recoveryState["required_logs"].get_int(),
								           recoveryState["required_commit_proxies"].get_int(),
								           recoveryState["required_grv_proxies"].get_int(),
								           recoveryState["required_resolvers"].get_int());
								if (statusObjCluster.has("machines") && statusObjCluster.has("processes")) {
									auto numOfNonExcludedProcessesAndZones =
									    getNumOfNonExcludedProcessAndZones(statusObjCluster);
									description +=
									    format("\nHave %d non-excluded processes on %d machines across %d zones.",
									           numOfNonExcludedProcessesAndZones.first,
									           getNumofNonExcludedMachines(statusObjCluster),
									           numOfNonExcludedProcessesAndZones.second);
								}
							} else if (name == "locking_old_transaction_servers" &&
							           recoveryState["missing_logs"].get_str().size()) {
								description += format("\nNeed one or more of the following log servers: %s",
								                      recoveryState["missing_logs"].get_str().c_str());
							}
							description = lineWrap(description.c_str(), 80);
							if (!printedCoordinators &&
							    (name == "reading_coordinated_state" || name == "locking_coordinated_state" ||
							     name == "configuration_never_created" || name == "writing_coordinated_state")) {
								description += getCoordinatorsInfoString(statusObj);
								description += "\n";
								printedCoordinators = true;
							}

							outputString += "\n" + description;
						}
					}
				}
			} catch (std::runtime_error&) {
			}

			// Check if cluster controllable is reachable
			try {
				// print any cluster messages
				if (statusObjCluster.has("messages") && statusObjCluster.last().get_array().size()) {

					// any messages we don't want to display
					std::set<std::string> skipMsgs = { "unreachable_process", "" };
					if (fatalRecoveryState) {
						skipMsgs.insert("status_incomplete");
						skipMsgs.insert("unreadable_configuration");
						skipMsgs.insert("immediate_priority_transaction_start_probe_timeout");
						skipMsgs.insert("batch_priority_transaction_start_probe_timeout");
						skipMsgs.insert("transaction_start_probe_timeout");
						skipMsgs.insert("read_probe_timeout");
						skipMsgs.insert("commit_probe_timeout");
					}

					for (StatusObjectReader msgObj : statusObjCluster.last().get_array()) {
						std::string messageName;
						if (!msgObj.get("name", messageName)) {
							continue;
						}
						if (skipMsgs.count(messageName)) {
							continue;
						} else if (messageName == "client_issues") {
							if (msgObj.has("issues")) {
								for (StatusObjectReader issue : msgObj["issues"].get_array()) {
									std::string issueName;
									if (!issue.get("name", issueName)) {
										continue;
									}

									std::string description;
									if (!issue.get("description", description)) {
										description = issueName;
									}

									std::string countStr;
									StatusArray addresses;
									if (!issue.has("addresses")) {
										countStr = "Some client(s)";
									} else {
										addresses = issue["addresses"].get_array();
										countStr = format("%d client(s)", addresses.size());
									}
									outputString +=
									    format("\n%s reported: %s\n", countStr.c_str(), description.c_str());

									if (level == StatusClient::StatusLevel::DETAILED) {
										for (int i = 0; i < addresses.size() && i < 4; ++i) {
											outputString += format("  %s\n", addresses[i].get_str().c_str());
										}
										if (addresses.size() > 4) {
											outputString += "  ...\n";
										}
									}
								}
							}
						} else {
							if (msgObj.has("description"))
								outputString += "\n" + lineWrap(msgObj.last().get_str().c_str(), 80);
						}
					}
				}
			} catch (std::runtime_error&) {
			}

			if (fatalRecoveryState) {
				printf("%s", outputString.c_str());
				return;
			}

			StatusObjectReader statusObjConfig;
			StatusArray excludedServersArr;
			Optional<std::string> activePrimaryDC;

			if (statusObjCluster.has("active_primary_dc")) {
				activePrimaryDC = statusObjCluster["active_primary_dc"].get_str();
			}
			if (statusObjCluster.get("configuration", statusObjConfig)) {
				if (statusObjConfig.has("excluded_servers"))
					excludedServersArr = statusObjConfig.last().get_array();
			}

			// If there is a configuration message then there is no configuration information to display
			outputString += "\nConfiguration:";
			std::string outputStringCache = outputString;
			bool isOldMemory = false;
			try {
				// Configuration section
				// FIXME: Should we suppress this if there are cluster messages implying that the database has no
				// configuration?

				outputString += "\n  Redundancy mode        - ";
				std::string strVal;

				if (statusObjConfig.get("redundancy_mode", strVal)) {
					outputString += strVal;
				} else
					outputString += "unknown";

				outputString += "\n  Storage engine         - ";
				if (statusObjConfig.get("storage_engine", strVal)) {
					if (strVal == "memory-1") {
						isOldMemory = true;
					}
					outputString += strVal;
				} else
					outputString += "unknown";

				int intVal;
				outputString += "\n  Coordinators           - ";
				if (statusObjConfig.get("coordinators_count", intVal)) {
					outputString += std::to_string(intVal);
				} else
					outputString += "unknown";

				if (excludedServersArr.size()) {
					outputString += format("\n  Exclusions             - %d (type `exclude' for details)",
					                       excludedServersArr.size());
				}

				if (statusObjConfig.get("commit_proxies", intVal))
					outputString += format("\n  Desired Commit Proxies - %d", intVal);

				if (statusObjConfig.get("grv_proxies", intVal))
					outputString += format("\n  Desired GRV Proxies    - %d", intVal);

				if (statusObjConfig.get("resolvers", intVal))
					outputString += format("\n  Desired Resolvers      - %d", intVal);

				if (statusObjConfig.get("logs", intVal))
					outputString += format("\n  Desired Logs           - %d", intVal);

				if (statusObjConfig.get("remote_logs", intVal))
					outputString += format("\n  Desired Remote Logs    - %d", intVal);

				if (statusObjConfig.get("log_routers", intVal))
					outputString += format("\n  Desired Log Routers    - %d", intVal);

				if (statusObjConfig.get("tss_count", intVal) && intVal > 0) {
					int activeTss = 0;
					if (statusObjCluster.has("active_tss_count")) {
						statusObjCluster.get("active_tss_count", activeTss);
					}
					outputString += format("\n  TSS                    - %d/%d", activeTss, intVal);

					if (statusObjConfig.get("tss_storage_engine", strVal))
						outputString += format("\n  TSS Storage Engine     - %s", strVal.c_str());
				}

				outputString += "\n  Usable Regions         - ";
				if (statusObjConfig.get("usable_regions", intVal)) {
					outputString += std::to_string(intVal);
				} else {
					outputString += "unknown";
				}

				StatusArray regions;
				if (statusObjConfig.has("regions")) {
					outputString += "\n  Regions: ";
					regions = statusObjConfig["regions"].get_array();
					for (StatusObjectReader region : regions) {
						bool isPrimary = false;
						std::vector<std::string> regionSatelliteDCs;
						std::string regionDC;
						for (StatusObjectReader dc : region["datacenters"].get_array()) {
							if (!dc.has("satellite")) {
								regionDC = dc["id"].get_str();
								if (activePrimaryDC.present() && dc["id"].get_str() == activePrimaryDC.get()) {
									isPrimary = true;
								}
							} else if (dc["satellite"].get_int() == 1) {
								regionSatelliteDCs.push_back(dc["id"].get_str());
							}
						}
						if (activePrimaryDC.present()) {
							if (isPrimary) {
								outputString += "\n    Primary -";
							} else {
								outputString += "\n    Remote -";
							}
						} else {
							outputString += "\n    Region -";
						}
						outputString += format("\n        Datacenter                    - %s", regionDC.c_str());
						if (regionSatelliteDCs.size() > 0) {
							outputString += "\n        Satellite datacenters         - ";
							for (int i = 0; i < regionSatelliteDCs.size(); i++) {
								if (i != regionSatelliteDCs.size() - 1) {
									outputString += format("%s, ", regionSatelliteDCs[i].c_str());
								} else {
									outputString += format("%s", regionSatelliteDCs[i].c_str());
								}
							}
						}
						isPrimary = false;
						if (region.get("satellite_redundancy_mode", strVal)) {
							outputString += format("\n        Satellite Redundancy Mode     - %s", strVal.c_str());
						}
						if (region.get("satellite_anti_quorum", intVal)) {
							outputString += format("\n        Satellite Anti Quorum         - %d", intVal);
						}
						if (region.get("satellite_logs", intVal)) {
							outputString += format("\n        Satellite Logs                - %d", intVal);
						}
						if (region.get("satellite_log_policy", strVal)) {
							outputString += format("\n        Satellite Log Policy          - %s", strVal.c_str());
						}
						if (region.get("satellite_log_replicas", intVal)) {
							outputString += format("\n        Satellite Log Replicas        - %d", intVal);
						}
						if (region.get("satellite_usable_dcs", intVal)) {
							outputString += format("\n        Satellite Usable DCs          - %d", intVal);
						}
					}
				}
			} catch (std::runtime_error&) {
				outputString = outputStringCache;
				outputString += "\n  Unable to retrieve configuration status";
			}

			// Cluster section
			outputString += "\n\nCluster:";
			StatusObjectReader processesMap;
			StatusObjectReader machinesMap;

			outputStringCache = outputString;

			bool machinesAreZones = true;
			std::map<std::string, int> zones;
			try {
				outputString += "\n  FoundationDB processes - ";
				if (statusObjCluster.get("processes", processesMap)) {

					outputString += format("%d", processesMap.obj().size());

					int errors = 0;
					int processExclusions = 0;
					for (auto p : processesMap.obj()) {
						StatusObjectReader process(p.second);
						bool excluded = process.has("excluded") && process.last().get_bool();
						if (excluded) {
							processExclusions++;
						}
						if (process.has("messages") && process.last().get_array().size()) {
							errors++;
						}

						std::string zoneId;
						if (process.get("locality.zoneid", zoneId)) {
							std::string machineId;
							if (!process.get("locality.machineid", machineId) || machineId != zoneId) {
								machinesAreZones = false;
							}
							int& nonExcluded = zones[zoneId];
							if (!excluded) {
								nonExcluded = 1;
							}
						}
					}

					if (errors > 0 || processExclusions) {
						outputString += format(" (less %d excluded; %d with errors)", processExclusions, errors);
					}

				} else
					outputString += "unknown";

				if (zones.size() > 0) {
					outputString += format("\n  Zones                  - %d", zones.size());
					int zoneExclusions = 0;
					for (auto itr : zones) {
						if (itr.second == 0) {
							++zoneExclusions;
						}
					}
					if (zoneExclusions > 0) {
						outputString += format(" (less %d excluded)", zoneExclusions);
					}
				} else {
					outputString += "\n  Zones                  - unknown";
				}

				outputString += "\n  Machines               - ";
				if (statusObjCluster.get("machines", machinesMap)) {
					outputString += format("%d", machinesMap.obj().size());

					int machineExclusions = 0;
					for (auto mach : machinesMap.obj()) {
						StatusObjectReader machine(mach.second);
						if (machine.has("excluded") && machine.last().get_bool())
							machineExclusions++;
					}

					if (machineExclusions) {
						outputString += format(" (less %d excluded)", machineExclusions);
					}

					int64_t minMemoryAvailable = std::numeric_limits<int64_t>::max();
					for (auto proc : processesMap.obj()) {
						StatusObjectReader process(proc.second);
						int64_t availBytes;
						if (process.get("memory.available_bytes", availBytes)) {
							minMemoryAvailable = std::min(minMemoryAvailable, availBytes);
						}
					}

					if (minMemoryAvailable < std::numeric_limits<int64_t>::max()) {
						double worstServerGb = minMemoryAvailable / (1024.0 * 1024 * 1024);
						outputString += "\n  Memory availability    - ";
						outputString += format("%.1f GB per process on machine with least available", worstServerGb);
						outputString += minMemoryAvailable < 4294967296
						                    ? "\n                           >>>>> (WARNING: 4.0 GB recommended) <<<<<"
						                    : "";
					}

					double retransCount = 0;
					for (auto mach : machinesMap.obj()) {
						StatusObjectReader machine(mach.second);
						double hz;
						if (machine.get("network.tcp_segments_retransmitted.hz", hz))
							retransCount += hz;
					}

					if (retransCount > 0) {
						outputString += format("\n  Retransmissions rate   - %d Hz", (int)round(retransCount));
					}
				} else
					outputString += "\n  Machines               - unknown";

				StatusObjectReader faultTolerance;
				if (statusObjCluster.get("fault_tolerance", faultTolerance)) {
					int availLoss, dataLoss;

					if (faultTolerance.get("max_zone_failures_without_losing_availability", availLoss) &&
					    faultTolerance.get("max_zone_failures_without_losing_data", dataLoss)) {

						outputString += "\n  Fault Tolerance        - ";

						int minLoss = std::min(availLoss, dataLoss);
						const char* faultDomain = machinesAreZones ? "machine" : "zone";
						outputString += format("%d %ss", minLoss, faultDomain);

						if (dataLoss > availLoss) {
							outputString += format(" (%d without data loss)", dataLoss);
						}

						if (dataLoss == -1) {
							ASSERT_WE_THINK(availLoss == -1);
							outputString += format(
							    "\n\n  Warning: the database may have data loss and availability loss. Please restart "
							    "following tlog interfaces, otherwise storage servers may never be able to catch "
							    "up.\n");
							StatusObjectReader logs;
							if (statusObjCluster.has("logs")) {
								for (StatusObjectReader logEpoch : statusObjCluster.last().get_array()) {
									bool possiblyLosingData;
									if (logEpoch.get("possibly_losing_data", possiblyLosingData) &&
									    !possiblyLosingData) {
										continue;
									}
									// Current epoch doesn't have an end version.
									int64_t epoch, beginVersion, endVersion = invalidVersion;
									bool current;
									logEpoch.get("epoch", epoch);
									logEpoch.get("begin_version", beginVersion);
									logEpoch.get("end_version", endVersion);
									logEpoch.get("current", current);
									std::string missing_log_interfaces;
									if (logEpoch.has("log_interfaces")) {
										for (StatusObjectReader logInterface : logEpoch.last().get_array()) {
											bool healthy;
											std::string address, id;
											if (logInterface.get("healthy", healthy) && !healthy) {
												logInterface.get("id", id);
												logInterface.get("address", address);
												missing_log_interfaces += format("%s,%s ", id.c_str(), address.c_str());
											}
										}
									}
									outputString += format(
									    "  %s log epoch: %lld begin: %lld end: %s, missing "
									    "log interfaces(id,address): %s\n",
									    current ? "Current" : "Old",
									    epoch,
									    beginVersion,
									    endVersion == invalidVersion ? "(unknown)" : format("%lld", endVersion).c_str(),
									    missing_log_interfaces.c_str());
								}
							}
						}
					}
				}

				std::string serverTime = getDateInfoString(statusObjCluster, "cluster_controller_timestamp");
				if (serverTime != "") {
					outputString += "\n  Server time            - " + serverTime;
				}
			} catch (std::runtime_error&) {
				outputString = outputStringCache;
				outputString += "\n  Unable to retrieve cluster status";
			}

			StatusObjectReader statusObjData;
			statusObjCluster.get("data", statusObjData);

			// Data section
			outputString += "\n\nData:";
			outputStringCache = outputString;
			try {
				outputString += "\n  Replication health     - ";

				StatusObjectReader statusObjDataState;
				statusObjData.get("state", statusObjDataState);

				std::string dataState;
				statusObjDataState.get("name", dataState);

				std::string description = "";
				statusObjDataState.get("description", description);

				bool healthy;
				if (statusObjDataState.get("healthy", healthy) && healthy) {
					outputString += "Healthy" + (description != "" ? " (" + description + ")" : "");
				} else if (dataState == "missing_data") {
					outputString += "UNHEALTHY" + (description != "" ? ": " + description : "");
				} else if (dataState == "healing") {
					outputString += "HEALING" + (description != "" ? ": " + description : "");
				} else if (description != "") {
					outputString += description;
				} else {
					outputString += "unknown";
				}

				if (statusObjData.has("moving_data")) {
					StatusObjectReader movingData = statusObjData.last();
					double dataInQueue, dataInFlight;
					if (movingData.get("in_queue_bytes", dataInQueue) &&
					    movingData.get("in_flight_bytes", dataInFlight))
						outputString += format("\n  Moving data            - %.3f GB",
						                       ((double)dataInQueue + (double)dataInFlight) / 1e9);
				} else if (dataState == "initializing") {
					outputString += "\n  Moving data            - unknown (initializing)";
				} else {
					outputString += "\n  Moving data            - unknown";
				}

				outputString += "\n  Sum of key-value sizes - ";

				if (statusObjData.has("total_kv_size_bytes")) {
					double totalDBBytes = statusObjData.last().get_int64();

					if (totalDBBytes >= 1e12)
						outputString += format("%.3f TB", (totalDBBytes / 1e12));

					else if (totalDBBytes >= 1e9)
						outputString += format("%.3f GB", (totalDBBytes / 1e9));

					else
						// no decimal points for MB
						outputString += format("%d MB", (int)round(totalDBBytes / 1e6));
				} else {
					outputString += "unknown";
				}

				outputString += "\n  Disk space used        - ";

				if (statusObjData.has("total_disk_used_bytes")) {
					double totalDiskUsed = statusObjData.last().get_int64();

					if (totalDiskUsed >= 1e12)
						outputString += format("%.3f TB", (totalDiskUsed / 1e12));

					else if (totalDiskUsed >= 1e9)
						outputString += format("%.3f GB", (totalDiskUsed / 1e9));

					else
						// no decimal points for MB
						outputString += format("%d MB", (int)round(totalDiskUsed / 1e6));
				} else
					outputString += "unknown";

			} catch (std::runtime_error&) {
				outputString = outputStringCache;
				outputString += "\n  Unable to retrieve data status";
			}
			// Storage Wiggle section
			StatusObjectReader storageWigglerObj;
			std::string storageWigglerString;
			try {
				if (statusObjCluster.get("storage_wiggler", storageWigglerObj)) {
					int size = 0;
					if (storageWigglerObj.has("wiggle_server_addresses")) {
						storageWigglerString += "\n  Wiggle server addresses-";
						for (auto& v : storageWigglerObj.obj().at("wiggle_server_addresses").get_array()) {
							storageWigglerString += " " + v.get_str();
							size += 1;
						}
					}
					storageWigglerString += "\n  Wiggle server count    - " + std::to_string(size);
				}
			} catch (std::runtime_error&) {
				storageWigglerString += "\n  Unable to retrieve storage wiggler status";
			}
			if (storageWigglerString.size()) {
				outputString += "\n\nStorage wiggle:";
				outputString += storageWigglerString;
			}

			// Operating space section
			outputString += "\n\nOperating space:";
			std::string operatingSpaceString = "";
			try {
				int64_t val;
				if (statusObjData.get("least_operating_space_bytes_storage_server", val))
					operatingSpaceString += format("\n  Storage server         - %.1f GB free on most full server",
					                               std::max(val / 1e9, 0.0));

				if (statusObjData.get("least_operating_space_bytes_log_server", val))
					operatingSpaceString += format("\n  Log server             - %.1f GB free on most full server",
					                               std::max(val / 1e9, 0.0));

			} catch (std::runtime_error&) {
				operatingSpaceString = "";
			}

			if (operatingSpaceString.empty()) {
				operatingSpaceString += "\n  Unable to retrieve operating space status";
			}
			outputString += operatingSpaceString;

			// Workload section
			outputString += "\n\nWorkload:";
			outputStringCache = outputString;
			bool foundLogAndStorage = false;
			try {
				// Determine which rates are unknown
				StatusObjectReader statusObjWorkload;
				statusObjCluster.get("workload", statusObjWorkload);

				std::string performanceLimited = "";
				bool unknownMCT = false;
				bool unknownRP = false;

				// Print performance limit details if known.
				try {
					StatusObjectReader limit = statusObjCluster["qos.performance_limited_by"];
					std::string name = limit["name"].get_str();
					if (name != "workload") {
						std::string desc = limit["description"].get_str();
						std::string serverID;
						limit.get("reason_server_id", serverID);
						std::string procAddr = getProcessAddressByServerID(processesMap, serverID);
						performanceLimited = format("\n  Performance limited by %s: %s",
						                            (procAddr == "unknown")
						                                ? ("server" + (serverID == "" ? "" : (" " + serverID))).c_str()
						                                : "process",
						                            desc.c_str());
						if (procAddr != "unknown")
							performanceLimited += format("\n  Most limiting process: %s", procAddr.c_str());
					}
				} catch (std::exception&) {
					// If anything here throws (such as for an incompatible type) ignore it.
				}

				// display the known rates
				outputString += "\n  Read rate              - ";
				outputString += getWorkloadRates(statusObjWorkload, unknownRP, "reads", "hz");

				outputString += "\n  Write rate             - ";
				outputString += getWorkloadRates(statusObjWorkload, unknownMCT, "writes", "hz");

				outputString += "\n  Transactions started   - ";
				outputString += getWorkloadRates(statusObjWorkload, unknownMCT, "started", "hz", true);

				outputString += "\n  Transactions committed - ";
				outputString += getWorkloadRates(statusObjWorkload, unknownMCT, "committed", "hz", true);

				outputString += "\n  Conflict rate          - ";
				outputString += getWorkloadRates(statusObjWorkload, unknownMCT, "conflicted", "hz", true);

				outputString += unknownRP ? "" : performanceLimited;

				// display any process messages
				// FIXME:  Above comment is not what this code block does, it actually just looks for a specific message
				// in the process map, *by description*, and adds process addresses that have it to a vector.  Either
				// change the comment or the code.
				std::vector<std::string> messagesAddrs;
				for (auto proc : processesMap.obj()) {
					StatusObjectReader process(proc.second);
					if (process.has("roles")) {
						StatusArray rolesArray = proc.second.get_obj()["roles"].get_array();
						bool storageRole = false;
						bool logRole = false;
						for (StatusObjectReader role : rolesArray) {
							if (role["role"].get_str() == "storage") {
								storageRole = true;
							} else if (role["role"].get_str() == "log") {
								logRole = true;
							}
						}
						if (storageRole && logRole) {
							foundLogAndStorage = true;
						}
					}
					if (process.has("messages")) {
						StatusArray processMessagesArr = process.last().get_array();
						if (processMessagesArr.size()) {
							for (StatusObjectReader msg : processMessagesArr) {
								std::string desc;
								std::string addr;
								if (msg.get("description", desc) && desc == "Unable to update cluster file." &&
								    process.get("address", addr)) {
									messagesAddrs.push_back(addr);
								}
							}
						}
					}
				}
				if (messagesAddrs.size()) {
					outputString += format("\n\n%d FoundationDB processes reported unable to update cluster file:",
					                       messagesAddrs.size());
					for (auto msg : messagesAddrs) {
						outputString += "\n  " + msg;
					}
				}
			} catch (std::runtime_error&) {
				outputString = outputStringCache;
				outputString += "\n  Unable to retrieve workload status";
			}

			// Backup and DR section
			outputString += "\n\nBackup and DR:";

			std::map<std::string, std::string> backupTags;
			getBackupDRTags(statusObjCluster, "backup", backupTags);

			std::map<std::string, std::string> drPrimaryTags;
			getBackupDRTags(statusObjCluster, "dr_backup", drPrimaryTags);

			std::map<std::string, std::string> drSecondaryTags;
			getBackupDRTags(statusObjCluster, "dr_backup_dest", drSecondaryTags);

			outputString += format("\n  Running backups        - %d", backupTags.size());
			outputString += format("\n  Running DRs            - ");

			if (drPrimaryTags.size() == 0 && drSecondaryTags.size() == 0) {
				outputString += format("%d", 0);
			} else {
				if (drPrimaryTags.size() > 0) {
					outputString += format("%d as primary", drPrimaryTags.size());
					if (drSecondaryTags.size() > 0) {
						outputString += ", ";
					}
				}
				if (drSecondaryTags.size() > 0) {
					outputString += format("%d as secondary", drSecondaryTags.size());
				}
			}

			// status details
			if (level == StatusClient::DETAILED) {
				outputString += logBackupDR("Running backup tags", backupTags);
				outputString += logBackupDR("Running DR tags (as primary)", drPrimaryTags);
				outputString += logBackupDR("Running DR tags (as secondary)", drSecondaryTags);

				outputString += "\n\nProcess performance details:";
				outputStringCache = outputString;
				try {
					// constructs process performance details output
					std::map<NetworkAddress, std::string> workerDetails;
					for (auto proc : processesMap.obj()) {
						StatusObjectReader procObj(proc.second);
						std::string address;
						procObj.get("address", address);

						std::string line;

						NetworkAddress parsedAddress;
						try {
							parsedAddress = NetworkAddress::parse(address);
						} catch (Error&) {
							// Groups all invalid IP address/port pair in the end of this detail group.
							line = format("  %-22s (invalid IP address or port)", address.c_str());
							IPAddress::IPAddressStore maxIp;
							for (int i = 0; i < maxIp.size(); ++i) {
								maxIp[i] = std::numeric_limits<std::remove_reference<decltype(maxIp[0])>::type>::max();
							}
							std::string& lastline =
							    workerDetails[NetworkAddress(IPAddress(maxIp), std::numeric_limits<uint16_t>::max())];
							if (!lastline.empty())
								lastline.append("\n");
							lastline += line;
							continue;
						}

						try {
							double tx = -1, rx = -1, mCPUUtil = -1;
							int64_t processTotalSize;

							// Get the machine for this process
							// StatusObjectReader mach = machinesMap[procObj["machine_id"].get_str()];
							StatusObjectReader mach;
							if (machinesMap.get(procObj["machine_id"].get_str(), mach, false)) {
								StatusObjectReader machCPU;
								if (mach.get("cpu", machCPU)) {

									machCPU.get("logical_core_utilization", mCPUUtil);

									StatusObjectReader network;
									if (mach.get("network", network)) {
										network.get("megabits_sent.hz", tx);
										network.get("megabits_received.hz", rx);
									}
								}
							}

							procObj.get("memory.used_bytes", processTotalSize);

							StatusObjectReader procCPUObj;
							procObj.get("cpu", procCPUObj);

							line = format("  %-22s (", address.c_str());

							double usageCores;
							if (procCPUObj.get("usage_cores", usageCores))
								line += format("%3.0f%% cpu;", usageCores * 100);

							line += mCPUUtil != -1 ? format("%3.0f%% machine;", mCPUUtil * 100) : "";
							line += std::min(tx, rx) != -1 ? format("%6.3f Gbps;", std::max(tx, rx) / 1000.0) : "";

							double diskBusy;
							if (procObj.get("disk.busy", diskBusy))
								line += format("%3.0f%% disk IO;", 100.0 * diskBusy);

							line += processTotalSize != -1
							            ? format("%4.1f GB", processTotalSize / (1024.0 * 1024 * 1024))
							            : "";

							double availableBytes;
							if (procObj.get("memory.available_bytes", availableBytes))
								line += format(" / %3.1f GB RAM  )", availableBytes / (1024.0 * 1024 * 1024));
							else
								line += "  )";

							if (procObj.has("messages")) {
								for (StatusObjectReader message : procObj.last().get_array()) {
									std::string desc;
									if (message.get("description", desc)) {
										if (message.has("type")) {
											line += "\n    Last logged error: " + desc;
										} else {
											line += "\n    " + desc;
										}
									}
								}
							}

							workerDetails[parsedAddress] = line;
						}

						catch (std::runtime_error&) {
							std::string noMetrics = format("  %-22s (no metrics available)", address.c_str());
							workerDetails[parsedAddress] = noMetrics;
						}
					}
					for (auto w : workerDetails)
						outputString += "\n" + format("%s", w.second.c_str());
				} catch (std::runtime_error&) {
					outputString = outputStringCache;
					outputString += "\n  Unable to retrieve process performance details";
				}

				if (!printedCoordinators) {
					printedCoordinators = true;
					outputString += "\n\nCoordination servers:";
					outputString += getCoordinatorsInfoString(statusObj);
				}
			}

			// client time
			std::string clientTime = getDateInfoString(statusObjClient, "timestamp");
			if (clientTime != "") {
				outputString += "\n\nClient time: " + clientTime;
			}

			if (processesMap.obj().size() > 1 && isOldMemory) {
				outputString += "\n\nWARNING: type `configure memory' to switch to a safer method of persisting data "
				                "on the transaction logs.";
			}
			if (processesMap.obj().size() > 9 && foundLogAndStorage) {
				outputString +=
				    "\n\nWARNING: A single process is both a transaction log and a storage server.\n  For best "
				    "performance use dedicated disks for the transaction logs by setting process classes.";
			}

			if (statusObjCluster.has("data_distribution_disabled")) {
				outputString += "\n\nWARNING: Data distribution is off.";
			} else {
				if (statusObjCluster.has("data_distribution_disabled_for_ss_failures")) {
					outputString += "\n\nWARNING: Data distribution is currently turned on but disabled for all "
					                "storage server failures.";
				}
				if (statusObjCluster.has("data_distribution_disabled_for_rebalance")) {
					outputString += "\n\nWARNING: Data distribution is currently turned on but shard size balancing is "
					                "currently disabled.";
				}
			}

			printf("%s\n", outputString.c_str());
		}

		// status minimal
		else if (level == StatusClient::MINIMAL) {
			// Checking for field exsistence is not necessary here because if a field is missing there is no additional
			// information that we would be able to display if we continued execution. Instead, any missing fields will
			// throw and the catch will display the proper message.
			try {
				// If any of these throw, can't get status because the result makes no sense.
				StatusObjectReader statusObjClient = statusObj["client"].get_obj();
				StatusObjectReader statusObjClientDatabaseStatus = statusObjClient["database_status"].get_obj();

				bool available = statusObjClientDatabaseStatus["available"].get_bool();

				// Database unavailable
				if (!available) {
					printf("%s", "The database is unavailable; type `status' for more information.\n");
				} else {
					try {
						bool healthy = statusObjClientDatabaseStatus["healthy"].get_bool();

						// Database available without issues
						if (healthy) {
							if (displayDatabaseAvailable) {
								printf("The database is available.\n");
							}
						} else { // Database running but with issues
							printf("The database is available, but has issues (type 'status' for more information).\n");
						}
					} catch (std::runtime_error&) {
						printf("The database is available, but has issues (type 'status' for more information).\n");
					}
				}

				bool upToDate;
				if (!statusObjClient.get("cluster_file.up_to_date", upToDate) || !upToDate) {
					fprintf(stderr,
					        "WARNING: The cluster file is not up to date. Type 'status' for more information.\n");
				}
			} catch (std::runtime_error&) {
				printf("Unable to determine database state, type 'status' for more information.\n");
			}

		}

		// status JSON
		else if (level == StatusClient::JSON) {
			printf("%s\n",
			       json_spirit::write_string(json_spirit::mValue(statusObj.obj()),
			                                 json_spirit::Output_options::pretty_print)
			           .c_str());
		}
	} catch (Error&) {
		if (hideErrorMessages)
			return;
		if (level == StatusClient::MINIMAL) {
			printf("Unable to determine database state, type 'status' for more information.\n");
		} else if (level == StatusClient::JSON) {
			printf("Could not retrieve status json.\n\n");
		} else {
			printf("Could not retrieve status, type 'status json' for more information.\n");
		}
	}
}

// "db" is the handler to the multiversion database
// localDb is the native Database object
// localDb is rarely needed except the "db" has not established a connection to the cluster where the operation will
// return Never as we expect status command to always return, we use "localDb" to return the default result
ACTOR Future<bool> statusCommandActor(Reference<IDatabase> db,
                                      Database localDb,
                                      std::vector<StringRef> tokens,
                                      bool isExecMode) {

	state StatusClient::StatusLevel level;
	if (tokens.size() == 1)
		level = StatusClient::NORMAL;
	else if (tokens.size() == 2 && tokencmp(tokens[1], "details"))
		level = StatusClient::DETAILED;
	else if (tokens.size() == 2 && tokencmp(tokens[1], "minimal"))
		level = StatusClient::MINIMAL;
	else if (tokens.size() == 2 && tokencmp(tokens[1], "json"))
		level = StatusClient::JSON;
	else {
		printUsage(tokens[0]);
		return false;
	}

	state StatusObject s;
	state Reference<ITransaction> tr = db->createTransaction();
	if (!tr->isValid()) {
		StatusObject _s = wait(StatusClient::statusFetcher(localDb));
		s = _s;
	} else {
		state ThreadFuture<Optional<Value>> statusValueF = tr->get(LiteralStringRef("\xff\xff/status/json"));
		Optional<Value> statusValue = wait(safeThreadFutureToFuture(statusValueF));
		if (!statusValue.present()) {
			fprintf(stderr, "ERROR: Failed to get status json from the cluster\n");
		}
		json_spirit::mValue mv;
		json_spirit::read_string(statusValue.get().toString(), mv);
		s = StatusObject(mv.get_obj());
	}

	if (!isExecMode)
		printf("\n");
	printStatus(s, level);
	if (!isExecMode)
		printf("\n");
	return true;
}

CommandFactory statusFactory(
    "status",
    CommandHelp("status [minimal|details|json]",
                "get the status of a FoundationDB cluster",
                "If the cluster is down, this command will print a diagnostic which may be useful in figuring out "
                "what is wrong. If the cluster is running, this command will print cluster "
                "statistics.\n\nSpecifying `minimal' will provide a minimal description of the status of your "
                "database.\n\nSpecifying `details' will provide load information for individual "
                "workers.\n\nSpecifying `json' will provide status information in a machine readable JSON format."));
} // namespace fdb_cli
