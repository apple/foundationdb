/*
 * MonitorLeader.actor.cpp
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

#include "fdbclient/ClusterConnectionMemoryRecord.h"
#include "fdbclient/MonitorLeader.h"
#include "fdbclient/CoordinationInterface.h"
#include "fdbclient/NativeAPI.actor.h"
#include "flow/ActorCollection.h"
#include "flow/UnitTest.h"
#include "fdbrpc/genericactors.actor.h"
#include "flow/Platform.h"
#include "flow/actorcompiler.h" // has to be last include

namespace {

std::string trim(std::string const& connectionString) {
	// Strip out whitespace
	// Strip out characters between a # and a newline
	std::string trimmed;
	auto end = connectionString.end();
	for (auto c = connectionString.begin(); c != end; ++c) {
		if (*c == '#') {
			++c;
			while (c != end && *c != '\n' && *c != '\r')
				++c;
			if (c == end)
				break;
		} else if (*c != ' ' && *c != '\n' && *c != '\r' && *c != '\t')
			trimmed += *c;
	}
	return trimmed;
}

} // namespace

FDB_DEFINE_BOOLEAN_PARAM(ConnectionStringNeedsPersisted);

// Returns the connection string currently held in this object. This may not match the stored record if it hasn't
// been persisted or if the persistent storage for the record has been modified externally.
ClusterConnectionString& IClusterConnectionRecord::getConnectionString() {
	return cs;
}

Future<bool> IClusterConnectionRecord::upToDate() {
	ClusterConnectionString temp;
	return upToDate(temp);
}

void IClusterConnectionRecord::notifyConnected() {
	if (connectionStringNeedsPersisted) {
		this->persist();
	}
}

bool IClusterConnectionRecord::needsToBePersisted() const {
	return connectionStringNeedsPersisted;
}

void IClusterConnectionRecord::setPersisted() {
	connectionStringNeedsPersisted = false;
}

ClusterConnectionString::ConnectionStringStatus IClusterConnectionRecord::connectionStringStatus() const {
	return cs.status;
}

Future<Void> IClusterConnectionRecord::resolveHostnames() {
	return cs.resolveHostnames();
}

void IClusterConnectionRecord::resolveHostnamesBlocking() {
	cs.resolveHostnamesBlocking();
}

std::string ClusterConnectionString::getErrorString(std::string const& source, Error const& e) {
	if (e.code() == error_code_connection_string_invalid) {
		return format("Invalid connection string `%s: %d %s", source.c_str(), e.code(), e.what());
	} else {
		return format("Unexpected error parsing connection string `%s: %d %s", source.c_str(), e.code(), e.what());
	}
}

ACTOR Future<Void> resolveHostnamesImpl(ClusterConnectionString* self) {
	loop {
		if (self->status == ClusterConnectionString::UNRESOLVED) {
			self->status = ClusterConnectionString::RESOLVING;
			std::vector<Future<Void>> fs;
			for (auto const& hostname : self->hostnames) {
				fs.push_back(map(INetworkConnections::net()->resolveTCPEndpoint(hostname.host, hostname.service),
				                 [=](std::vector<NetworkAddress> const& addresses) -> Void {
					                 NetworkAddress address =
					                     addresses[deterministicRandom()->randomInt(0, addresses.size())];
					                 address.flags = 0; // Reset the parsed address to public
					                 address.fromHostname = NetworkAddressFromHostname::True;
					                 if (hostname.isTLS) {
						                 address.flags |= NetworkAddress::FLAG_TLS;
					                 }
					                 self->addResolved(hostname, address);
					                 return Void();
				                 }));
			}
			wait(waitForAll(fs));
			std::sort(self->coords.begin(), self->coords.end());
			if (std::unique(self->coords.begin(), self->coords.end()) != self->coords.end()) {
				self->status = ClusterConnectionString::UNRESOLVED;
				self->resolveFinish.trigger();
				throw connection_string_invalid();
			}
			self->status = ClusterConnectionString::RESOLVED;
			self->resolveFinish.trigger();
			break;
		} else if (self->status == ClusterConnectionString::RESOLVING) {
			wait(self->resolveFinish.onTrigger());
			if (self->status == ClusterConnectionString::RESOLVED) {
				break;
			}
			// Otherwise, this means other threads failed on resolve, so here we go back to the loop and try to resolve
			// again.
		} else {
			// status is RESOLVED, nothing to do.
			break;
		}
	}
	return Void();
}

Future<Void> ClusterConnectionString::resolveHostnames() {
	return resolveHostnamesImpl(this);
}

void ClusterConnectionString::resolveHostnamesBlocking() {
	if (status != RESOLVED) {
		status = RESOLVING;
		for (auto const& hostname : hostnames) {
			std::vector<NetworkAddress> addresses =
			    INetworkConnections::net()->resolveTCPEndpointBlocking(hostname.host, hostname.service);
			NetworkAddress address = addresses[deterministicRandom()->randomInt(0, addresses.size())];
			address.flags = 0; // Reset the parsed address to public
			address.fromHostname = NetworkAddressFromHostname::True;
			if (hostname.isTLS) {
				address.flags |= NetworkAddress::FLAG_TLS;
			}
			addResolved(hostname, address);
		}
		std::sort(coords.begin(), coords.end());
		if (std::unique(coords.begin(), coords.end()) != coords.end()) {
			status = UNRESOLVED;
			throw connection_string_invalid();
		}
		status = RESOLVED;
	}
}

void ClusterConnectionString::resetToUnresolved() {
	if (status == RESOLVED && hostnames.size() > 0) {
		coords.clear();
		hostnames.clear();
		networkAddressToHostname.clear();
		status = UNRESOLVED;
		parseConnString();
	}
}

void ClusterConnectionString::resetConnectionString() {
	connectionString = toString();
}

void ClusterConnectionString::parseConnString() {
	// Split on '@' into key@addrs
	int pAt = connectionString.find_first_of('@');
	if (pAt == connectionString.npos) {
		throw connection_string_invalid();
	}
	std::string key = connectionString.substr(0, pAt);
	std::string addrs = connectionString.substr(pAt + 1);

	parseKey(key);
	std::string curAddr;
	for (int p = 0; p <= addrs.size();) {
		int pComma = addrs.find_first_of(',', p);
		if (pComma == addrs.npos)
			pComma = addrs.size();
		curAddr = addrs.substr(p, pComma - p);
		if (Hostname::isHostname(curAddr)) {
			hostnames.push_back(Hostname::parse(curAddr));
		} else {
			coords.push_back(NetworkAddress::parse(curAddr));
		}
		p = pComma + 1;
	}
	if (hostnames.size() > 0) {
		status = UNRESOLVED;
	}
	ASSERT((coords.size() + hostnames.size()) > 0);

	std::sort(coords.begin(), coords.end());
	// Check that there are no duplicate addresses
	if (std::unique(coords.begin(), coords.end()) != coords.end()) {
		throw connection_string_invalid();
	}
}

ClusterConnectionString::ClusterConnectionString(const std::string& connStr) {
	connectionString = trim(connStr);
	parseConnString();
}

TEST_CASE("/fdbclient/MonitorLeader/parseConnectionString/addresses") {
	std::string input;

	{
		input = "asdf:2345@1.1.1.1:345";
		ClusterConnectionString cs(input);
		ASSERT(input == cs.toString());
	}

	{
		input = "0xxdeadbeef:100100100@1.1.1.1:34534,5.1.5.3:23443";
		ClusterConnectionString cs(input);
		ASSERT(input == cs.toString());
	}

	{
		input = "0xxdeadbeef:100100100@1.1.1.1:34534,5.1.5.3:23443";
		std::string commented("#start of comment\n");
		commented += input;
		commented += "\n";
		commented += "# asdfasdf ##";

		ClusterConnectionString cs(commented);
		ASSERT(input == cs.toString());
	}

	{
		input = "0xxdeadbeef:100100100@[::1]:1234,[::1]:1235";
		std::string commented("#start of comment\n");
		commented += input;
		commented += "\n";
		commented += "# asdfasdf ##";

		ClusterConnectionString cs(commented);
		ASSERT(input == cs.toString());
	}

	{
		input = "0xxdeadbeef:100100100@[abcd:dcba::1]:1234,[abcd:dcba::abcd:1]:1234";
		std::string commented("#start of comment\n");
		commented += input;
		commented += "\n";
		commented += "# asdfasdf ##";

		ClusterConnectionString cs(commented);
		ASSERT(input == cs.toString());
	}

	return Void();
}

TEST_CASE("/fdbclient/MonitorLeader/parseConnectionString/hostnames") {
	std::string input;

	{
		input = "asdf:2345@localhost:1234";
		ClusterConnectionString cs(input);
		ASSERT(cs.status == ClusterConnectionString::UNRESOLVED);
		ASSERT(cs.hostnames.size() == 1);
		ASSERT(input == cs.toString());
	}

	{
		input = "0xxdeadbeef:100100100@localhost:34534,host-name:23443";
		ClusterConnectionString cs(input);
		ASSERT(cs.status == ClusterConnectionString::UNRESOLVED);
		ASSERT(cs.hostnames.size() == 2);
		ASSERT(input == cs.toString());
	}

	{
		input = "0xxdeadbeef:100100100@localhost:34534,host-name:23443";
		std::string commented("#start of comment\n");
		commented += input;
		commented += "\n";
		commented += "# asdfasdf ##";

		ClusterConnectionString cs(commented);
		ASSERT(cs.status == ClusterConnectionString::UNRESOLVED);
		ASSERT(cs.hostnames.size() == 2);
		ASSERT(input == cs.toString());
	}

	{
		input = "0xxdeadbeef:100100100@localhost:34534,host-name_part1.host-name_part2:1234:tls";
		std::string commented("#start of comment\n");
		commented += input;
		commented += "\n";
		commented += "# asdfasdf ##";

		ClusterConnectionString cs(commented);
		ASSERT(cs.status == ClusterConnectionString::UNRESOLVED);
		ASSERT(cs.hostnames.size() == 2);
		ASSERT(input == cs.toString());
	}

	return Void();
}

TEST_CASE("/fdbclient/MonitorLeader/ConnectionString") {
	state std::string connectionString = "TestCluster:0@localhost:1234,host-name:5678";
	std::string hn1 = "localhost", port1 = "1234";
	state std::string hn2 = "host-name";
	state std::string port2 = "5678";
	state std::vector<Hostname> hostnames;
	hostnames.push_back(Hostname::parse(hn1 + ":" + port1));
	hostnames.push_back(Hostname::parse(hn2 + ":" + port2));

	NetworkAddress address1 = NetworkAddress::parse("127.0.0.0:1234");
	NetworkAddress address2 = NetworkAddress::parse("127.0.0.1:5678");

	INetworkConnections::net()->addMockTCPEndpoint(hn1, port1, { address1 });
	INetworkConnections::net()->addMockTCPEndpoint(hn2, port2, { address2 });

	state ClusterConnectionString cs(hostnames, LiteralStringRef("TestCluster:0"));
	ASSERT(cs.status == ClusterConnectionString::UNRESOLVED);
	ASSERT(cs.hostnames.size() == 2);
	ASSERT(cs.coordinators().size() == 0);
	wait(cs.resolveHostnames());
	ASSERT(cs.status == ClusterConnectionString::RESOLVED);
	ASSERT(cs.hostnames.size() == 2);
	ASSERT(cs.coordinators().size() == 2);
	ASSERT(cs.toString() == connectionString);
	cs.resetToUnresolved();
	ASSERT(cs.status == ClusterConnectionString::UNRESOLVED);
	ASSERT(cs.hostnames.size() == 2);
	ASSERT(cs.coordinators().size() == 0);
	ASSERT(cs.toString() == connectionString);

	INetworkConnections::net()->removeMockTCPEndpoint(hn2, port2);
	NetworkAddress address3 = NetworkAddress::parse("127.0.0.0:5678");
	INetworkConnections::net()->addMockTCPEndpoint(hn2, port2, { address3 });

	try {
		wait(cs.resolveHostnames());
	} catch (Error& e) {
		ASSERT(e.code() == error_code_connection_string_invalid);
	}

	return Void();
}

TEST_CASE("/flow/FlatBuffers/LeaderInfo") {
	{
		LeaderInfo in;
		LeaderInfo out;
		in.forward = deterministicRandom()->coinflip();
		in.changeID = deterministicRandom()->randomUniqueID();
		{
			std::string rndString(deterministicRandom()->randomInt(10, 400), 'x');
			for (auto& c : rndString) {
				c = deterministicRandom()->randomAlphaNumeric();
			}
			in.serializedInfo = rndString;
		}
		ObjectWriter writer(IncludeVersion());
		writer.serialize(in);
		Standalone<StringRef> copy = writer.toStringRef();
		ArenaObjectReader reader(copy.arena(), copy, IncludeVersion());
		reader.deserialize(out);
		ASSERT(in.forward == out.forward);
		ASSERT(in.changeID == out.changeID);
		ASSERT(in.serializedInfo == out.serializedInfo);
	}
	LeaderInfo leaderInfo;
	leaderInfo.forward = deterministicRandom()->coinflip();
	leaderInfo.changeID = deterministicRandom()->randomUniqueID();
	{
		std::string rndString(deterministicRandom()->randomInt(10, 400), 'x');
		for (auto& c : rndString) {
			c = deterministicRandom()->randomAlphaNumeric();
		}
		leaderInfo.serializedInfo = rndString;
	}
	ErrorOr<EnsureTable<Optional<LeaderInfo>>> objIn(leaderInfo);
	ErrorOr<EnsureTable<Optional<LeaderInfo>>> objOut;
	Standalone<StringRef> copy;
	ObjectWriter writer(IncludeVersion());
	writer.serialize(objIn);
	copy = writer.toStringRef();
	ArenaObjectReader reader(copy.arena(), copy, IncludeVersion());
	reader.deserialize(objOut);

	ASSERT(!objOut.isError());
	ASSERT(objOut.get().asUnderlyingType().present());
	LeaderInfo outLeader = objOut.get().asUnderlyingType().get();
	ASSERT(outLeader.changeID == leaderInfo.changeID);
	ASSERT(outLeader.forward == leaderInfo.forward);
	ASSERT(outLeader.serializedInfo == leaderInfo.serializedInfo);
	return Void();
}

TEST_CASE("/fdbclient/MonitorLeader/parseConnectionString/fuzz") {
	// For a static connection string, add in fuzzed comments and whitespace
	// SOMEDAY: create a series of random connection strings, rather than the one we started with
	std::string connectionString = "0xxdeadbeef:100100100@1.1.1.1:34534,5.1.5.3:23443";
	for (int i = 0; i < 10000; i++) {
		std::string output("");
		auto c = connectionString.begin();
		while (c != connectionString.end()) {
			if (deterministicRandom()->random01() < 0.1) // Add whitespace character
				output += deterministicRandom()->randomChoice(LiteralStringRef(" \t\n\r"));
			if (deterministicRandom()->random01() < 0.5) { // Add one of the input characters
				output += *c;
				++c;
			}
			if (deterministicRandom()->random01() < 0.1) { // Add a comment block
				output += "#";
				int charCount = deterministicRandom()->randomInt(0, 20);
				for (int i = 0; i < charCount; i++) {
					output += deterministicRandom()->randomChoice(LiteralStringRef("asdfzxcv123345:!@#$#$&()<\"\' \t"));
				}
				output += deterministicRandom()->randomChoice(LiteralStringRef("\n\r"));
			}
		}

		ClusterConnectionString cs(output);
		ASSERT(connectionString == cs.toString());
	}
	return Void();
}

ClusterConnectionString::ClusterConnectionString(const std::vector<NetworkAddress>& servers, Key key)
  : status(RESOLVED), coords(servers) {
	std::string keyString = key.toString();
	parseKey(keyString);
	resetConnectionString();
}

ClusterConnectionString::ClusterConnectionString(const std::vector<Hostname>& hosts, Key key)
  : status(UNRESOLVED), hostnames(hosts) {
	std::string keyString = key.toString();
	parseKey(keyString);
	resetConnectionString();
}

void ClusterConnectionString::parseKey(const std::string& key) {
	// Check the structure of the given key, and fill in this->key and this->keyDesc

	// The key must contain one (and only one) : character
	int colon = key.find_first_of(':');
	if (colon == key.npos) {
		throw connection_string_invalid();
	}
	std::string desc = key.substr(0, colon);
	std::string id = key.substr(colon + 1);

	// Check that description contains only allowed characters (a-z, A-Z, 0-9, _)
	for (auto c = desc.begin(); c != desc.end(); ++c) {
		if (!(isalnum(*c) || *c == '_')) {
			throw connection_string_invalid();
		}
	}

	// Check that ID contains only allowed characters (a-z, A-Z, 0-9)
	for (auto c = id.begin(); c != id.end(); ++c) {
		if (!isalnum(*c)) {
			throw connection_string_invalid();
		}
	}

	this->key = StringRef(key);
	this->keyDesc = StringRef(desc);
}

std::string ClusterConnectionString::toString() const {
	std::string s = key.toString();
	s += '@';
	for (int i = 0; i < coords.size(); i++) {
		if (networkAddressToHostname.find(coords[i]) == networkAddressToHostname.end()) {
			if (s.find('@') != s.length() - 1) {
				s += ',';
			}
			s += coords[i].toString();
		}
	}
	for (auto const& host : hostnames) {
		if (s.find('@') != s.length() - 1) {
			s += ',';
		}
		s += host.toString();
	}
	return s;
}

ClientCoordinators::ClientCoordinators(Reference<IClusterConnectionRecord> ccr) : ccr(ccr) {
	ASSERT(ccr->connectionStringStatus() == ClusterConnectionString::RESOLVED);
	ClusterConnectionString cs = ccr->getConnectionString();
	for (auto s = cs.coordinators().begin(); s != cs.coordinators().end(); ++s)
		clientLeaderServers.push_back(ClientLeaderRegInterface(*s));
	clusterKey = cs.clusterKey();
}

ClientCoordinators::ClientCoordinators(Key clusterKey, std::vector<NetworkAddress> coordinators)
  : clusterKey(clusterKey) {
	for (const auto& coord : coordinators) {
		clientLeaderServers.push_back(ClientLeaderRegInterface(coord));
	}
	ccr = makeReference<ClusterConnectionMemoryRecord>(ClusterConnectionString(coordinators, clusterKey));
}

ClientLeaderRegInterface::ClientLeaderRegInterface(NetworkAddress remote)
  : getLeader(Endpoint::wellKnown({ remote }, WLTOKEN_CLIENTLEADERREG_GETLEADER)),
    openDatabase(Endpoint::wellKnown({ remote }, WLTOKEN_CLIENTLEADERREG_OPENDATABASE)),
    checkDescriptorMutable(Endpoint::wellKnown({ remote }, WLTOKEN_CLIENTLEADERREG_DESCRIPTOR_MUTABLE)) {}

ClientLeaderRegInterface::ClientLeaderRegInterface(INetwork* local) {
	getLeader.makeWellKnownEndpoint(WLTOKEN_CLIENTLEADERREG_GETLEADER, TaskPriority::Coordination);
	openDatabase.makeWellKnownEndpoint(WLTOKEN_CLIENTLEADERREG_OPENDATABASE, TaskPriority::Coordination);
	checkDescriptorMutable.makeWellKnownEndpoint(WLTOKEN_CLIENTLEADERREG_DESCRIPTOR_MUTABLE,
	                                             TaskPriority::Coordination);
}

// Nominee is the worker among all workers that are considered as leader by one coordinator
// This function contacts a coordinator coord to ask who is its nominee.
// Note: for coordinators whose NetworkAddress is parsed out of a hostname, a connection failure will cause this actor
// to throw `coordinators_changed()` error
ACTOR Future<Void> monitorNominee(Key key,
                                  ClientLeaderRegInterface coord,
                                  AsyncTrigger* nomineeChange,
                                  Optional<LeaderInfo>* info,
                                  Optional<Hostname> hostname = Optional<Hostname>()) {
	loop {
		state Optional<LeaderInfo> li;

		if (coord.getLeader.getEndpoint().getPrimaryAddress().fromHostname) {
			state ErrorOr<Optional<LeaderInfo>> rep =
			    wait(coord.getLeader.tryGetReply(GetLeaderRequest(key, info->present() ? info->get().changeID : UID()),
			                                     TaskPriority::CoordinationReply));
			if (rep.isError()) {
				// Connecting to nominee failed, most likely due to connection failed.
				TraceEvent("MonitorNomineeError")
				    .error(rep.getError())
				    .detail("Hostname", hostname.present() ? hostname.get().toString() : "UnknownHostname")
				    .detail("OldAddr", coord.getLeader.getEndpoint().getPrimaryAddress().toString());
				if (rep.getError().code() == error_code_request_maybe_delivered) {
					// Delay to prevent tight resolving loop due to outdated DNS cache
					wait(delay(CLIENT_KNOBS->COORDINATOR_HOSTNAME_RESOLVE_DELAY));
					throw coordinators_changed();
				} else {
					throw rep.getError();
				}
			} else if (rep.present()) {
				li = rep.get();
			}
		} else {
			Optional<LeaderInfo> tmp =
			    wait(retryBrokenPromise(coord.getLeader,
			                            GetLeaderRequest(key, info->present() ? info->get().changeID : UID()),
			                            TaskPriority::CoordinationReply));
			li = tmp;
		}

		wait(Future<Void>(Void())); // Make sure we weren't cancelled

		TraceEvent("GetLeaderReply")
		    .suppressFor(1.0)
		    .detail("Coordinator", coord.getLeader.getEndpoint().getPrimaryAddress())
		    .detail("Nominee", li.present() ? li.get().changeID : UID())
		    .detail("ClusterKey", key.printable());

		if (li != *info) {
			*info = li;
			nomineeChange->trigger();

			if (li.present() && li.get().forward)
				wait(Future<Void>(Never()));
		}
	}
}

// Also used in fdbserver/LeaderElection.actor.cpp!
// bool represents if the LeaderInfo is a majority answer or not.
// This function also masks the first 7 bits of changeId of the nominees and returns the Leader with masked changeId
Optional<std::pair<LeaderInfo, bool>> getLeader(const std::vector<Optional<LeaderInfo>>& nominees) {
	// If any coordinator says that the quorum is forwarded, then it is
	for (int i = 0; i < nominees.size(); i++)
		if (nominees[i].present() && nominees[i].get().forward)
			return std::pair<LeaderInfo, bool>(nominees[i].get(), true);

	std::vector<std::pair<UID, int>> maskedNominees;
	maskedNominees.reserve(nominees.size());
	for (int i = 0; i < nominees.size(); i++) {
		if (nominees[i].present()) {
			maskedNominees.emplace_back(
			    UID(nominees[i].get().changeID.first() & LeaderInfo::changeIDMask, nominees[i].get().changeID.second()),
			    i);
		}
	}

	if (!maskedNominees.size())
		return Optional<std::pair<LeaderInfo, bool>>();

	std::sort(maskedNominees.begin(),
	          maskedNominees.end(),
	          [](const std::pair<UID, int>& l, const std::pair<UID, int>& r) { return l.first < r.first; });

	int bestCount = 0;
	int bestIdx = 0;
	int currentIdx = 0;
	int curCount = 1;
	for (int i = 1; i < maskedNominees.size(); i++) {
		if (maskedNominees[currentIdx].first == maskedNominees[i].first) {
			curCount++;
		} else {
			if (curCount > bestCount) {
				bestIdx = currentIdx;
				bestCount = curCount;
			}
			currentIdx = i;
			curCount = 1;
		}
	}
	if (curCount > bestCount) {
		bestIdx = currentIdx;
		bestCount = curCount;
	}

	bool majority = bestCount >= nominees.size() / 2 + 1;
	return std::pair<LeaderInfo, bool>(nominees[maskedNominees[bestIdx].second].get(), majority);
}

// Leader is the process that will be elected by coordinators as the cluster controller
ACTOR Future<MonitorLeaderInfo> monitorLeaderOneGeneration(Reference<IClusterConnectionRecord> connRecord,
                                                           Reference<AsyncVar<Value>> outSerializedLeaderInfo,
                                                           MonitorLeaderInfo info) {
	loop {
		wait(connRecord->resolveHostnames());
		wait(info.intermediateConnRecord->resolveHostnames());
		state ClientCoordinators coordinators(info.intermediateConnRecord);
		state AsyncTrigger nomineeChange;
		state std::vector<Optional<LeaderInfo>> nominees;
		state Future<Void> allActors;

		nominees.resize(coordinators.clientLeaderServers.size());

		state std::vector<Future<Void>> actors;
		// Ask all coordinators if the worker is considered as a leader (leader nominee) by the coordinator.
		actors.reserve(coordinators.clientLeaderServers.size());
		for (int i = 0; i < coordinators.clientLeaderServers.size(); i++) {
			Optional<Hostname> hostname;
			auto r = connRecord->getConnectionString().networkAddressToHostname.find(
			    coordinators.clientLeaderServers[i].getLeader.getEndpoint().getPrimaryAddress());
			if (r != connRecord->getConnectionString().networkAddressToHostname.end()) {
				hostname = r->second;
			}
			actors.push_back(monitorNominee(
			    coordinators.clusterKey, coordinators.clientLeaderServers[i], &nomineeChange, &nominees[i], hostname));
		}
		allActors = waitForAll(actors);

		loop {
			Optional<std::pair<LeaderInfo, bool>> leader = getLeader(nominees);
			TraceEvent("MonitorLeaderChange")
			    .detail("NewLeader", leader.present() ? leader.get().first.changeID : UID(1, 1));
			if (leader.present()) {
				if (leader.get().first.forward) {
					TraceEvent("MonitorLeaderForwarding")
					    .detail("NewConnStr", leader.get().first.serializedInfo.toString())
					    .detail("OldConnStr", info.intermediateConnRecord->getConnectionString().toString())
					    .trackLatest("MonitorLeaderForwarding");
					info.intermediateConnRecord = connRecord->makeIntermediateRecord(
					    ClusterConnectionString(leader.get().first.serializedInfo.toString()));
					return info;
				}
				if (connRecord != info.intermediateConnRecord) {
					if (!info.hasConnected) {
						TraceEvent(SevWarnAlways, "IncorrectClusterFileContentsAtConnection")
						    .detail("ClusterFile", connRecord->toString())
						    .detail("StoredConnectionString", connRecord->getConnectionString().toString())
						    .detail("CurrentConnectionString",
						            info.intermediateConnRecord->getConnectionString().toString());
					}
					connRecord->setAndPersistConnectionString(info.intermediateConnRecord->getConnectionString());
					info.intermediateConnRecord = connRecord;
				}

				info.hasConnected = true;
				connRecord->notifyConnected();

				outSerializedLeaderInfo->set(leader.get().first.serializedInfo);
			}
			try {
				wait(nomineeChange.onTrigger() || allActors);
			} catch (Error& e) {
				if (e.code() == error_code_coordinators_changed) {
					TraceEvent("MonitorLeaderCoordinatorsChanged").suppressFor(1.0);
					connRecord->getConnectionString().resetToUnresolved();
					break;
				} else {
					throw e;
				}
			}
		}
	}
}

ACTOR Future<Void> monitorLeaderInternal(Reference<IClusterConnectionRecord> connRecord,
                                         Reference<AsyncVar<Value>> outSerializedLeaderInfo) {
	state MonitorLeaderInfo info(connRecord);
	loop {
		MonitorLeaderInfo _info = wait(monitorLeaderOneGeneration(connRecord, outSerializedLeaderInfo, info));
		info = _info;
	}
}

ACTOR Future<Void> asyncDeserializeClusterInterface(Reference<AsyncVar<Value>> serializedInfo,
                                                    Reference<AsyncVar<Optional<ClusterInterface>>> outKnownLeader) {
	state Reference<AsyncVar<Optional<ClusterControllerClientInterface>>> knownLeader(
	    new AsyncVar<Optional<ClusterControllerClientInterface>>{});
	state Future<Void> deserializer = asyncDeserialize(serializedInfo, knownLeader);
	loop {
		choose {
			when(wait(deserializer)) { UNSTOPPABLE_ASSERT(false); }
			when(wait(knownLeader->onChange())) {
				if (knownLeader->get().present()) {
					outKnownLeader->set(knownLeader->get().get().clientInterface);
				} else {
					outKnownLeader->set(Optional<ClusterInterface>{});
				}
			}
		}
	}
}

struct ClientStatusStats {
	int count;
	std::vector<std::pair<NetworkAddress, Key>> examples;

	ClientStatusStats() : count(0) { examples.reserve(CLIENT_KNOBS->CLIENT_EXAMPLE_AMOUNT); }
};

OpenDatabaseRequest ClientData::getRequest() {
	OpenDatabaseRequest req;

	std::map<StringRef, ClientStatusStats> issueMap;
	std::map<ClientVersionRef, ClientStatusStats> versionMap;
	std::map<StringRef, ClientStatusStats> maxProtocolMap;
	int clientCount = 0;

	// SOMEDAY: add a yield in this loop
	for (auto& ci : clientStatusInfoMap) {
		for (auto& it : ci.second.issues) {
			auto& entry = issueMap[it];
			entry.count++;
			if (entry.examples.size() < CLIENT_KNOBS->CLIENT_EXAMPLE_AMOUNT) {
				entry.examples.emplace_back(ci.first, ci.second.traceLogGroup);
			}
		}
		if (ci.second.versions.size()) {
			clientCount++;
			StringRef maxProtocol;
			for (auto& it : ci.second.versions) {
				maxProtocol = std::max(maxProtocol, it.protocolVersion);
				auto& entry = versionMap[it];
				entry.count++;
				if (entry.examples.size() < CLIENT_KNOBS->CLIENT_EXAMPLE_AMOUNT) {
					entry.examples.emplace_back(ci.first, ci.second.traceLogGroup);
				}
			}
			auto& maxEntry = maxProtocolMap[maxProtocol];
			maxEntry.count++;
			if (maxEntry.examples.size() < CLIENT_KNOBS->CLIENT_EXAMPLE_AMOUNT) {
				maxEntry.examples.emplace_back(ci.first, ci.second.traceLogGroup);
			}
		} else {
			auto& entry = versionMap[ClientVersionRef()];
			entry.count++;
			if (entry.examples.size() < CLIENT_KNOBS->CLIENT_EXAMPLE_AMOUNT) {
				entry.examples.emplace_back(ci.first, ci.second.traceLogGroup);
			}
		}
	}

	req.issues.reserve(issueMap.size());
	for (auto& it : issueMap) {
		req.issues.push_back(ItemWithExamples<Key>(it.first, it.second.count, it.second.examples));
	}
	req.supportedVersions.reserve(versionMap.size());
	for (auto& it : versionMap) {
		req.supportedVersions.push_back(
		    ItemWithExamples<Standalone<ClientVersionRef>>(it.first, it.second.count, it.second.examples));
	}
	req.maxProtocolSupported.reserve(maxProtocolMap.size());
	for (auto& it : maxProtocolMap) {
		req.maxProtocolSupported.push_back(ItemWithExamples<Key>(it.first, it.second.count, it.second.examples));
	}
	req.clientCount = clientCount;

	return req;
}

ACTOR Future<Void> getClientInfoFromLeader(Reference<AsyncVar<Optional<ClusterControllerClientInterface>>> knownLeader,
                                           ClientData* clientData) {
	while (!knownLeader->get().present()) {
		wait(knownLeader->onChange());
	}

	state double lastRequestTime = now();
	state OpenDatabaseRequest req = clientData->getRequest();

	loop {
		if (now() - lastRequestTime > CLIENT_KNOBS->MAX_CLIENT_STATUS_AGE) {
			lastRequestTime = now();
			req = clientData->getRequest();
		} else {
			resetReply(req);
		}
		req.knownClientInfoID = clientData->clientInfo->get().read().id;
		choose {
			when(ClientDBInfo ni =
			         wait(brokenPromiseToNever(knownLeader->get().get().clientInterface.openDatabase.getReply(req)))) {
				TraceEvent("GetClientInfoFromLeaderGotClientInfo", knownLeader->get().get().clientInterface.id())
				    .detail("CommitProxy0", ni.commitProxies.size() ? ni.commitProxies[0].address().toString() : "")
				    .detail("GrvProxy0", ni.grvProxies.size() ? ni.grvProxies[0].address().toString() : "")
				    .detail("ClientID", ni.id);
				clientData->clientInfo->set(CachedSerialization<ClientDBInfo>(ni));
			}
			when(wait(knownLeader->onChange())) {}
		}
	}
}

ACTOR Future<Void> monitorLeaderAndGetClientInfo(Key clusterKey,
                                                 std::vector<NetworkAddress> coordinators,
                                                 ClientData* clientData,
                                                 Reference<AsyncVar<Optional<LeaderInfo>>> leaderInfo,
                                                 Reference<AsyncVar<Void>> coordinatorsChanged) {
	state std::vector<ClientLeaderRegInterface> clientLeaderServers;
	state AsyncTrigger nomineeChange;
	state std::vector<Optional<LeaderInfo>> nominees;
	state Future<Void> allActors;
	state Reference<AsyncVar<Optional<ClusterControllerClientInterface>>> knownLeader(
	    new AsyncVar<Optional<ClusterControllerClientInterface>>{});

	for (auto s = coordinators.begin(); s != coordinators.end(); ++s) {
		clientLeaderServers.push_back(ClientLeaderRegInterface(*s));
	}

	nominees.resize(clientLeaderServers.size());

	std::vector<Future<Void>> actors;
	// Ask all coordinators if the worker is considered as a leader (leader nominee) by the coordinator.
	actors.reserve(clientLeaderServers.size());
	for (int i = 0; i < clientLeaderServers.size(); i++) {
		actors.push_back(monitorNominee(clusterKey, clientLeaderServers[i], &nomineeChange, &nominees[i]));
	}
	actors.push_back(getClientInfoFromLeader(knownLeader, clientData));
	allActors = waitForAll(actors);

	loop {
		Optional<std::pair<LeaderInfo, bool>> leader = getLeader(nominees);
		TraceEvent("MonitorLeaderAndGetClientInfoLeaderChange")
		    .detail("NewLeader", leader.present() ? leader.get().first.changeID : UID(1, 1))
		    .detail("Key", clusterKey.printable());
		if (leader.present()) {
			if (leader.get().first.forward) {
				ClientDBInfo outInfo;
				outInfo.id = deterministicRandom()->randomUniqueID();
				outInfo.forward = leader.get().first.serializedInfo;
				clientData->clientInfo->set(CachedSerialization<ClientDBInfo>(outInfo));
				leaderInfo->set(leader.get().first);
				TraceEvent("MonitorLeaderAndGetClientInfoForwarding")
				    .detail("NewConnStr", leader.get().first.serializedInfo.toString());
				return Void();
			}

			if (leader.get().first.serializedInfo.size()) {
				ObjectReader reader(leader.get().first.serializedInfo.begin(), IncludeVersion());
				ClusterControllerClientInterface res;
				reader.deserialize(res);
				knownLeader->set(res);
				leaderInfo->set(leader.get().first);
			}
		}
		try {
			wait(nomineeChange.onTrigger() || allActors);
		} catch (Error& e) {
			if (e.code() == error_code_coordinators_changed) {
				coordinatorsChanged->trigger();
			}
			throw e;
		}
	}
}

void shrinkProxyList(ClientDBInfo& ni,
                     std::vector<UID>& lastCommitProxyUIDs,
                     std::vector<CommitProxyInterface>& lastCommitProxies,
                     std::vector<UID>& lastGrvProxyUIDs,
                     std::vector<GrvProxyInterface>& lastGrvProxies) {
	if (ni.commitProxies.size() > CLIENT_KNOBS->MAX_COMMIT_PROXY_CONNECTIONS) {
		std::vector<UID> commitProxyUIDs;
		for (auto& commitProxy : ni.commitProxies) {
			commitProxyUIDs.push_back(commitProxy.id());
		}
		if (commitProxyUIDs != lastCommitProxyUIDs) {
			lastCommitProxyUIDs.swap(commitProxyUIDs);
			lastCommitProxies = ni.commitProxies;
			deterministicRandom()->randomShuffle(lastCommitProxies);
			lastCommitProxies.resize(CLIENT_KNOBS->MAX_COMMIT_PROXY_CONNECTIONS);
			for (int i = 0; i < lastCommitProxies.size(); i++) {
				TraceEvent("ConnectedCommitProxy").detail("CommitProxy", lastCommitProxies[i].id());
			}
		}
		ni.firstCommitProxy = ni.commitProxies[0];
		ni.commitProxies = lastCommitProxies;
	}
	if (ni.grvProxies.size() > CLIENT_KNOBS->MAX_GRV_PROXY_CONNECTIONS) {
		std::vector<UID> grvProxyUIDs;
		for (auto& grvProxy : ni.grvProxies) {
			grvProxyUIDs.push_back(grvProxy.id());
		}
		if (grvProxyUIDs != lastGrvProxyUIDs) {
			lastGrvProxyUIDs.swap(grvProxyUIDs);
			lastGrvProxies = ni.grvProxies;
			deterministicRandom()->randomShuffle(lastGrvProxies);
			lastGrvProxies.resize(CLIENT_KNOBS->MAX_GRV_PROXY_CONNECTIONS);
			for (int i = 0; i < lastGrvProxies.size(); i++) {
				TraceEvent("ConnectedGrvProxy").detail("GrvProxy", lastGrvProxies[i].id());
			}
		}
		ni.grvProxies = lastGrvProxies;
	}
}

ACTOR Future<MonitorLeaderInfo> monitorProxiesOneGeneration(
    Reference<IClusterConnectionRecord> connRecord,
    Reference<AsyncVar<ClientDBInfo>> clientInfo,
    Reference<AsyncVar<Optional<ClientLeaderRegInterface>>> coordinator,
    MonitorLeaderInfo info,
    Reference<ReferencedObject<Standalone<VectorRef<ClientVersionRef>>>> supportedVersions,
    Key traceLogGroup) {
	state ClusterConnectionString cs = info.intermediateConnRecord->getConnectionString();
	state std::vector<NetworkAddress> addrs = cs.coordinators();
	state int index = 0;
	state int successIndex = 0;
	state Optional<double> incorrectTime;
	state std::vector<UID> lastCommitProxyUIDs;
	state std::vector<CommitProxyInterface> lastCommitProxies;
	state std::vector<UID> lastGrvProxyUIDs;
	state std::vector<GrvProxyInterface> lastGrvProxies;

	deterministicRandom()->randomShuffle(addrs);
	loop {
		state ClientLeaderRegInterface clientLeaderServer(addrs[index]);
		state OpenDatabaseCoordRequest req;

		coordinator->set(clientLeaderServer);

		req.clusterKey = cs.clusterKey();
		req.coordinators = cs.coordinators();
		req.knownClientInfoID = clientInfo->get().id;
		req.supportedVersions = supportedVersions->get();
		req.traceLogGroup = traceLogGroup;

		state ClusterConnectionString storedConnectionString;
		if (connRecord) {
			bool upToDate = wait(connRecord->upToDate(storedConnectionString));
			if (!upToDate) {
				req.issues.push_back_deep(req.issues.arena(), LiteralStringRef("incorrect_cluster_file_contents"));
				std::string connectionString = connRecord->getConnectionString().toString();
				if (!incorrectTime.present()) {
					incorrectTime = now();
				}

				// Don't log a SevWarnAlways initially to account for transient issues (e.g. someone else changing
				// the file right before us)
				TraceEvent(now() - incorrectTime.get() > 300 ? SevWarnAlways : SevWarn, "IncorrectClusterFileContents")
				    .detail("ClusterFile", connRecord->toString())
				    .detail("StoredConnectionString", storedConnectionString.toString())
				    .detail("CurrentConnectionString", connectionString);
			} else {
				incorrectTime = Optional<double>();
			}
		} else {
			incorrectTime = Optional<double>();
		}

		state ErrorOr<CachedSerialization<ClientDBInfo>> rep =
		    wait(clientLeaderServer.openDatabase.tryGetReply(req, TaskPriority::CoordinationReply));
		if (rep.present()) {
			if (rep.get().read().forward.present()) {
				TraceEvent("MonitorProxiesForwarding")
				    .detail("NewConnStr", rep.get().read().forward.get().toString())
				    .detail("OldConnStr", info.intermediateConnRecord->getConnectionString().toString());
				info.intermediateConnRecord = connRecord->makeIntermediateRecord(
				    ClusterConnectionString(rep.get().read().forward.get().toString()));
				return info;
			}
			if (connRecord != info.intermediateConnRecord) {
				if (!info.hasConnected) {
					TraceEvent(SevWarnAlways, "IncorrectClusterFileContentsAtConnection")
					    .detail("ClusterFile", connRecord->toString())
					    .detail("StoredConnectionString", connRecord->getConnectionString().toString())
					    .detail("CurrentConnectionString",
					            info.intermediateConnRecord->getConnectionString().toString());
				}
				connRecord->setAndPersistConnectionString(info.intermediateConnRecord->getConnectionString());
				info.intermediateConnRecord = connRecord;
			}

			info.hasConnected = true;
			connRecord->notifyConnected();

			auto& ni = rep.get().mutate();
			shrinkProxyList(ni, lastCommitProxyUIDs, lastCommitProxies, lastGrvProxyUIDs, lastGrvProxies);
			clientInfo->set(ni);
			successIndex = index;
		} else {
			TEST(rep.getError().code() == error_code_failed_to_progress); // Coordinator cant talk to cluster controller
			if (rep.getError().code() == error_code_coordinators_changed) {
				throw coordinators_changed();
			}
			index = (index + 1) % addrs.size();
			if (index == successIndex) {
				wait(delay(CLIENT_KNOBS->COORDINATOR_RECONNECTION_DELAY));
				// When the client fails talking to all coordinators, we throw coordinators_changed() and let the caller
				// re-resolve the connection string and retry.
				throw coordinators_changed();
			}
		}
	}
}

ACTOR Future<Void> monitorProxies(
    Reference<AsyncVar<Reference<IClusterConnectionRecord>>> connRecord,
    Reference<AsyncVar<ClientDBInfo>> clientInfo,
    Reference<AsyncVar<Optional<ClientLeaderRegInterface>>> coordinator,
    Reference<ReferencedObject<Standalone<VectorRef<ClientVersionRef>>>> supportedVersions,
    Key traceLogGroup) {
	wait(connRecord->get()->resolveHostnames());
	state MonitorLeaderInfo info(connRecord->get());
	loop {
		try {
			wait(info.intermediateConnRecord->resolveHostnames());
			choose {
				when(MonitorLeaderInfo _info = wait(monitorProxiesOneGeneration(
				         connRecord->get(), clientInfo, coordinator, info, supportedVersions, traceLogGroup))) {
					info = _info;
				}
				when(wait(connRecord->onChange())) {
					info.hasConnected = false;
					info.intermediateConnRecord = connRecord->get();
				}
			}
		} catch (Error& e) {
			if (e.code() == error_code_coordinators_changed) {
				TraceEvent("MonitorProxiesCoordinatorsChanged").suppressFor(1.0);
				info.intermediateConnRecord->getConnectionString().resetToUnresolved();
			} else {
				throw e;
			}
		}
	}
}
