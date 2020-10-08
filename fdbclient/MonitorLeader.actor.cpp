/*
 * MonitorLeader.actor.cpp
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

#include "fdbclient/MonitorLeader.h"
#include "fdbclient/CoordinationInterface.h"
#include "flow/ActorCollection.h"
#include "flow/UnitTest.h"
#include "fdbrpc/genericactors.actor.h"
#include "flow/Platform.h"
#include "flow/actorcompiler.h" // has to be last include

std::pair< std::string, bool > ClusterConnectionFile::lookupClusterFileName( std::string const& filename ) {
	if (filename.length())
		return std::make_pair(filename, false);

	std::string f;
	bool isDefaultFile = true;
	if (platform::getEnvironmentVar(CLUSTER_FILE_ENV_VAR_NAME, f)) {
		// If this is set but points to a file that does not
		// exist, we will not fallback to any other methods
		isDefaultFile = false;
	} else if (fileExists("fdb.cluster"))
		f = "fdb.cluster";
	else
		f = platform::getDefaultClusterFilePath();

	return std::make_pair( f, isDefaultFile );
}

std::string ClusterConnectionFile::getErrorString( std::pair<std::string, bool> const& resolvedClusterFile, Error const& e ) {
	bool isDefault = resolvedClusterFile.second;
	if( e.code() == error_code_connection_string_invalid ) {
		return format("Invalid cluster file `%s': %d %s", resolvedClusterFile.first.c_str(), e.code(), e.what());
	} else if( e.code() == error_code_no_cluster_file_found ) {
		if( isDefault )
			return format("Unable to read cluster file `./fdb.cluster' or `%s' and %s unset: %d %s",
						  platform::getDefaultClusterFilePath().c_str(), CLUSTER_FILE_ENV_VAR_NAME, e.code(), e.what());
		else
			return format("Unable to read cluster file `%s': %d %s", resolvedClusterFile.first.c_str(), e.code(), e.what());
	} else {
		return format("Unexpected error loading cluster file `%s': %d %s", resolvedClusterFile.first.c_str(), e.code(), e.what());
	}
}

ClusterConnectionFile::ClusterConnectionFile( std::string const& filename ) {
	if( !fileExists( filename ) ) {
		throw no_cluster_file_found();
	}

	cs = ClusterConnectionString(readFileBytes(filename, MAX_CLUSTER_FILE_BYTES));
	this->filename = filename;
	setConn = false;
}

ClusterConnectionFile::ClusterConnectionFile(std::string const& filename, ClusterConnectionString const& contents) {
	this->filename = filename;
	cs = contents;
	setConn = true;
}

ClusterConnectionString const& ClusterConnectionFile::getConnectionString() const {
	return cs;
}

void ClusterConnectionFile::notifyConnected() {
	if (setConn){
		this->writeFile();
	}
}

bool ClusterConnectionFile::fileContentsUpToDate() const {
	ClusterConnectionString temp;
	return fileContentsUpToDate(temp);
}

bool ClusterConnectionFile::fileContentsUpToDate(ClusterConnectionString &fileConnectionString) const {
	try {
		// the cluster file hasn't been created yet so there's nothing to check
		if (setConn)
			return true;

		ClusterConnectionFile temp( filename );
		fileConnectionString = temp.getConnectionString();
		return fileConnectionString.toString() == cs.toString();
	}
	catch (Error& e) {
		TraceEvent(SevWarnAlways, "ClusterFileError").error(e).detail("Filename", filename);
		return false; // Swallow the error and report that the file is out of date
	}
}

bool ClusterConnectionFile::writeFile() {
	setConn = false;
	if(filename.size()) {
		try {
			atomicReplace( filename, "# DO NOT EDIT!\n# This file is auto-generated, it is not to be edited by hand\n" + cs.toString().append("\n") );
			if(!fileContentsUpToDate()) {
				// This should only happen in rare scenarios where multiple processes are updating the same file to different values simultaneously
				// In that case, we don't have any guarantees about which file will ultimately be written
				TraceEvent(SevWarnAlways, "ClusterFileChangedAfterReplace").detail("Filename", filename).detail("ConnStr", cs.toString());
				return false;
			}

			return true;
		} catch( Error &e ) {
			TraceEvent(SevWarnAlways, "UnableToChangeConnectionFile").error(e).detail("Filename", filename).detail("ConnStr", cs.toString());
		}
	}

	return false;
}

void ClusterConnectionFile::setConnectionString( ClusterConnectionString const& conn ) {
	ASSERT( filename.size() );
	cs = conn;
	writeFile();
}

std::string ClusterConnectionString::getErrorString( std::string const& source, Error const& e ) {
	if( e.code() == error_code_connection_string_invalid ) {
		return format("Invalid connection string `%s: %d %s", source.c_str(), e.code(), e.what());
	}
	else {
		return format("Unexpected error parsing connection string `%s: %d %s", source.c_str(), e.code(), e.what());
	}
}

std::string trim( std::string const& connectionString ) {
	// Strip out whitespace
	// Strip out characters between a # and a newline
	std::string trimmed;
	auto end = connectionString.end();
	for(auto c=connectionString.begin(); c!=end; ++c) {
		if (*c == '#') {
			++c;
			while(c!=end && *c != '\n' && *c != '\r')
				++c;
			if(c == end)
				break;
		}
		else if (*c != ' ' && *c != '\n' && *c != '\r' && *c != '\t')
			trimmed += *c;
	}
	return trimmed;
}

ClusterConnectionString::ClusterConnectionString( std::string const& connectionString ) {
	auto trimmed = trim(connectionString);

	// Split on '@' into key@addrs
	int pAt = trimmed.find_first_of('@');
	if (pAt == trimmed.npos)
		throw connection_string_invalid();
	std::string key = trimmed.substr(0, pAt);
	std::string addrs = trimmed.substr(pAt+1);

	parseKey(key);

	coord = NetworkAddress::parseList(addrs);
	ASSERT( coord.size() > 0 );  // parseList() always returns at least one address if it doesn't throw

	std::sort( coord.begin(), coord.end() );
	// Check that there are no duplicate addresses
	if ( std::unique( coord.begin(), coord.end() ) != coord.end() )
		throw connection_string_invalid();
}

TEST_CASE("/fdbclient/MonitorLeader/parseConnectionString/basic") {
	std::string input;

	{
		input = "asdf:2345@1.1.1.1:345";
		ClusterConnectionString cs(input);
		ASSERT( input == cs.toString() );
	}

	{
		input = "0xxdeadbeef:100100100@1.1.1.1:34534,5.1.5.3:23443";
		ClusterConnectionString cs(input);
		ASSERT( input == cs.toString() );
	}

	{
		input = "0xxdeadbeef:100100100@1.1.1.1:34534,5.1.5.3:23443";
		std::string commented("#start of comment\n");
		commented += input;
		commented += "\n";
		commented += "# asdfasdf ##";

		ClusterConnectionString cs(commented);
		ASSERT( input == cs.toString() );
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
	for(int i=0; i<10000; i++)
	{
		std::string output("");
		auto c=connectionString.begin();
		while(c!=connectionString.end()) {
			if(deterministicRandom()->random01() < 0.1) // Add whitespace character
				output += deterministicRandom()->randomChoice(LiteralStringRef(" \t\n\r"));
			if(deterministicRandom()->random01() < 0.5) { // Add one of the input characters
				output += *c;
				++c;
			}
			if(deterministicRandom()->random01() < 0.1) { // Add a comment block
				output += "#";
				int charCount = deterministicRandom()->randomInt(0, 20);
				for(int i = 0; i < charCount; i++) {
					output += deterministicRandom()->randomChoice(LiteralStringRef("asdfzxcv123345:!@#$#$&()<\"\' \t"));
				}
				output += deterministicRandom()->randomChoice(LiteralStringRef("\n\r"));
			}
		}

		ClusterConnectionString cs(output);
		ASSERT( connectionString == cs.toString() );
	}
	return Void();
}

ClusterConnectionString::ClusterConnectionString( vector<NetworkAddress> servers, Key key )
	: coord(servers)
{
	parseKey(key.toString());
}

void ClusterConnectionString::parseKey( std::string const& key ) {
	// Check the structure of the given key, and fill in this->key and this->keyDesc

	// The key must contain one (and only one) : character
	int colon = key.find_first_of(':');
	if (colon == key.npos)
		throw connection_string_invalid();
	std::string desc = key.substr(0, colon);
	std::string id = key.substr(colon+1);

	// Check that description contains only allowed characters (a-z, A-Z, 0-9, _)
	for(auto c=desc.begin(); c!=desc.end(); ++c)
		if (!(isalnum(*c) || *c == '_'))
			throw connection_string_invalid();

	// Check that ID contains only allowed characters (a-z, A-Z, 0-9)
	for(auto c=id.begin(); c!=id.end(); ++c)
		if (!isalnum(*c))
			throw connection_string_invalid();

	this->key = StringRef(key);
	this->keyDesc = StringRef(desc);
}

std::string ClusterConnectionString::toString() const {
	std::string s = key.toString();
	s += '@';
	for(int i=0; i<coord.size(); i++) {
		if (i) s += ',';
		s += coord[i].toString();
	}
	return s;
}

ClientCoordinators::ClientCoordinators( Reference<ClusterConnectionFile> ccf )
	: ccf(ccf)
{
	ClusterConnectionString cs = ccf->getConnectionString();
	for(auto s = cs.coordinators().begin(); s != cs.coordinators().end(); ++s)
		clientLeaderServers.push_back( ClientLeaderRegInterface( *s ) );
	clusterKey = cs.clusterKey();
}

ClientCoordinators::ClientCoordinators( Key clusterKey, std::vector<NetworkAddress> coordinators )
	: clusterKey(clusterKey) {
	for (const auto& coord : coordinators) {
		clientLeaderServers.push_back( ClientLeaderRegInterface( coord ) );
	}
	ccf = Reference<ClusterConnectionFile>(new ClusterConnectionFile( ClusterConnectionString( coordinators, clusterKey ) ) );
}


UID WLTOKEN_CLIENTLEADERREG_GETLEADER( -1, 2 );
UID WLTOKEN_CLIENTLEADERREG_OPENDATABASE( -1, 3 );

ClientLeaderRegInterface::ClientLeaderRegInterface( NetworkAddress remote )
	: getLeader( Endpoint({remote}, WLTOKEN_CLIENTLEADERREG_GETLEADER) ),
    openDatabase( Endpoint({remote}, WLTOKEN_CLIENTLEADERREG_OPENDATABASE) )
{
}

ClientLeaderRegInterface::ClientLeaderRegInterface( INetwork* local ) {
	getLeader.makeWellKnownEndpoint( WLTOKEN_CLIENTLEADERREG_GETLEADER, TaskPriority::Coordination );
	openDatabase.makeWellKnownEndpoint( WLTOKEN_CLIENTLEADERREG_OPENDATABASE, TaskPriority::Coordination );
}

// Nominee is the worker among all workers that are considered as leader by a coordinator
// This function contacts a coordinator coord to ask if the worker is considered as a leader (i.e., if the worker
// is a nominee)
ACTOR Future<Void> monitorNominee( Key key, ClientLeaderRegInterface coord, AsyncTrigger* nomineeChange, Optional<LeaderInfo> *info ) {
	loop {
		state Optional<LeaderInfo> li = wait( retryBrokenPromise( coord.getLeader, GetLeaderRequest( key, info->present() ? info->get().changeID : UID() ), TaskPriority::CoordinationReply ) );
		wait( Future<Void>(Void()) ); // Make sure we weren't cancelled

		TraceEvent("GetLeaderReply").suppressFor(1.0).detail("Coordinator", coord.getLeader.getEndpoint().getPrimaryAddress()).detail("Nominee", li.present() ? li.get().changeID : UID()).detail("ClusterKey", key.printable());

		if (li != *info) {
			*info = li;
			nomineeChange->trigger();

			if( li.present() && li.get().forward )
				wait( Future<Void>(Never()) );
			wait( Future<Void>(Void()) );
		}
	}
}

// Also used in fdbserver/LeaderElection.actor.cpp!
// bool represents if the LeaderInfo is a majority answer or not.
// This function also masks the first 7 bits of changeId of the nominees and returns the Leader with masked changeId
Optional<std::pair<LeaderInfo, bool>> getLeader( const vector<Optional<LeaderInfo>>& nominees ) {
	// If any coordinator says that the quorum is forwarded, then it is
	for(int i=0; i<nominees.size(); i++)
		if (nominees[i].present() && nominees[i].get().forward)
			return std::pair<LeaderInfo, bool>(nominees[i].get(), true);
	
	vector<std::pair<UID,int>> maskedNominees;
	maskedNominees.reserve(nominees.size());
	for (int i =0; i < nominees.size(); i++) {
		if (nominees[i].present()) {
			maskedNominees.push_back(std::make_pair(UID(nominees[i].get().changeID.first() & LeaderInfo::mask, nominees[i].get().changeID.second()), i));
		}
	}	

	if(!maskedNominees.size())
		return Optional<std::pair<LeaderInfo, bool>>();

	std::sort(maskedNominees.begin(), maskedNominees.end(),
		[](const std::pair<UID,int>& l, const std::pair<UID,int>& r) { return l.first < r.first; });

	int bestCount = 0;
	int bestIdx = 0;
	int currentIdx = 0;
	int curCount = 1;
	for (int i = 1; i < maskedNominees.size(); i++) {
		if (maskedNominees[currentIdx].first == maskedNominees[i].first) {
			curCount++;
		}
		else {
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
ACTOR Future<MonitorLeaderInfo> monitorLeaderOneGeneration( Reference<ClusterConnectionFile> connFile, Reference<AsyncVar<Value>> outSerializedLeaderInfo, MonitorLeaderInfo info ) {
	state ClientCoordinators coordinators( info.intermediateConnFile );
	state AsyncTrigger nomineeChange;
	state std::vector<Optional<LeaderInfo>> nominees;
	state Future<Void> allActors;

	nominees.resize(coordinators.clientLeaderServers.size());

	std::vector<Future<Void>> actors;
	// Ask all coordinators if the worker is considered as a leader (leader nominee) by the coordinator.
	for(int i=0; i<coordinators.clientLeaderServers.size(); i++)
		actors.push_back( monitorNominee( coordinators.clusterKey, coordinators.clientLeaderServers[i], &nomineeChange, &nominees[i] ) );
	allActors = waitForAll(actors);

	loop {
		Optional<std::pair<LeaderInfo, bool>> leader = getLeader(nominees);
		TraceEvent("MonitorLeaderChange").detail("NewLeader", leader.present() ? leader.get().first.changeID : UID(1,1));
		if (leader.present()) {
			if( leader.get().first.forward ) {
				TraceEvent("MonitorLeaderForwarding").detail("NewConnStr", leader.get().first.serializedInfo.toString()).detail("OldConnStr", info.intermediateConnFile->getConnectionString().toString());
				info.intermediateConnFile = Reference<ClusterConnectionFile>(new ClusterConnectionFile(connFile->getFilename(), ClusterConnectionString(leader.get().first.serializedInfo.toString())));
				return info;
			}
			if(connFile != info.intermediateConnFile) {
				if(!info.hasConnected) {
					TraceEvent(SevWarnAlways, "IncorrectClusterFileContentsAtConnection").detail("Filename", connFile->getFilename())
						.detail("ConnectionStringFromFile", connFile->getConnectionString().toString())
						.detail("CurrentConnectionString", info.intermediateConnFile->getConnectionString().toString());
				}
				connFile->setConnectionString(info.intermediateConnFile->getConnectionString());
				info.intermediateConnFile = connFile;
			}

			info.hasConnected = true;
			connFile->notifyConnected();

			outSerializedLeaderInfo->set( leader.get().first.serializedInfo );
		}
		wait( nomineeChange.onTrigger() || allActors );
	}
}

Future<Void> monitorLeaderRemotelyInternal( Reference<ClusterConnectionFile> const& connFile, Reference<AsyncVar<Value>> const& outSerializedLeaderInfo );

template <class LeaderInterface>
Future<Void> monitorLeaderRemotely(Reference<ClusterConnectionFile> const& connFile,
						   Reference<AsyncVar<Optional<LeaderInterface>>> const& outKnownLeader) {
	LeaderDeserializer<LeaderInterface> deserializer;
	Reference<AsyncVar<Value>> serializedInfo( new AsyncVar<Value> );
	Future<Void> m = monitorLeaderRemotelyInternal( connFile, serializedInfo );
	return m || deserializer( serializedInfo, outKnownLeader );
}

ACTOR Future<Void> monitorLeaderInternal( Reference<ClusterConnectionFile> connFile, Reference<AsyncVar<Value>> outSerializedLeaderInfo ) {
	state MonitorLeaderInfo info(connFile);
	loop {
		MonitorLeaderInfo _info = wait( monitorLeaderOneGeneration( connFile, outSerializedLeaderInfo, info ) );
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
	std::vector<std::pair<NetworkAddress,Key>> examples;

	ClientStatusStats() : count(0) {
		examples.reserve(CLIENT_KNOBS->CLIENT_EXAMPLE_AMOUNT);
	}
};

OpenDatabaseRequest ClientData::getRequest() {
	OpenDatabaseRequest req;

	std::map<StringRef, ClientStatusStats> issueMap;
	std::map<ClientVersionRef, ClientStatusStats> versionMap;
	std::map<StringRef, ClientStatusStats> maxProtocolMap;
	int clientCount = 0;

	//SOMEDAY: add a yield in this loop
	for(auto& ci : clientStatusInfoMap) {
		for(auto& it : ci.second.issues) {
			auto& entry = issueMap[it];
			entry.count++;
			if(entry.examples.size() < CLIENT_KNOBS->CLIENT_EXAMPLE_AMOUNT) {
				entry.examples.push_back(std::make_pair(ci.first, ci.second.traceLogGroup));
			}
		}
		if(ci.second.versions.size()) {
			clientCount++;
			StringRef maxProtocol;
			for(auto& it : ci.second.versions) {
				maxProtocol = std::max(maxProtocol, it.protocolVersion);
				auto& entry = versionMap[it];
				entry.count++;
				if(entry.examples.size() < CLIENT_KNOBS->CLIENT_EXAMPLE_AMOUNT) {
					entry.examples.push_back(std::make_pair(ci.first, ci.second.traceLogGroup));
				}
			}
			auto& maxEntry = maxProtocolMap[maxProtocol];
			maxEntry.count++;
			if(maxEntry.examples.size() < CLIENT_KNOBS->CLIENT_EXAMPLE_AMOUNT) {
				maxEntry.examples.push_back(std::make_pair(ci.first, ci.second.traceLogGroup));
			}
		} else {
			auto& entry = versionMap[ClientVersionRef()];
			entry.count++;
			if(entry.examples.size() < CLIENT_KNOBS->CLIENT_EXAMPLE_AMOUNT) {
				entry.examples.push_back(std::make_pair(ci.first, ci.second.traceLogGroup));
			}
		}
	}

	req.issues.reserve(issueMap.size());
	for(auto& it : issueMap) {
		req.issues.push_back(ItemWithExamples<Key>(it.first, it.second.count, it.second.examples));
	}
	req.supportedVersions.reserve(versionMap.size());
	for(auto& it : versionMap) {
		req.supportedVersions.push_back(ItemWithExamples<Standalone<ClientVersionRef>>(it.first, it.second.count, it.second.examples));
	}
	req.maxProtocolSupported.reserve(maxProtocolMap.size());
	for(auto& it : maxProtocolMap) {
		req.maxProtocolSupported.push_back(ItemWithExamples<Key>(it.first, it.second.count, it.second.examples));
	}
	req.clientCount = clientCount;

	return req;
}

ACTOR Future<Void> getClientInfoFromLeader( Reference<AsyncVar<Optional<ClusterControllerClientInterface>>> knownLeader, ClientData* clientData ) {
	while( !knownLeader->get().present() ) {
		wait( knownLeader->onChange() );
	}
	
	state double lastRequestTime = now();
	state OpenDatabaseRequest req = clientData->getRequest();
	
	loop {
		if(now() - lastRequestTime > CLIENT_KNOBS->MAX_CLIENT_STATUS_AGE) {
			lastRequestTime = now();
			req = clientData->getRequest();
		} else {
			resetReply(req);
		}
		req.knownClientInfoID = clientData->clientInfo->get().read().id;
		choose {
			when( ClientDBInfo ni = wait( brokenPromiseToNever( knownLeader->get().get().clientInterface.openDatabase.getReply( req ) ) ) ) {
				TraceEvent("MonitorLeaderForProxiesGotClientInfo", knownLeader->get().get().clientInterface.id())
				    .detail("CommitProxy0", ni.commitProxies.size() ? ni.commitProxies[0].id() : UID())
				    .detail("GrvProxy0", ni.grvProxies.size() ? ni.grvProxies[0].id() : UID())
				    .detail("ClientID", ni.id);
				clientData->clientInfo->set(CachedSerialization<ClientDBInfo>(ni));
			}
			when( wait( knownLeader->onChange() ) ) {}
		}
	}
}

ACTOR Future<Void> monitorLeaderForProxies( Key clusterKey, vector<NetworkAddress> coordinators, ClientData* clientData, Reference<AsyncVar<Optional<LeaderInfo>>> leaderInfo ) {
	state vector< ClientLeaderRegInterface > clientLeaderServers;
	state AsyncTrigger nomineeChange;
	state std::vector<Optional<LeaderInfo>> nominees;
	state Future<Void> allActors;
	state Reference<AsyncVar<Optional<ClusterControllerClientInterface>>> knownLeader(new AsyncVar<Optional<ClusterControllerClientInterface>>{});

	for(auto s = coordinators.begin(); s != coordinators.end(); ++s) {
		clientLeaderServers.push_back( ClientLeaderRegInterface( *s ) );
	}

	nominees.resize(clientLeaderServers.size());

	std::vector<Future<Void>> actors;
	// Ask all coordinators if the worker is considered as a leader (leader nominee) by the coordinator.
	for(int i=0; i<clientLeaderServers.size(); i++) {
		actors.push_back( monitorNominee( clusterKey, clientLeaderServers[i], &nomineeChange, &nominees[i] ) );
	}
	actors.push_back( getClientInfoFromLeader( knownLeader, clientData ) );
	allActors = waitForAll(actors);

	loop {
		Optional<std::pair<LeaderInfo, bool>> leader = getLeader(nominees);
		TraceEvent("MonitorLeaderForProxiesChange").detail("NewLeader", leader.present() ? leader.get().first.changeID : UID(1,1)).detail("Key", clusterKey.printable());
		if (leader.present()) {
			if( leader.get().first.forward ) {
				ClientDBInfo outInfo;
				outInfo.id = deterministicRandom()->randomUniqueID();
				outInfo.forward = leader.get().first.serializedInfo;
				clientData->clientInfo->set(CachedSerialization<ClientDBInfo>(outInfo));
				leaderInfo->set(leader.get().first);
				TraceEvent("MonitorLeaderForProxiesForwarding").detail("NewConnStr", leader.get().first.serializedInfo.toString());
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
		wait( nomineeChange.onTrigger() || allActors );
	}
}

void shrinkProxyList(ClientDBInfo& ni, std::vector<UID>& lastCommitProxyUIDs,
                     std::vector<CommitProxyInterface>& lastCommitProxies, std::vector<UID>& lastGrvProxyUIDs,
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
	if(ni.grvProxies.size() > CLIENT_KNOBS->MAX_GRV_PROXY_CONNECTIONS) {
		std::vector<UID> grvProxyUIDs;
		for(auto& grvProxy : ni.grvProxies) {
			grvProxyUIDs.push_back(grvProxy.id());
		}
		if(grvProxyUIDs != lastGrvProxyUIDs) {
			lastGrvProxyUIDs.swap(grvProxyUIDs);
			lastGrvProxies = ni.grvProxies;
			deterministicRandom()->randomShuffle(lastGrvProxies);
			lastGrvProxies.resize(CLIENT_KNOBS->MAX_GRV_PROXY_CONNECTIONS);
			for(int i = 0; i < lastGrvProxies.size(); i++) {
				TraceEvent("ConnectedGrvProxy").detail("GrvProxy", lastGrvProxies[i].id());
			}
		}
		ni.grvProxies = lastGrvProxies;
	}
}

// Leader is the process that will be elected by coordinators as the cluster controller
ACTOR Future<MonitorLeaderInfo> monitorProxiesOneGeneration(
    Reference<ClusterConnectionFile> connFile, Reference<AsyncVar<ClientDBInfo>> clientInfo, MonitorLeaderInfo info,
    Reference<ReferencedObject<Standalone<VectorRef<ClientVersionRef>>>> supportedVersions, Key traceLogGroup) {
	state ClusterConnectionString cs = info.intermediateConnFile->getConnectionString();
	state vector<NetworkAddress> addrs = cs.coordinators();
	state int idx = 0;
	state int successIdx = 0;
	state Optional<double> incorrectTime;
	state std::vector<UID> lastCommitProxyUIDs;
	state std::vector<CommitProxyInterface> lastCommitProxies;
	state std::vector<UID> lastGrvProxyUIDs;
	state std::vector<GrvProxyInterface> lastGrvProxies;

	deterministicRandom()->randomShuffle(addrs);
	loop {
		state ClientLeaderRegInterface clientLeaderServer( addrs[idx] );
		state OpenDatabaseCoordRequest req;
		req.clusterKey = cs.clusterKey();
		req.coordinators = cs.coordinators();
		req.knownClientInfoID = clientInfo->get().id;
		req.supportedVersions = supportedVersions->get();
		req.traceLogGroup = traceLogGroup;

		ClusterConnectionString fileConnectionString;
		if (connFile && !connFile->fileContentsUpToDate(fileConnectionString)) {
			req.issues.push_back_deep(req.issues.arena(), LiteralStringRef("incorrect_cluster_file_contents"));
			std::string connectionString = connFile->getConnectionString().toString();
			if(!incorrectTime.present()) {
				incorrectTime = now();
			}
			if(connFile->canGetFilename()) {
				// Don't log a SevWarnAlways initially to account for transient issues (e.g. someone else changing the file right before us)
				TraceEvent(now() - incorrectTime.get() > 300 ? SevWarnAlways : SevWarn, "IncorrectClusterFileContents")
					.detail("Filename", connFile->getFilename())
					.detail("ConnectionStringFromFile", fileConnectionString.toString())
					.detail("CurrentConnectionString", connectionString);
			}
		}
		else {
			incorrectTime = Optional<double>();
		}

		state ErrorOr<CachedSerialization<ClientDBInfo>> rep = wait( clientLeaderServer.openDatabase.tryGetReply( req, TaskPriority::CoordinationReply ) );
		if (rep.present()) {
			if( rep.get().read().forward.present() ) {
				TraceEvent("MonitorProxiesForwarding").detail("NewConnStr", rep.get().read().forward.get().toString()).detail("OldConnStr", info.intermediateConnFile->getConnectionString().toString());
				info.intermediateConnFile = Reference<ClusterConnectionFile>(new ClusterConnectionFile(connFile->getFilename(), ClusterConnectionString(rep.get().read().forward.get().toString())));
				return info;
			}
			if(connFile != info.intermediateConnFile) {
				if(!info.hasConnected) {
					TraceEvent(SevWarnAlways, "IncorrectClusterFileContentsAtConnection").detail("Filename", connFile->getFilename())
						.detail("ConnectionStringFromFile", connFile->getConnectionString().toString())
						.detail("CurrentConnectionString", info.intermediateConnFile->getConnectionString().toString());
				}
				connFile->setConnectionString(info.intermediateConnFile->getConnectionString());
				info.intermediateConnFile = connFile;
			}

			info.hasConnected = true;
			connFile->notifyConnected();

			auto& ni = rep.get().mutate();
			shrinkProxyList(ni, lastCommitProxyUIDs, lastCommitProxies, lastGrvProxyUIDs, lastGrvProxies);
			clientInfo->set( ni );
			successIdx = idx;
		} else {
			idx = (idx+1)%addrs.size();
			if(idx == successIdx) {
				wait(delay(CLIENT_KNOBS->COORDINATOR_RECONNECTION_DELAY));
			}
		}
	}
}

ACTOR Future<Void> monitorProxies( Reference<AsyncVar<Reference<ClusterConnectionFile>>> connFile, Reference<AsyncVar<ClientDBInfo>> clientInfo, Reference<ReferencedObject<Standalone<VectorRef<ClientVersionRef>>>> supportedVersions, Key traceLogGroup ) {
	state MonitorLeaderInfo info(connFile->get());
	loop {
		choose {
			when(MonitorLeaderInfo _info = wait( monitorProxiesOneGeneration( connFile->get(), clientInfo, info, supportedVersions, traceLogGroup ) )) {
				info = _info;
			}
			when(wait(connFile->onChange())) {
				info.hasConnected = false;
				info.intermediateConnFile = connFile->get();
			}
		}
	}
}
