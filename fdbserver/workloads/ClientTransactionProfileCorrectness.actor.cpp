#include "workloads.h"
#include "fdbserver/ServerDBInfo.h"
#include "fdbclient/ManagementAPI.h"
#include "fdbclient/RunTransaction.actor.h"


static const Key CLIENT_LATENCY_INFO_PREFIX = LiteralStringRef("client_latency/");
static const Key CLIENT_LATENCY_INFO_CTR_PREFIX = LiteralStringRef("client_latency_counter/");

/*
FF               - 2 bytes \xff\x02
SSSSSSSSSS       - 10 bytes Version Stamp
RRRRRRRRRRRRRRRR - 16 bytes Transaction id
NNNN             - 4 Bytes Chunk number (Big Endian)
TTTT             - 4 Bytes Total number of chunks (Big Endian)
*/
StringRef sampleTrInfoKey = LiteralStringRef("\xff\x02/fdbClientInfo/client_latency/SSSSSSSSSS/RRRRRRRRRRRRRRRR/NNNNTTTT/");
static const auto chunkNumStartIndex = sampleTrInfoKey.toString().find('N');
static const auto numChunksStartIndex = sampleTrInfoKey.toString().find('T');
static const int chunkFormatSize = 4;
static const auto trIdStartIndex = sampleTrInfoKey.toString().find('R');
static const int trIdFormatSize = 16;


// Checks TransactionInfo format
bool checkTxInfoEntryFormat(BinaryReader &reader) {
	// Check protocol version
	uint64_t protocolVersion;
	reader >> protocolVersion;
	reader.setProtocolVersion(protocolVersion);

	while (reader.empty()) {
		// Get EventType and timestamp
		FdbClientLogEvents::EventType event;
		reader >> event;
		double timeStamp;
		reader >> timeStamp;

		switch (event)
		{
		case FdbClientLogEvents::GET_VERSION_LATENCY:
		{
			FdbClientLogEvents::EventGetVersion gv;
			reader >> gv;
			break;
		}
		case FdbClientLogEvents::GET_LATENCY:
		{
			FdbClientLogEvents::EventGet g;
			reader >> g;
			break;
		}
		case FdbClientLogEvents::GET_RANGE_LATENCY:
		{
			FdbClientLogEvents::EventGetRange gr;
			reader >> gr;
			break;
		}
		case FdbClientLogEvents::COMMIT_LATENCY:
		{
			FdbClientLogEvents::EventCommit c;
			reader >> c;
			break;
		}
		case FdbClientLogEvents::ERROR_GET:
		{
			FdbClientLogEvents::EventGetError ge;
			reader >> ge;
			break;
		}
		case FdbClientLogEvents::ERROR_GET_RANGE:
		{
			FdbClientLogEvents::EventGetRangeError gre;
			reader >> gre;
			break;
		}
		case FdbClientLogEvents::ERROR_COMMIT:
		{
			FdbClientLogEvents::EventCommitError ce;
			reader >> ce;
			break;
		}
		default:
			TraceEvent(SevError, "ClientTransactionProfilingUnknownEvent").detail("EventType", event);
			return false;
		}
	}

	return true;
}

struct ClientTransactionProfileCorrectnessWorkload : TestWorkload {
	double samplingProbability;
	int64_t trInfoSizeLimit;

	ClientTransactionProfileCorrectnessWorkload(WorkloadContext const& wcx)
		: TestWorkload(wcx)
	{
		if (clientId == 0) {
			samplingProbability = getOption(options, LiteralStringRef("samplingProbability"), g_random->random01() / 10); //rand range 0 - 0.1
			trInfoSizeLimit = getOption(options, LiteralStringRef("trInfoSizeLimit"), g_random->randomInt(100 * 1024, 10 * 1024 * 1024)); // 100 KB - 10 MB
			TraceEvent(SevInfo, "ClientTransactionProfilingSetup").detail("samplingProbability", samplingProbability).detail("trInfoSizeLimit", trInfoSizeLimit);
		}
	}

	virtual std::string description() { return "ClientTransactionProfileCorrectness"; }

	virtual Future<Void> setup(Database const& cx) {
		if (clientId == 0) {
			const_cast<ClientKnobs *>(CLIENT_KNOBS)->CSI_STATUS_DELAY = 2.0; // 2 seconds
			return changeProfilingParameters(cx, trInfoSizeLimit, samplingProbability);
		}
		return Void();
	}

	virtual Future<Void> start(Database const& cx) {
		return Void();
	}

	int getNumChunks(KeyRef key) {
		return bigEndian32(BinaryReader::fromStringRef<int>(key.substr(numChunksStartIndex, chunkFormatSize), Unversioned()));
	}

	int getChunkNum(KeyRef key) {
		return bigEndian32(BinaryReader::fromStringRef<int>(key.substr(chunkNumStartIndex, chunkFormatSize), Unversioned()));
	}

	std::string getTrId(KeyRef key) {
		return key.substr(trIdStartIndex, trIdFormatSize).toString();
	}

	bool checkTxInfoEntriesFormat(const Standalone<RangeResultRef> &txInfoEntries) {
		std::string val;
		std::map<std::string, std::vector<ValueRef>> trInfoChunks;
		for (auto kv : txInfoEntries) {
			int numChunks = getNumChunks(kv.key);
			int chunkNum = getChunkNum(kv.key);
			std::string trId = getTrId(kv.key);

			if (numChunks == 1) {
				ASSERT(chunkNum == 1);
				BinaryReader reader(kv.value, Unversioned());
					if (!checkTxInfoEntryFormat(reader))
						return false;
			}
			else {
				if (chunkNum == 1) { // First chunk
					ASSERT(trInfoChunks.find(trId) == trInfoChunks.end());
					trInfoChunks.insert(std::pair < std::string, std::vector<ValueRef> >(trId, {kv.value}));
				}
				else {
					if (trInfoChunks.find(trId) == trInfoChunks.end()) {
						// Some of the earlier chunks for this trId should have been deleted.
						// Discard this chunk as it is of not much use
						TraceEvent(SevInfo, "ClientTransactionProfilingSomeChunksMissing").detail("trId", trId);
					}
					else {
						trInfoChunks.find(trId)->second.push_back(kv.value);
					}
				}
				if (chunkNum == numChunks && trInfoChunks.find(trId) != trInfoChunks.end()) {
					auto iter = trInfoChunks.find(trId);
					BinaryWriter bw(Unversioned());
					for (auto val : iter->second)
						bw.serializeBytes(val.begin(), val.size());
					BinaryReader reader(bw.getData(), bw.getLength(), Unversioned());
					if (!checkTxInfoEntryFormat(reader))
						return false;
					trInfoChunks.erase(iter);
				}
			}
		}
		ASSERT(trInfoChunks.empty());
		return true;
	}

	ACTOR Future<Void> changeProfilingParameters(Database cx, int64_t sizeLimit, double sampleProbability) {

		Void _ = wait(runRYWTransaction(cx, [=](Reference<ReadYourWritesTransaction> tr) -> Future<Void>
						{
							tr->setOption(FDBTransactionOptions::ACCESS_SYSTEM_KEYS);
							tr->setOption(FDBTransactionOptions::LOCK_AWARE);
							tr->set(fdbClientInfoTxnSampleRate, BinaryWriter::toValue(sampleProbability, Unversioned()));
							tr->set(fdbClientInfoTxnSizeLimit, BinaryWriter::toValue(sizeLimit, Unversioned()));
							return Void();
						}
					 ));
		return Void();
	}

	ACTOR Future<bool> _check(Database cx, ClientTransactionProfileCorrectnessWorkload* self) {
		Void _ = wait(self->changeProfilingParameters(cx, self->trInfoSizeLimit, 0));  // Disable sampling
		// FIXME: Better way to ensure that all client profile data has been flushed to the database
		Void _ = wait(delay(CLIENT_KNOBS->CSI_STATUS_DELAY));

		state Key clientLatencyAtomicCtr = CLIENT_LATENCY_INFO_CTR_PREFIX.withPrefix(fdbClientInfoPrefixRange.begin);
		state int64_t counter;
		state Standalone<RangeResultRef> txInfoEntries;
		Optional<Value> ctrValue = wait(runRYWTransaction(cx, [=](Reference<ReadYourWritesTransaction> tr) -> Future<Optional<Value>> 
																											{ 
																												tr->setOption(FDBTransactionOptions::ACCESS_SYSTEM_KEYS);
																												tr->setOption(FDBTransactionOptions::LOCK_AWARE);
																												return tr->get(clientLatencyAtomicCtr); 
																											}
																											));
		counter = ctrValue.present() ? BinaryReader::fromStringRef<int64_t>(ctrValue.get(), Unversioned()) : 0;
		state Key clientLatencyName = CLIENT_LATENCY_INFO_PREFIX.withPrefix(fdbClientInfoPrefixRange.begin);

		state KeySelector begin = firstGreaterOrEqual(CLIENT_LATENCY_INFO_PREFIX.withPrefix(fdbClientInfoPrefixRange.begin));
		state KeySelector end = firstGreaterOrEqual(strinc(begin.getKey()));
		loop {
			state Transaction tr(cx);
			try {
				tr.setOption(FDBTransactionOptions::ACCESS_SYSTEM_KEYS);
				tr.setOption(FDBTransactionOptions::LOCK_AWARE);
				state Standalone<RangeResultRef> kvRange = wait(tr.getRange(begin, end, 10));
				if (kvRange.empty())
					break;
				txInfoEntries.arena().dependsOn(kvRange.arena());
				txInfoEntries.append(txInfoEntries.arena(), kvRange.begin(), kvRange.size());
				begin = firstGreaterThan(kvRange.back().key);
			}
			catch (Error& e) {
				Void _ = wait(tr.onError(e));
			}
		}

		// Check if the counter value matches the size of contents
		int64_t contentsSize = 0;
		for (auto &kv : txInfoEntries) {
			contentsSize += kv.key.size() + kv.value.size();
		}
		// FIXME: Find a way to check that contentsSize is not greater than a certain limit.
		//if (counter != contentsSize) {
		//	TraceEvent(SevError, "ClientTransactionProfilingIncorrectCtrVal").detail("counter", counter).detail("contentsSize", contentsSize);
		//	return false;
		//}
		TraceEvent(SevInfo, "ClientTransactionProfilingCtrval").detail("counter", counter);
		TraceEvent(SevInfo, "ClientTransactionProfilingContentsSize").detail("contentsSize", contentsSize);

		// Check if the data format is as expected
		return self->checkTxInfoEntriesFormat(txInfoEntries);
	}

	virtual Future<bool> check(Database const& cx) {
		if (clientId != 0)
			return true;
		return _check(cx, this);
	}

	virtual void getMetrics(vector<PerfMetric>& m) {
	}

};

WorkloadFactory<ClientTransactionProfileCorrectnessWorkload> ClientTransactionProfileCorrectnessWorkloadFactory("ClientTransactionProfileCorrectness");
