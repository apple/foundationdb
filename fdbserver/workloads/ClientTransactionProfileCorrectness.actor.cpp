#include "flow/actorcompiler.h"
#include "fdbserver/TesterInterface.h"
#include "fdbclient/NativeAPI.h"
#include "workloads.h"


static const Key CLIENT_LATENCY_INFO_PREFIX = LiteralStringRef("client_latency/");
static const Key CLIENT_LATENCY_INFO_CTR_PREFIX = LiteralStringRef("client_latency_counter/");

/*
FF               - 2 bytes \xff\x02
SSSSSSSSSS       - 10 bytes Version Stamp
RRRRRRRRRRRRRRRR - 16 bytes Transaction id
NNNN             - 4 Bytes Chunk number
TTTT             - 4 Bytes Total number of chunks
*/
StringRef sampleTrInfoKey = LiteralStringRef("\xff\x02/fdbClientInfo/client_latency/SSSSSSSSSS/RRRRRRRRRRRRRRRR/NNNNTTTT/");
static const auto chunkNumStartIndex = sampleTrInfoKey.toString().find('N');
static const auto numChunksStartIndex = sampleTrInfoKey.toString().find('T');
static const int chunkFormatSize = 4;
static const auto trIdStartIndex = sampleTrInfoKey.toString().find('R');
static const int trIdFormatSize = 16;

// Checks TransactionInfo format for current protocol version.
bool checkTxInfoEntryFormat(BinaryReader &reader) {
	// Check protocol version
	uint64_t protocolVersion;
	reader >> protocolVersion;
	if (protocolVersion != currentProtocolVersion) {
		TraceEvent(SevError, "ClientTransactionProfilingVersionMismatch").detail("currentProtocolVersion", currentProtocolVersion).detail("entryProtocolVersion", protocolVersion);
		return false;
	}

	// Get EventType and timestamp
	FdbClientLogEvents::EventType event;
	reader >> event;
	double timeStamp;
	reader >> timeStamp;

	switch (event)
	{
	case FdbClientLogEvents::GET_VERSION_LATENCY:
		FdbClientLogEvents::EventGetVersion getVersion();
		break;
	default:
		// TODO: return false;
		break;
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
			trInfoSizeLimit = getOption(options, LiteralStringRef("trInfoSizeLimit"), g_random->randomInt(1024, 10 * 1024 * 1024)); // 1024 bytes - 10 MB
			TraceEvent(SevInfo, "ClientTransactionProfilingSetup").detail("samplingProbability", samplingProbability).detail("trInfoSizeLimit", trInfoSizeLimit);
		}
	}

	virtual std::string description() { return "ClientTransactionProfileCorrectness"; }

	virtual Future<Void> setup(Database const& cx) {
		if (clientId == 0) {
			const_cast<ClientKnobs *>(CLIENT_KNOBS)->CSI_SAMPLING_PROBABILITY = samplingProbability;
			const_cast<ClientKnobs *>(CLIENT_KNOBS)->CSI_SIZE_LIMIT = trInfoSizeLimit;
		}
		return Void();
	}

	virtual Future<Void> start(Database const& cx) {
		return Void();
	}

	int getNumChunks(KeyRef key) {
		return BinaryReader::fromStringRef<int>(key.substr(numChunksStartIndex, chunkFormatSize), Unversioned());
	}

	int getChunkNum(KeyRef key) {
		return BinaryReader::fromStringRef<int>(key.substr(chunkNumStartIndex, chunkFormatSize), Unversioned());
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

	ACTOR Future<bool> _check(Database cx, ClientTransactionProfileCorrectnessWorkload* self) {
		const_cast<ClientKnobs *>(CLIENT_KNOBS)->CSI_SAMPLING_PROBABILITY = 0; // Disable sampling
		// Wait for any client transaction profiler data to be flushed
		Void _ = wait(delay(CLIENT_KNOBS->CSI_STATUS_DELAY));

		state Key clientLatencyName = CLIENT_LATENCY_INFO_PREFIX.withPrefix(fdbClientInfoPrefixRange.begin);
		state Key clientLatencyAtomicCtr = CLIENT_LATENCY_INFO_CTR_PREFIX.withPrefix(fdbClientInfoPrefixRange.begin);
		state int64_t counter;
		state Standalone<RangeResultRef> txInfoEntries;

		loop{
			state Transaction tr(cx);
			try {
				tr.setOption(FDBTransactionOptions::ACCESS_SYSTEM_KEYS);
				tr.setOption(FDBTransactionOptions::LOCK_AWARE);
				Optional<Value> ctrValue = wait(tr.get(clientLatencyAtomicCtr));
				counter = ctrValue.present() ? BinaryReader::fromStringRef<int64_t>(ctrValue.get(), Unversioned()) : 0;
				Standalone<RangeResultRef> kvRange = wait(tr.getRange(KeyRangeRef(clientLatencyName, strinc(clientLatencyName)), CLIENT_KNOBS->TOO_MANY));
				txInfoEntries = kvRange;
				break;
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
		if (counter != contentsSize) {
			TraceEvent(SevError, "ClientTransactionProfilingIncorrectCtrVal").detail("counter", counter).detail("contentsSize", contentsSize);
			return false;
		}
		TraceEvent(SevInfo, "ClientTransactionProfilingCtrval").detail("counter", counter);

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