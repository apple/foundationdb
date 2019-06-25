#ifndef POLEVAULTFUZZ_H
#define POLEVAULTFUZZ_H
#pragma once

#include <boost/array.hpp>
#include <boost/asio.hpp>
#include <boost/bind.hpp>

#include "ConsumerAdapterUtils.h"
#include <string>

#include "ConsumerAdapterProtocol_generated.h"
#include "ConsumerClient.h"
#include <list>
#include <set>

/*
  The EndpointLoadGenerator provides an API to generate requests with randomized data
  and endpoints It also tracks which endpoints and data ranges are waiting for
  replies or verifies- ensuring that waiting endpoints aren't reused and a new
  push batch doesn't overwrite a waiting verify range.  Its up to the caller to
  update EndpointLoadGenerator with the responses.
  Finally, it
 */

class EndpointLoadGenerator {
	struct VerifyRangeInfo {
		std::pair<int64_t, int64_t> keyRange;
		int checksum;
		VerifyRangeInfo() {}
		VerifyRangeInfo(int64_t b, int64_t e, int cs) : keyRange(b, e), checksum(cs){};
		std::string toStr() const {
			return fmt::format("keyStart:{} keyEnd:{} checksum:{}", keyRange.first, keyRange.second, checksum);
		}
		bool operator<(const VerifyRangeInfo& other) const {
			if (keyRange == other.keyRange) {
				return checksum < other.checksum;
			}
			return keyRange < other.keyRange;
		}
	};
	ConsAdapter::serialization::UID registeredUID;
	// params for generating request fuzz
	int keyRange = 1000;
	int valueRange = 256;
	int mutationCountMax = 100;
	int maxKeyRangeSize = 10;
	int rangeCountMax = 3;
	int maxOutstandingVerifyRanges = 100; // max key range verifies waiting for send and response
	std::string keyPrefix = "some/key/path/";
	std::unique_ptr<uint64_t[]> dataFuzz;
	Log log;

	std::set<int> epsWaitingForSet; // remove on set req reply
	std::set<int> epsWaitingForReply; // remove on req reply
	// Track current verify requests:
	std::set<VerifyRangeInfo> verifyRangesWaitingToSend; // remove on req send
	std::set<VerifyRangeInfo> verifyRangesWaitingForResponse; // remove on verify
	                                                          // finish response
	std::map<int, std::vector<VerifyRangeInfo>> epToVerifyRangesWaitingForResponse; // add on verify, remove on
	                                                                                // verify finish response
	std::map<int, VerifyRangeInfo> epToVerifyRangeWaitingForPush; // add on push, remove on push
	                                                              // response
	std::map<int, MessageStats> epToStats;

	void printMutVector(
	    const flatbuffers::Vector<flatbuffers::Offset<ConsAdapter::serialization::Mutation>>* mutations);
	void printRangesVector(
	    const flatbuffers::Vector<flatbuffers::Offset<ConsAdapter::serialization::KeyRange>>* keyRanges);

	ConsAdapter::serialization::UID uidFuzz();
	ConsAdapter::serialization::GlobalVersion globalVersionFuzz();
	ConsAdapter::serialization::ReplicatorState replicatorStateFuzz();
	ConsAdapter::serialization::ReplicatorState registerReplicatorState();
	int endpointFuzz();
	int64_t keyIdxFuzz();
	std::string keyFromKeyIdx(uint64_t index);
	// int64_t keyIdxFromKey(std::string key);
	std::string valueFuzz();

	bool keyInVerifyRanges(int64_t keyIdx);

public:
	int getGetRepStateReq(flatbuffers::FlatBufferBuilder& serializer);
	int getSetRepStateReq(flatbuffers::FlatBufferBuilder& serializer);
	int getPushBatchReq(flatbuffers::FlatBufferBuilder& serializer);
	int getVerifyRangesReq(flatbuffers::FlatBufferBuilder& serializer);

	MessageStats waitingEPGotReply(int endpoint);
	MessageStats waitingEPGotVerifyFinish(int endpoint);
	void updateEndpointSendTime(int endpoint);

	int endpointsWaitingForSet() { return epsWaitingForSet.size(); }
	int endpointsWaitingForReply() { return epsWaitingForReply.size(); }
	int endpointsWaitingForVerifyFinish() { return epToVerifyRangesWaitingForResponse.size(); }
	int verifyReqsWaitingToSend() { return verifyRangesWaitingToSend.size(); }

	void init(int kRange, int vRange, int mCountMax, int mKRSize, int rCountMax, int maxOSVR);
};

#endif
