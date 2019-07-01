#ifndef POLEVAULTUTILS_H
#define POLEVAULTUTILS_H
#pragma once

#include "ConsumerAdapterProtocol_generated.h"
#include "Crc32.h"
#include "iostream"
#include <boost/array.hpp>

#ifdef INGEST_ADAPTER_SIM_TEST
#include "SimLog.h"
#include "fmt/format.h"
#else
#include "Log.h"
#endif

#include <list>
#include <string>
#include <chrono>

uint64_t rand64();

enum class MessageBufferType : uint8_t {
	T_UNINIT = 0,
	T_GetReplicatorStateReq,
	T_SetReplicatorStateReq,
	T_PushBatchReq,
	T_VerifyRangeReq
};

enum class MessageResponseType : uint8_t { T_UNINIT = 0, T_ReplyResp, T_FinishResp };

struct MessageHeader {
	uint32_t size;
	uint32_t checksum;

	std::string toStr() const { return fmt::format("(size:{} sum:{})", size, checksum); }
	MessageHeader(){};
	MessageHeader(uint32_t s, uint32_t c) : size(s), checksum(c){};
};

std::string printObj(const MessageHeader& head);

struct UIDExt {
	int64_t part1;
	int64_t part2;
	std::string toStr() const { return fmt::format("({}:{})", part1, part2); }
	UIDExt() {}
	UIDExt(const UIDExt& other) : part1(other.part1), part2(other.part2) {}

	UIDExt(ConsAdapter::serialization::UID serialObj) : part1(serialObj.part1()), part2(serialObj.part2()) {}
	ConsAdapter::serialization::UID serialize() { return ConsAdapter::serialization::UID(part1, part2); }
	bool operator==(UIDExt const& other) { return part1 == other.part1 && part2 == other.part2; }
	bool operator!=(UIDExt const& other) { return part1 != other.part1 || part2 != other.part2; }
};

struct GlobalVersionExt {
	int64_t version;
	int64_t previousVersion;
	int32_t switchCount;
	std::string toStr() const { return fmt::format("(sw:{}, v:{}, prev:{})", switchCount, version, previousVersion); }
	GlobalVersionExt() {}
	GlobalVersionExt(const GlobalVersionExt& other)
	  : version(other.version), previousVersion(other.previousVersion), switchCount(other.switchCount) {}
	GlobalVersionExt& operator=(const GlobalVersionExt& other) = default; // {
	//  version = other.version;
	//   previousVersion = other.previousVersion;
	//   switchCount = other.switchCount;
	//   return *this;
	// }
	GlobalVersionExt(ConsAdapter::serialization::GlobalVersion serialObj)
	  : version(serialObj.version()), previousVersion(serialObj.previousVersion()),
	    switchCount(serialObj.switchCount()) {}
	ConsAdapter::serialization::GlobalVersion serialize() {
		return ConsAdapter::serialization::GlobalVersion(version, previousVersion, switchCount);
	}
};

struct ReplicatorStateExt {
	GlobalVersionExt lastRepVersion;
	UIDExt id;

	std::string toStr() const { return fmt::format("(id:{}, lastRepVersion:{})", id.toStr(), lastRepVersion.toStr()); }

	ReplicatorStateExt() {}
	ReplicatorStateExt(ConsAdapter::serialization::ReplicatorState serialObj)
	  : lastRepVersion(GlobalVersionExt(serialObj.lastRepVersion())), id(UIDExt(serialObj.id())) {}
	ConsAdapter::serialization::ReplicatorState serialize() {
		return ConsAdapter::serialization::ReplicatorState(lastRepVersion.serialize(), id.serialize());
	}
};

struct MessageBuffer {
	uint64_t id; // for deduplication
	bool readyForWrite = false;
	MessageHeader header;
	// for access convenience after callbacks.  Just set the type and
	// pass the MessageBuffer in you callback chain
	int endpoint;

	MessageBufferType type;
	MessageResponseType respType = MessageResponseType::T_ReplyResp;

	// reply data
	int curVerifyRange = 0;
	int error = 0;
	ReplicatorStateExt replyRepState;
	std::vector<uint32_t> replyChecksums;
	// handy access methods
	const ConsAdapter::serialization::GetReplicatorStateReq* getGetReplicatorStateReq();
	const ConsAdapter::serialization::SetReplicatorStateReq* getSetReplicatorStateReq();
	const ConsAdapter::serialization::PushBatchReq* getPushBatchReq();
	const ConsAdapter::serialization::VerifyRangeReq* getVerifyRangeReq();

	ReplicatorStateExt getRepState();
	const ConsAdapter::serialization::ReplicatorState* getRepStateData();
	const flatbuffers::Vector<flatbuffers::Offset<ConsAdapter::serialization::Mutation>>* getMutations();
	const flatbuffers::Vector<flatbuffers::Offset<ConsAdapter::serialization::KeyRange>>* getRanges();
	uint32_t getChecksum(int index);

	const flatbuffers::Vector<unsigned>* getChecksums();

  bool checkReplicatorIDRegistry(std::shared_ptr<Log> log);
	// buffers
	flatbuffers::FlatBufferBuilder serializer;
	std::vector<char> readBuffer;
	ConsAdapter::serialization::ReplicatorState setStateObj;

	static uint64_t nextUID;

#ifndef INGEST_ADAPTER_SIM_TEST
	std::vector<boost::asio::const_buffer> writeBuffers;
	void prepareWrite();
	std::vector<boost::asio::const_buffer> getWriteBuffers() {
		assert(readyForWrite);
		return writeBuffers;
	}
#endif
	std::string toStr();
	void resetReply() {
		error = 0;
		curVerifyRange = 0;
		respType = MessageResponseType::T_ReplyResp;
		replyRepState = ReplicatorStateExt();
		replyChecksums.clear();
	};
	MessageBuffer();
	~MessageBuffer() {
		std::cout << "delete message buffer endpoint:" << endpoint << " header:" << printObj(header) << std::endl;
	}
};

struct MessageStats {
	MessageBufferType type;
	uint64_t bytes = 0;
	int ranges = 0;
	std::chrono::system_clock::time_point sendTS;
	MessageStats() = default;
};

std::string printObj(const MessageHeader& head);

std::string printObj(const ConsAdapter::serialization::UID& id);

std::string printObj(const ConsAdapter::serialization::GlobalVersion& v);

std::string printObj(const ConsAdapter::serialization::ReplicatorState& s);

std::string printObj(const ConsAdapter::serialization::Mutation& m);

std::string printObj(const ConsAdapter::serialization::KeyRange& kr);

std::string printObj(const UIDExt& id);

std::string printObj(const GlobalVersionExt& v);

std::string printObj(const ReplicatorStateExt& s);

std::string printRequestType(MessageBufferType type);

std::string printResponseType(MessageResponseType type);

// todo: create macro for logging
// todo: create 'detail' abstraction for json input to logger

#endif
