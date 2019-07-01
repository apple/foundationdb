#include "ConsumerAdapterUtils.h"
using namespace std;
using namespace ConsAdapter::serialization;

uint64_t MessageBuffer::nextUID = 0;

// initialized for writing
MessageBuffer::MessageBuffer() : serializer(1024) {
	id = ++nextUID;
}
// access the request
const GetReplicatorStateReq* MessageBuffer::getGetReplicatorStateReq() {
	assert(type == MessageBufferType::T_GetReplicatorStateReq);
	auto polevaultReq = GetConsumerAdapterRequest(static_cast<const void*>(readBuffer.data()));
	return static_cast<const GetReplicatorStateReq*>(polevaultReq->request());
}
const SetReplicatorStateReq* MessageBuffer::getSetReplicatorStateReq() {
	assert(type == MessageBufferType::T_SetReplicatorStateReq);
	auto polevaultReq = GetConsumerAdapterRequest(static_cast<const void*>(readBuffer.data()));
	return static_cast<const SetReplicatorStateReq*>(polevaultReq->request());
}
const PushBatchReq* MessageBuffer::getPushBatchReq() {
	assert(type == MessageBufferType::T_PushBatchReq);
	auto polevaultReq = GetConsumerAdapterRequest(static_cast<const void*>(readBuffer.data()));
	return static_cast<const PushBatchReq*>(polevaultReq->request());
}
const VerifyRangeReq* MessageBuffer::getVerifyRangeReq() {
	assert(type == MessageBufferType::T_VerifyRangeReq);
	auto polevaultReq = GetConsumerAdapterRequest(static_cast<const void*>(readBuffer.data()));
	return static_cast<const VerifyRangeReq*>(polevaultReq->request());
}

const flatbuffers::Vector<flatbuffers::Offset<Mutation>>* MessageBuffer::getMutations() {
	auto pushReq = getPushBatchReq();
	return pushReq->mutations();
}

const flatbuffers::Vector<flatbuffers::Offset<KeyRange>>* MessageBuffer::getRanges() {
	auto pushReq = getVerifyRangeReq();
	return pushReq->ranges();
}

uint32_t MessageBuffer::getChecksum(int index) {
	auto pushReq = getVerifyRangeReq();
	return pushReq->checksums()->Get(index);
}

const flatbuffers::Vector<unsigned>* MessageBuffer::getChecksums() {
	auto pushReq = getVerifyRangeReq();
	return pushReq->checksums();
}

ReplicatorStateExt MessageBuffer::getRepState() {
	switch (type) {
	case (MessageBufferType::T_GetReplicatorStateReq):
		return ReplicatorStateExt(*getGetReplicatorStateReq()->repState());
	case (MessageBufferType::T_SetReplicatorStateReq):
		return ReplicatorStateExt(*getSetReplicatorStateReq()->repState());
	case (MessageBufferType::T_PushBatchReq):
		return ReplicatorStateExt(*getPushBatchReq()->repState());
	case (MessageBufferType::T_VerifyRangeReq):
		return ReplicatorStateExt(*getVerifyRangeReq()->repState());
	default:
		assert(1);
		return ReplicatorStateExt();
	}
}

const ReplicatorState* MessageBuffer::getRepStateData() {
	switch (type) {
	case (MessageBufferType::T_GetReplicatorStateReq):
		return getGetReplicatorStateReq()->repState();
	case (MessageBufferType::T_SetReplicatorStateReq):
		return getSetReplicatorStateReq()->repState();
	case (MessageBufferType::T_PushBatchReq):
		return getPushBatchReq()->repState();
	case (MessageBufferType::T_VerifyRangeReq):
		return getVerifyRangeReq()->repState();
	default:
		assert(1);
		return getGetReplicatorStateReq()->repState();
	}
}

std::string printRequestType(MessageBufferType type) {
	switch (type) {
	case (MessageBufferType::T_GetReplicatorStateReq):
		return "T_GetRepStateReq";
	case (MessageBufferType::T_SetReplicatorStateReq):
		return "T_SetRepStateReq";
	case (MessageBufferType::T_PushBatchReq):
		return "T_PushBatchReq";
	case (MessageBufferType::T_VerifyRangeReq):
		return "T_VerifyRangeReq";
	default:
		return "UNINITED";
	}
}

std::string printResponseType(MessageResponseType type) {
	switch (type) {
	case (MessageResponseType::T_ReplyResp):
		return "T_ReplyResp";
	case (MessageResponseType::T_FinishResp):
		return "T_FinishResp";
	default:
		return "UNINITED";
	}
}

std::string MessageBuffer::toStr() {
	return fmt::format("message id:{} endpoint:{} type:{} replyType:{} curErr:{} "
	                   " curVerifyRange:{}",
	                   id, endpoint, printRequestType(type), printResponseType(respType), error, curVerifyRange);
}

bool MessageBuffer::checkReplicatorIDRegistry(std::shared_ptr<Log> log) {
	auto reqState = getRepState();
	if (replyRepState.id == reqState.id) {
		return true;
	} else {
		log->trace("ReplicatorIDDoesntMatchError", { { "cur", replyRepState.toStr() }, { "req", reqState.toStr() } });
		return false;
	}
}

#ifndef INGEST_ADAPTER_SIM_TEST
void MessageBuffer::prepareWrite() {
	Crc32 crc;
	header = MessageHeader(serializer.GetSize(), crc.sum(serializer.GetBufferPointer(), serializer.GetSize()));
	// TODO: make vector of buffer references...
	writeBuffers.clear();
	writeBuffers.push_back(boost::asio::buffer((void*)&header, sizeof(header)));
	writeBuffers.push_back(boost::asio::buffer(serializer.GetBufferPointer(), serializer.GetSize()));
	readyForWrite = true;
}
#endif

uint64_t rand64() {
	uint64_t ret = ((uint64_t)rand() << 32) + rand();
	return ret;
};

bool operator==(UID const& l, UID const& r) {
	return l.part1() == r.part1() && l.part2() == r.part2();
};

bool operator!=(UID const& l, UID const& r) {
	return l.part1() != r.part1() || l.part2() != r.part2();
};

std::string printObj(const MessageHeader& head) {
	return fmt::format("(size:{} sum:{})", head.size, head.checksum);
};

std::string printObj(const UID& id) {
	return fmt::format("({}:{})", id.part1(), id.part2());
};

std::string printObj(const GlobalVersion& v) {
	return fmt::format("(sw:{}, v:{}, prev:{})", v.switchCount(), v.version(), v.previousVersion());
};

std::string printObj(const ReplicatorState& s) {
	return fmt::format("(id:{}, lastRepVersion:{})", printObj(s.id()), printObj(s.lastRepVersion()));
};

// need to do something special for serialized strings
std::string printObj(const Mutation& m) {
	return fmt::format("(type:{}, key:{}, val:{})", m.type(), m.param1()->str(), m.param2()->str());
};

std::string printObj(const KeyRange& kr) {
	return fmt::format("(begin:{}, end:{})", kr.begin()->str(), kr.end()->str());
};

std::string printObj(const UIDExt& id) {
	return id.toStr();
};

std::string printObj(const GlobalVersionExt& v) {
	return v.toStr();
};

std::string printObj(const ReplicatorStateExt& s) {
	return s.toStr();
};
