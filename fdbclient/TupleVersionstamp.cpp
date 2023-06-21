#include "fdbclient/TupleVersionstamp.h"

TupleVersionstamp::TupleVersionstamp(StringRef str) {
	if (str.size() != VERSIONSTAMP_TUPLE_SIZE) {
		throw invalid_versionstamp_size();
	}
	data = str;
}

TupleVersionstamp::TupleVersionstamp(int64_t version, uint16_t batchNumber, uint16_t userVersion) {
	data = makeString(VERSIONSTAMP_TUPLE_SIZE);
	uint8_t* buf = mutateString(data);
	*reinterpret_cast<int64_t*>(buf) = bigEndian64(version);
	*reinterpret_cast<uint16_t*>(buf + sizeof(int64_t)) = bigEndian16(batchNumber);
	*reinterpret_cast<uint16_t*>(buf + sizeof(int64_t) + sizeof(uint16_t)) = bigEndian16(userVersion);
}

int16_t TupleVersionstamp::getBatchNumber() const {
	const uint8_t* begin = data.begin();
	begin += 8;
	int16_t batchNumber = *(int16_t*)(begin);
	batchNumber = bigEndian16(batchNumber);
	return batchNumber;
}

int16_t TupleVersionstamp::getUserVersion() const {
	const uint8_t* begin = data.begin();
	begin += 10;
	int16_t userVersion = *(int16_t*)(begin);
	userVersion = bigEndian16(userVersion);
	return userVersion;
}

const uint8_t* TupleVersionstamp::begin() const {
	return data.begin();
}

int64_t TupleVersionstamp::getVersion() const {
	const uint8_t* begin = data.begin();
	int64_t version = *(int64_t*)begin;
	version = bigEndian64(version);
	return version;
}

size_t TupleVersionstamp::size() const {
	return VERSIONSTAMP_TUPLE_SIZE;
}

bool TupleVersionstamp::operator==(const TupleVersionstamp& other) const {
	return getVersion() == other.getVersion() && getBatchNumber() == other.getBatchNumber() &&
	       getUserVersion() == other.getUserVersion();
}