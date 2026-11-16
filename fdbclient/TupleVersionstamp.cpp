#include "fdbclient/TupleVersionstamp.h"

#include <cstring>

TupleVersionstamp::TupleVersionstamp(StringRef str) {
	if (str.size() != VERSIONSTAMP_TUPLE_SIZE) {
		throw invalid_versionstamp_size();
	}
	data = str;
}

TupleVersionstamp::TupleVersionstamp(int64_t version, uint16_t batchNumber, uint16_t userVersion) {
	data = makeString(VERSIONSTAMP_TUPLE_SIZE);
	uint8_t* buf = mutateString(data);
	const int64_t encodedVersion = bigEndian64(version);
	const uint16_t encodedBatchNumber = bigEndian16(batchNumber);
	const uint16_t encodedUserVersion = bigEndian16(userVersion);
	std::memcpy(buf, &encodedVersion, sizeof(encodedVersion));
	std::memcpy(buf + sizeof(encodedVersion), &encodedBatchNumber, sizeof(encodedBatchNumber));
	std::memcpy(
	    buf + sizeof(encodedVersion) + sizeof(encodedBatchNumber), &encodedUserVersion, sizeof(encodedUserVersion));
}

int16_t TupleVersionstamp::getBatchNumber() const {
	int16_t batchNumber;
	std::memcpy(&batchNumber, data.begin() + sizeof(int64_t), sizeof(batchNumber));
	batchNumber = bigEndian16(batchNumber);
	return batchNumber;
}

int16_t TupleVersionstamp::getUserVersion() const {
	int16_t userVersion;
	std::memcpy(&userVersion, data.begin() + sizeof(int64_t) + sizeof(uint16_t), sizeof(userVersion));
	userVersion = bigEndian16(userVersion);
	return userVersion;
}

const uint8_t* TupleVersionstamp::begin() const {
	return data.begin();
}

int64_t TupleVersionstamp::getVersion() const {
	int64_t version;
	std::memcpy(&version, data.begin(), sizeof(version));
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
