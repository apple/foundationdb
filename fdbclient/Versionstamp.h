#ifndef FDBCLIENT_VERSIONSTAMP_H
#define FDBCLIENT_VERSIONSTAMP_H

#pragma once

#include "flow/Arena.h"

const size_t VERSIONSTAMP_TUPLE_SIZE = 12;

struct Versionstamp {
	Versionstamp(StringRef);

	int64_t getVersion() const;
	int16_t getBatchNumber() const;
	int16_t getClientWrittenNumber() const;
	size_t size() const;
	const uint8_t* begin() const;
	bool operator==(const Versionstamp&) const;

private:
	Standalone<StringRef> data;
};

#endif