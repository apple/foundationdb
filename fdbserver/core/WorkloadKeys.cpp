#include "flow/ParseNumber.h"
#include <cstdio>
#include <inttypes.h>

#include "fdbserver/core/WorkloadKeys.h"
#include "flow/flow.h"

Key doubleToTestKey(double p) {
	return StringRef(format("%016llx", *(uint64_t*)&p));
}

double testKeyToDouble(const KeyRef& p) {
	uint64_t x = parseNumberPrefix<uint64_t>(p, 16).orDefault(0);
	return *(double*)&x;
}

Key doubleToTestKey(double p, const KeyRef& prefix) {
	return doubleToTestKey(p).withPrefix(prefix);
}

double testKeyToDouble(const KeyRef& p, const KeyRef& prefix) {
	return testKeyToDouble(p.removePrefix(prefix));
}
