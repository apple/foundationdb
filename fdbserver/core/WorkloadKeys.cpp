#include <cstdio>
#include <inttypes.h>

#include "fdbserver/core/WorkloadKeys.h"
#include "flow/flow.h"

Key doubleToTestKey(double p) {
	return StringRef(format("%016llx", std::bit_cast<uint64_t>(p)));
}

double testKeyToDouble(const KeyRef& p) {
	uint64_t x = 0;
	sscanf(p.toString().c_str(), "%" SCNx64, &x);
	return std::bit_cast<double>(x);
}

Key doubleToTestKey(double p, const KeyRef& prefix) {
	return doubleToTestKey(p).withPrefix(prefix);
}

double testKeyToDouble(const KeyRef& p, const KeyRef& prefix) {
	return testKeyToDouble(p.removePrefix(prefix));
}
