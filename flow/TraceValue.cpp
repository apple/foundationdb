#include "TraceValue.h"

namespace {

std::string toString(const TraceString& traceString) {
	return traceString.value;
}

std::string toString(const TraceString& trac)

} // namespace

TraceValue::toString() const {
	return std::visit([](const auto& val) { return val.toString() });
}
