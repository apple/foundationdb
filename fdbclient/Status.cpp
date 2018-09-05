#include "flow/flow.h"
#include "Status.h"

JsonString	StatusObject::toJsonString() const {
	std::string jsonText = json_spirit::write_string(json_spirit::mValue(*this));
	return JsonString().swapJsonText(jsonText);
}
