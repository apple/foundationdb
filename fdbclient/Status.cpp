#include "flow/flow.h"
#include "Status.h"

JsonString	StatusObject::toJsonString() const {
	JsonString	jsonStringObj;
	std::string jsonText = json_spirit::write_string(json_spirit::mValue(*this));
	jsonStringObj.setJson(jsonText);
	return jsonStringObj;
}
