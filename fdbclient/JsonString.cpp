#include "JsonString.h"
#include <iostream>

JsonString JsonString::makeMessage(const char *name, const char *description) {
	JsonString out;
	out["name"] = name;
	out["description"] = description;
	return out;
}
