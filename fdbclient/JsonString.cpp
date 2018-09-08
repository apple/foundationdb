#include "JsonString.h"
#include <iostream>

JsonBuilderObject JsonBuilder::makeMessage(const char *name, const char *description) {
	JsonBuilderObject out;
	out["name"] = name;
	out["description"] = description;
	return out;
}
