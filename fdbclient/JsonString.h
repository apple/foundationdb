#pragma once

#include <string>
#include <vector>
#include "flow/flow.h"
#include "flow/Trace.h"
#include "fdbrpc/JSONDoc.h"

class JsonBuilder;
class JsonBuilderObject;
class JsonBuilderArray;
typedef JsonBuilder JsonString;
template <typename T> class JsonBuilderObjectSetter;

// Class for building JSON string values.  
// Default value is null, as in the JSON type
class JsonBuilder {
protected:
	// Destroyed state is for when the contents can be std::moved'd somewhere useful efficiently
	enum EType { NULLVALUE, OBJECT, ARRAY, DESTROYED };

public:
	// Default value is null, which will be considered "empty"
	JsonBuilder() : type(NULLVALUE), elements(0) {}

	int getFinalLength() const {
		return jsonText.size() + strlen(getEnd());
	}

	// TODO: Remove the need for this by changing usages to steal this's content
	std::string getJson() const {
		return jsonText + getEnd();
	}

	int size() const {
		return elements;
	}

	bool empty() const {
		return elements == 0;
	}

	static JsonBuilderObject makeMessage(const char *name, const char *description);

protected:
	EType type;
	std::string jsonText;
	int elements;

	template<typename T> inline void write(T &&s) {
		jsonText.append(std::forward<T>(s));
		//printf("%p: after write: '%s'\n", this, jsonText.c_str());
	}

	// Catch-all for json spirit types
	// TODO:  Reduce this to just mArray and mValue, and provide faster version for other types
	void writeValue(const json_spirit::mValue &val) {
		write(json_spirit::write_string(val));
	}

	// Write the finalized (closed) form of val
	void writeValue(const JsonBuilder &val) {
		write(val.jsonText);
		write(val.getEnd());
	}

	// Get the text necessary to finish the JSON string
	const char * getEnd() const {
		ASSERT(type != DESTROYED);

		switch(type) {
			case NULLVALUE:
				return "null";
			case OBJECT:
				return "}";
			case ARRAY:
				return "]";
			default:
				return "";
		};
	}
};

class JsonBuilderArray : public JsonBuilder {
public:
	JsonBuilderArray() {
		type = ARRAY;
		write("[");
	}

	template<typename VT> inline JsonBuilderArray & push_back(VT &&val) {
		if(elements++ > 0) {
			write(",");
		}
		writeValue(std::forward<VT>(val));
		return *this;
	}

	JsonBuilderArray & addContents(const json_spirit::mArray &arr) {
		for(auto &v : arr) {
			push_back(v);
		}
		return *this;
	}

	JsonBuilderArray & addContents(const JsonBuilderArray &arr) {
		jsonText.append(arr.jsonText.substr(1));
		elements += arr.elements;
		return *this;
	}
};

class JsonBuilderObject : public JsonBuilder {
public:
	JsonBuilderObject() {
		type = OBJECT;
		write("{");
	}

	template<typename KT, typename VT> inline JsonBuilderObject & setKey(KT &&name, VT &&val) {
		if(elements++ > 0) {
			write(",");
		}
		write("\"");
		write(std::forward<KT>(name));
		write("\":");
		writeValue(std::forward<VT>(val));
		return *this;
	}

	template<typename T> inline JsonBuilderObjectSetter<T> operator[](T &&name);

	JsonBuilderObject & addContents(const json_spirit::mObject &obj) {
		for(auto &kv : obj) {
			setKey(kv.first, kv.second);
		}
		return *this;
	}

	JsonBuilderObject & addContents(const JsonBuilderObject &obj) {
		jsonText.append(obj.jsonText.substr(1));
		elements += obj.elements;
		return *this;
	}
};

// Template is the key name, accepted as an r-value if possible to avoid copying if it's a string
template<typename KT>
class JsonBuilderObjectSetter {
public:
	JsonBuilderObjectSetter(JsonBuilderObject &dest, KT &&name) : dest(dest), name(std::forward<KT>(name)) {}

	// Value is accepted as an rvalue if possible
	template <class VT> inline void operator=(VT &&value) {
		dest.setKey(name, std::forward<VT>(value));
	}

protected:
	JsonBuilderObject& dest;
	KT name;
};

template<typename T> inline JsonBuilderObjectSetter<T> JsonBuilderObject::operator[](T &&name) {
	return JsonBuilderObjectSetter<T>(*this, std::forward<T>(name));
}

