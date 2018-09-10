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
	enum EType { NULLVALUE, OBJECT, ARRAY };

public:
	// Default value is null, which will be considered "empty"
	JsonBuilder() : type(NULLVALUE), elements(0), bytes(0) {}

	int getFinalLength() const {
		return bytes + strlen(getEnd());
	}

	// TODO: Remove the need for this by changing usages to steal this's content
	std::string getJson() const {
		std::string result;
		result.reserve(bytes + 1);
		for(auto& it : jsonText) {
			result.append(it.begin(), it.end());
		}
		result.append(getEnd());
		return result;
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
	Arena arena;
	mutable std::vector<VectorRef<uint8_t>> jsonText;
	int elements;
	int bytes;

	inline void write( const std::string& s) {
		if(!jsonText.size()) {
			jsonText.push_back( VectorRef<uint8_t>() );
		}
		bytes += s.size();
		jsonText.back().append(arena, (const uint8_t*)&s[0], s.size());
		//printf("%p: after write: '%s'\n", this, jsonText.c_str());
	}

	inline void write(const char* s) {
		if(!jsonText.size()) {
			jsonText.push_back( VectorRef<uint8_t>() );
		}
		bytes += strlen(s);
		jsonText.back().append(arena, (const uint8_t*)&s[0], strlen(s));
		//printf("%p: after write: '%s'\n", this, jsonText.c_str());
	}

	inline void write(char s) {
		if(!jsonText.size()) {
			jsonText.push_back( VectorRef<uint8_t>() );
		}
		bytes++;
		jsonText.back().push_back(arena, s);
		//printf("%p: after write: '%s'\n", this, jsonText.c_str());
	}

	// Catch-all for json spirit types
	// TODO:  Reduce this to just mArray and mValue, and provide faster version for other types
	void writeValue(const json_spirit::mValue &val) {
		write(json_spirit::write_string(val));
	}

	void writeValue(const bool& val) {
		write(val ? "true" : "false");
	}

	void writeValue(const int64_t& val) {
		write(format("%lld",val));
	}

	void writeValue(const uint64_t& val) {
		write(format("%llu",val));
	}

	void writeValue(const int& val) {
		write(format("%d",val));
	}

	void writeValue(const double& val) {
		write(format("%g",val));
	}

	bool shouldEscape(char c) {
		switch( c ) {
			case '"':
			case '\\':
			case '\b':
			case '\f':
			case '\n':
			case '\r':
			case '\t':
				return true;
			default:
				return false;
		}
	}

	void writeValue(const std::string& val) {
		write('"');
		int beginCopy = 0;
		for (int i = 0; i < val.size(); i++) {
			if (shouldEscape(val[i])) {
				jsonText.back().append(arena, (const uint8_t*)&(val[beginCopy]), i - beginCopy);
				beginCopy = i + 1;
				write('\\');
				write(val[i]);
			}
		}
		if(beginCopy < val.size()) {
			jsonText.back().append(arena, (const uint8_t*)&(val[beginCopy]), val.size() - beginCopy);
		}
		write('"');
	}

	// Write the finalized (closed) form of val
	void writeValue(const JsonBuilder &val) {
		bytes += val.bytes;
		for(auto& it : val.jsonText) {
			jsonText.push_back(it);
		}
		val.jsonText.push_back(VectorRef<uint8_t>());
		arena.dependsOn(val.arena);
		write(val.getEnd());
	}

	// Get the text necessary to finish the JSON string
	const char * getEnd() const {
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
		write('[');
	}

	template<typename VT> inline JsonBuilderArray & push_back(VT &&val) {
		if(elements++ > 0) {
			write(',');
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
		if(!arr.jsonText.size()) {
			return *this;
		}

		bytes += arr.bytes - 1;
		jsonText.push_back(VectorRef<uint8_t>((uint8_t*)&arr.jsonText[0][1], arr.jsonText[0].size()-1));
		for(int i = 1; i < arr.jsonText.size(); i++) {
			jsonText.push_back(arr.jsonText[i]);
		}
		arr.jsonText.push_back(VectorRef<uint8_t>());
		arena.dependsOn(arr.arena);

		elements += arr.elements;
		return *this;
	}
};

class JsonBuilderObject : public JsonBuilder {
public:
	JsonBuilderObject() {
		type = OBJECT;
		write('{');
	}

	template<typename KT, typename VT> inline JsonBuilderObject & setKey(KT &&name, VT &&val) {
		if(elements++ > 0) {
			write(',');
		}
		write('"');
		write(name);
		write('"');
		write(':');
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
		if(!obj.jsonText.size()) {
			return *this;
		}

		bytes += obj.bytes - 1;
		jsonText.push_back(VectorRef<uint8_t>((uint8_t*)&obj.jsonText[0][1], obj.jsonText[0].size()-1));
		for(int i = 1; i < obj.jsonText.size(); i++) {
			jsonText.push_back(obj.jsonText[i]);
		}
		obj.jsonText.push_back(VectorRef<uint8_t>());
		arena.dependsOn(obj.arena);

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

