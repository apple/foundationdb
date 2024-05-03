#pragma once

#include <string>
#include <vector>
#include <cmath>
#include "flow/flow.h"
#include "flow/Trace.h"
#include "fdbclient/JSONDoc.h"

class JsonBuilder;
class JsonBuilderObject;
class JsonBuilderArray;
typedef JsonBuilder JsonString;
template <typename T>
class JsonBuilderObjectSetter;

// Class for building JSON strings linearly.
// JSON data structure is only appendable.  No key deduplication is done in JSON Objects, and the output is not readable
// other than obtaining a complete JSON string of what has been written to the builder. Default value is null, as in the
// JSON type
class JsonBuilder {
protected:
	enum EType { NULLVALUE, OBJECT, ARRAY };

	typedef VectorRef<char> VString;

public:
	// Default value is null, which will be considered "empty"
	JsonBuilder() : type(NULLVALUE), elements(0), bytes(0) { jsonText.resize(arena, 1); }

	int getFinalLength() const { return bytes + strlen(getEnd()); }

	// TODO: Remove the need for this by changing usages to steal this's content
	std::string getJson() const {
		std::string result;
		result.reserve(bytes + 1);
		for (auto& it : jsonText) {
			result.append(it.begin(), it.end());
		}
		result.append(getEnd());
		return result;
	}

	int size() const { return elements; }

	bool empty() const { return elements == 0; }

	static JsonBuilderObject makeMessage(const char* name, const char* description);

	static int coerceAsciiNumberToJSON(const char* s, int len, char* dst);

protected:
	EType type;
	Arena arena;
	mutable VectorRef<VString> jsonText;
	int elements;
	int bytes;

	// 'raw' write methods
	inline void write(const char* s, int len) {
		bytes += len;
		jsonText.back().append(arena, s, len);
	}

	inline void write(const char* s) { write(s, strlen(s)); }

	inline void write(const StringRef& s) { write((char*)s.begin(), s.size()); }

	inline void write(char s) {
		++bytes;
		jsonText.back().push_back(arena, s);
	}

	// writeValue() methods write JSON form of the value
	void writeValue(const json_spirit::mValue& val) {
		switch (val.type()) {
		case json_spirit::int_type:
			return writeValue(val.get_int64());
		case json_spirit::bool_type:
			return writeValue(val.get_bool());
		case json_spirit::real_type:
			return writeValue(val.get_real());
		case json_spirit::str_type:
			return writeValue(val.get_str());
		default:
			// Catch-all for objects/arrays
			return write(json_spirit::write_string(val));
		};
	}

	void writeValue(const bool& val) { write(val ? "true" : "false"); }

	template <typename T>
	inline void writeFormat(const char* fmt, const T& val) {
		VString& dst = jsonText.back();
		const int limit = 30;
		dst.reserve(arena, dst.size() + limit);
		int len = snprintf(dst.end(), limit, fmt, val);
		if (len > 0 && len < limit) {
			dst.extendUnsafeNoReallocNoInit(len);
		} else {
			write(format(fmt, val));
		}
	}

	void writeValue(const int64_t& val) { writeFormat("%lld", val); }

	void writeValue(const uint64_t& val) { writeFormat("%llu", val); }

	void writeValue(const int& val) { writeFormat("%d", val); }

	void writeValue(const double& val) {
		if (std::isfinite(val)) {
			writeFormat("%g", val);
		} else if (std::isnan(val)) {
			write("-999");
		} else {
			write("1e99");
		}
	}

	bool shouldEscape(char c) {
		switch (c) {
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

	void writeValue(const char* val, int len) {
		write('"');
		int beginCopy = 0;
		VString& dst = jsonText.back();
		for (int i = 0; i < len; i++) {
			if (shouldEscape(val[i])) {
				dst.append(arena, val + beginCopy, i - beginCopy);
				beginCopy = i + 1;
				write('\\');
				write(val[i]);
			}
		}
		if (beginCopy < len) {
			dst.append(arena, val + beginCopy, len - beginCopy);
		}
		write('"');
	}

	inline void writeValue(const std::string& val) { writeValue(val.data(), val.size()); }

	inline void writeValue(const char* val) { writeValue(val, strlen(val)); }

	inline void writeValue(const StringRef& s) { writeValue((const char*)s.begin(), s.size()); }

	// Write the finalized (closed) form of val
	void writeValue(const JsonBuilder& val) {
		bytes += val.bytes;
		jsonText.append(arena, val.jsonText.begin(), val.jsonText.size());
		val.jsonText.push_back(arena, VString());
		arena.dependsOn(val.arena);
		write(val.getEnd());
	}

	void writeCoercedAsciiNumber(const char* s, int len) {
		VString& val = jsonText.back();
		val.reserve(arena, val.size() + len + 3);
		int written = coerceAsciiNumberToJSON(s, len, val.end());
		if (written > 0) {
			val.extendUnsafeNoReallocNoInit(written);
		} else {
			write("-999");
		}
	}

	inline void writeCoercedAsciiNumber(const StringRef& s) {
		writeCoercedAsciiNumber((const char*)s.begin(), s.size());
	}

	inline void writeCoercedAsciiNumber(const std::string& s) { writeCoercedAsciiNumber(s.data(), s.size()); }

	// Helper function to add contents of another JsonBuilder to this one.
	// This is only used by the subclasses to combine like-typed (at compile time) objects,
	// so it can be assumed that the other object has been initialized with an opening character.
	void _addContents(const JsonBuilder& other) {
		if (other.empty()) {
			return;
		}

		if (elements > 0) {
			write(',');
		}

		// Add everything but the first byte of the first string in arr
		bytes += other.bytes - 1;
		const VString& front = other.jsonText.front();
		jsonText.push_back(arena, front.slice(1, front.size()));
		jsonText.append(arena, other.jsonText.begin() + 1, other.jsonText.size() - 1);

		// Both JsonBuilders would now want to write to the same additional VString capacity memory
		// if they were modified, so force the other (likely to not be modified again) to start a new one.
		other.jsonText.push_back(arena, VString());

		arena.dependsOn(other.arena);
		elements += other.elements;
	}

	// Get the text necessary to finish the JSON string
	const char* getEnd() const {
		switch (type) {
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

	template <typename VT>
	inline JsonBuilderArray& push_back(const VT& val) {
		if (elements++ > 0) {
			write(',');
		}
		writeValue(val);
		return *this;
	}

	JsonBuilderArray& addContents(const json_spirit::mArray& arr) {
		for (auto& v : arr) {
			push_back(v);
		}
		return *this;
	}

	JsonBuilderArray& addContents(const JsonBuilderArray& arr) {
		_addContents(arr);
		return *this;
	}
};

class JsonBuilderObject : public JsonBuilder {
public:
	JsonBuilderObject() {
		type = OBJECT;
		write('{');
	}

	template <typename KT, typename VT>
	inline JsonBuilderObject& setKey(const KT& name, const VT& val) {
		if (elements++ > 0) {
			write(',');
		}
		write('"');
		write(name);
		write("\":");
		writeValue(val);
		return *this;
	}

	template <typename KT, typename VT>
	inline JsonBuilderObject& setKeyRawNumber(const KT& name, const VT& val) {
		if (elements++ > 0) {
			write(',');
		}
		write('"');
		write(name);
		write("\":");
		writeCoercedAsciiNumber(val);
		return *this;
	}

	template <typename T>
	inline JsonBuilderObjectSetter<T> operator[](T&& name);

	JsonBuilderObject& addContents(const json_spirit::mObject& obj) {
		for (auto& kv : obj) {
			setKey(kv.first, kv.second);
		}
		return *this;
	}

	JsonBuilderObject& addContents(const JsonBuilderObject& obj) {
		_addContents(obj);
		return *this;
	}
};

// Template is the key name, accepted as an r-value if possible to avoid copying if it's a string
template <typename KT>
class JsonBuilderObjectSetter {
public:
	JsonBuilderObjectSetter(JsonBuilderObject& dest, KT&& name) : dest(dest), name(std::forward<KT>(name)) {}

	// Value is accepted as an rvalue if possible
	template <class VT>
	inline void operator=(const VT& value) {
		dest.setKey(name, value);
	}

protected:
	JsonBuilderObject& dest;
	KT name;
};

template <typename T>
inline JsonBuilderObjectSetter<T> JsonBuilderObject::operator[](T&& name) {
	return JsonBuilderObjectSetter<T>(*this, std::forward<T>(name));
}
