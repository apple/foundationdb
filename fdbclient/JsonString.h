
#ifndef JSONSTRING_H
#define JSONSTRING_H

#include <string>
#include <vector>
#include "flow/flow.h"
#include "flow/Trace.h"
#include "fdbrpc/JSONDoc.h"

class JsonString;
template <typename T> class JsonStringSetter;

// Class for building JSON string values.  
// Default value is 'empty' which is different from JSON null
class JsonString {
public:
	enum EType { EMPTY, NULLVALUE, SCALAR, OBJECT, ARRAY, FINAL, DESTROYED };
	JsonString(EType type = EType::EMPTY) : type(type)
	{
		if(type == EType::NULLVALUE) {
			write("null");
		}
		else if(type == EType::ARRAY) {
			write("[");
		}
		else if(type == EType::OBJECT) {
			write("{");
		}
	}

	int empty() const {
		return type == EType::EMPTY || (jsonText.size() == 1 && (type == EType::ARRAY || type == EType::OBJECT));
	}
	
	void clear() {
		type = EMPTY;
		jsonText.clear();
	}

	// Finish the JSON value - close array or object if still open and set final state in either case
	// Scalar and null types stay as they are
	void finalize() {
		ASSERT(type != EType::DESTROYED);
		if(type != EType::FINAL) {
			if(type == EType::ARRAY) {
				write("]");
				type = EType::FINAL;
			}
			else if(type == EType::OBJECT) {
				write("}");
				type = EType::FINAL;
			}
		}
	}

	int getFinalLength() {
		finalize();
		return jsonText.size();
	}

	std::string getJson() {
		finalize();
		return jsonText;
	}

	// Can add value to an array or turn an empty into a scalar
	template<typename VT> inline JsonString & append(VT &&val) {
		if(type == EType::ARRAY) {
			if(jsonText.size() != 1)
				write(",");
		}
		else {
			ASSERT(type == EType::EMPTY);
			type = EType::SCALAR;
		}

		writeValue(val);
		return *this;
	}

	template<typename KT, typename VT> inline JsonString & append(KT &&name, VT &&val) {
		if(type == EType::EMPTY) {
			type = EType::OBJECT;
			write("{");
		}
		else {
			ASSERT(type == EType::OBJECT);
			if(jsonText.size() != 1)
				write(",");
		}

		write("\"");
		write(name);
		write("\":");
		// The easy alternative to this is using format() which also creates a temporary string, so let's 
		// let json_spirit handle stringification of native types for now.
		writeValue(val);
		return *this;
	}

	template<typename T> inline JsonStringSetter<T> operator[](T &&name);

	static JsonString makeMessage(const char *name, const char *description);

protected:
	EType type;
	std::string jsonText;

	template<typename T> inline void write(T &&s) {
		jsonText.append(std::forward<T>(s));
	}

	template<typename T> inline void writeValue(T &&val) {
		write(json_spirit::write_string(json_spirit::mValue(val)));
	}
};

// A JsonString is already a proper JSON string (or empty)
template<> inline void JsonString::writeValue(const JsonString &val) {
	// Convert empty to null because in this context we must write something.
	if(val.type == EType::EMPTY) {
		write("null");
	}
	else {
		write(const_cast<JsonString &>(val).getJson());
	}
}

template<> inline void JsonString::writeValue(JsonString &val) {
	writeValue<const JsonString &>(val);
}

// "Deep" append of mObject based on type of this
// Add kv pairs individually for empty or object, add as a closed object for array
template<> inline JsonString & JsonString::append(const json_spirit::mObject &obj) {
	if(type == EType::EMPTY || type == EType::OBJECT) {
		for(auto &kv : obj) {
			append(kv.first, kv.second);
		}
	}
	else {
		ASSERT(type == EType::ARRAY);
		append(json_spirit::mValue(obj));
	}
	return *this;
}

// Starts as an Array type and adds push_back interface to appear array-like for writing
class JsonStringArray : protected JsonString {
public:
	JsonStringArray() : JsonString(EType::ARRAY) {}

	template<typename VT> void push_back(VT &&o) {
		append(std::forward<VT>(o));
		++count;
	}

	// Clear but maintain Array-ness
	void clear() {
		clear();
		type = EType::ARRAY;
	}

	int size() const {
		return count;
	}

protected:
	int count;
};

// A JsonString is already a proper JSON string (or empty)
template<> inline void JsonString::writeValue(JsonStringArray &val) {
	writeValue((JsonString &)val);
}

// Template is the key name, accepted as an r-value to avoid copying if it's a string
template<typename KT>
class JsonStringSetter {
public:
	JsonStringSetter( JsonString& dest, KT &&name) : dest(dest), name(std::forward<KT>(name)) {}

	template <class VT> inline JsonStringSetter& operator=(VT &&value) {
		dest.append(name, std::forward<VT>(value));
		return *this;
	}

protected:
	JsonString& dest;
	KT name;
};

template<typename T> inline JsonStringSetter<T> JsonString::operator[](T &&name) {
	return JsonStringSetter<T>(*this, std::forward<T>(name));
}

#endif
