#include "JsonString.h"
#include <iostream>

std::string format( const char* form, ... );

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

void escape( const std::string& in, std::string& out ) {
	int beginCopy = 0;
	for (int i = 0; i < in.size(); i++) {
		if (shouldEscape(in[i])) {
			out.append(in, beginCopy, i - beginCopy);
			beginCopy = i + 1;
			out += '\\';
			out += in[i];
		}
	}
	if(beginCopy < in.size()) {
		out.append(in, beginCopy, in.size() - beginCopy);
	}
}

JsonString::JsonString() : hasKey(false) {
}
JsonString::JsonString( const JsonString& jsonString) : _jsonText(jsonString._jsonText), hasKey(jsonString.hasKey) {
}
JsonString::JsonString( const JsonStringArray& jsonArray) : hasKey(false) {
	append(jsonArray);
}

JsonString::JsonString( const std::string& value ) : hasKey(false) {
	append(value);
}
JsonString::JsonString( const char* value ) : hasKey(false) {
	append(value);
}

JsonString::JsonString( const json_spirit::mObject& value ) : hasKey(false) {
	_jsonText = json_spirit::write_string(json_spirit::mValue(value));
	_jsonText = _jsonText.substr(1,_jsonText.size()-2); //remove outer {}
	hasKey = !_jsonText.empty();
}

JsonString::JsonString( const std::string& name, const std::string& value ) : hasKey(false) {
	append(name, value);
}
JsonString::JsonString( const std::string& name, const char* value ) : hasKey(false) {
	append(name, value);
}
JsonString::JsonString( const std::string& name, double value ) : hasKey(false) {
	append(name, value);
}
JsonString::JsonString( const std::string& name, long int value ) : hasKey(false) {
	append(name, value);
}
JsonString::JsonString( const std::string& name, long unsigned int value ) : hasKey(false) {
	append(name, value);
}
JsonString::JsonString( const std::string& name, long long int value ) : hasKey(false) {
	append(name, value);
}
JsonString::JsonString( const std::string& name, long long unsigned int value ) : hasKey(false) {
	append(name, value);
}
JsonString::JsonString( const std::string& name, int value ) : hasKey(false) {
	append(name, value);
}
JsonString::JsonString( const std::string& name, unsigned value ) : hasKey(false) {
	append(name, value);
}
JsonString::JsonString( const std::string& name, bool value ) : hasKey(false) {
	append(name, value);
}
JsonString::JsonString( const std::string& name, const JsonString& value ) : hasKey(false) {
	append(name, value);
}
JsonString::JsonString( const std::string& name, const JsonStringArray& value ) : hasKey(false) {
	append(name, value);
}

JsonString JsonString::makeMessage(const char *name, const char *description) {
	JsonString out;
	out["name"] = name;
	out["description"] = description;
	return out;
}

JsonString& JsonString::appendImpl( const std::string& name, const std::string& value, bool quote ) {
	_jsonText.reserve(_jsonText.size() + name.size() + (quote ? (2*value.size() + 6) : (value.size() + 4)));
	hasKey = true;
	if(!_jsonText.empty()) {
		_jsonText += ',';
	}
	_jsonText += '"';
	_jsonText += name;
	_jsonText += "\":";
	if (quote) {
		_jsonText += "\"";
		escape(value, _jsonText);
		_jsonText += "\"";
	} else {
		_jsonText += value;
	}
	return *this;
}
JsonString& JsonString::appendImpl( const std::string& value, bool quote ) {
	_jsonText.reserve(_jsonText.size() + (quote ? (2*value.size() + 3) : (value.size() + 1)));
	if(!_jsonText.empty()) {
		_jsonText += ',';
	}
	if (quote) {
		_jsonText += "\"";
		escape(value, _jsonText);
		_jsonText += "\"";
	} else {
		_jsonText += value;
	}
	return *this;
}

std::string	JsonString::stringify(const char* value) {
	return std::string(value);
}
std::string	JsonString::stringify(double value) {
	return format("%g", value);
}
std::string	JsonString::stringify(long int value) {
	return format("%ld", value);
}
std::string	JsonString::stringify(long unsigned int value) {
	return format("%lu", value);
}
std::string	JsonString::stringify(long long int value) {
	return format("%lld", value);
}
std::string	JsonString::stringify(long long unsigned int value) {
	return format("%llu", value);
}
std::string	JsonString::stringify(int value) {
	return format("%d", value);
}
std::string	JsonString::stringify(unsigned value) {
	return format("%u", value);
}
std::string	JsonString::stringify(bool value) {
	return value ? "true" : "false";
}

JsonString& JsonString::append( const std::string& name, const std::string& value ) {
	return appendImpl(name, value, true);
}
JsonString& JsonString::append( const std::string& name, const char* value ) {
	return appendImpl(name, stringify(value), true);
}
JsonString& JsonString::append( const std::string& name, double value ) {
	return appendImpl(name, stringify(value), false);
}
JsonString& JsonString::append( const std::string& name, long int value ) {
	return appendImpl(name, stringify(value), false);
}
JsonString& JsonString::append( const std::string& name, long unsigned int value ) {
	return appendImpl(name, stringify(value), false);
}
JsonString& JsonString::append( const std::string& name, long long int value ) {
	return appendImpl(name, stringify(value), false);
}
JsonString& JsonString::append( const std::string& name, long long unsigned int value ) {
	return appendImpl(name, stringify(value), false);
}
JsonString& JsonString::append( const std::string& name, int value ) {
	return appendImpl(name, stringify(value), false);
}
JsonString& JsonString::append( const std::string& name, unsigned value ) {
	return appendImpl(name, stringify(value), false);
}
JsonString& JsonString::append( const std::string& name, bool value ) {
	return appendImpl(name, stringify(value), false);
}
JsonString& JsonString::append( const std::string& name, const JsonString& value ) {
	_jsonText.reserve(_jsonText.size() + name.size() + value._jsonText.size() + 6);
	hasKey = true;
	if(!_jsonText.empty()) {
		_jsonText += ',';
	}
	_jsonText += '\"';
	_jsonText += name;
	_jsonText += "\":{";
	_jsonText += value._jsonText;
	_jsonText += '}';
	return *this;
}
JsonString& JsonString::append( const std::string& name, const JsonStringArray& values ) {
	int valueBytes = 0;
	for (auto const& value : values) {
		valueBytes += value.getLength();
	}
	_jsonText.reserve(_jsonText.size() + name.size() + values.size() + valueBytes + 6);
	hasKey = true;
	if(!_jsonText.empty()) {
		_jsonText += ',';
	}
	_jsonText += '"';
	_jsonText += name;
	_jsonText += "\":[";
	bool first = true;
	for (auto const& value : values) {
		if (!first) {
			_jsonText += ',';
		}
		_jsonText += value.getJson();
		first = false;
	}
	_jsonText += ']';
	return *this;
}

JsonString& JsonString::append( const std::string& value ) {
	return appendImpl(value, true);
}
JsonString& JsonString::append( const char* value ) {
	return appendImpl(stringify(value), true);
}
JsonString& JsonString::append( double value ) {
	return appendImpl(stringify(value), false);
}
JsonString& JsonString::append( long int value ) {
	return appendImpl(stringify(value), false);
}
JsonString& JsonString::append( long unsigned int value ) {
	return appendImpl(stringify(value), false);
}
JsonString& JsonString::append( long long int value ) {
	return appendImpl(stringify(value), false);
}
JsonString& JsonString::append( long long unsigned int value ) {
	return appendImpl(stringify(value), false);
}
JsonString& JsonString::append( int value ) {
	return appendImpl(stringify(value), false);
}
JsonString& JsonString::append( unsigned value ) {
	return appendImpl(stringify(value), false);
}
JsonString& JsonString::append( bool value ) {
	return appendImpl(stringify(value), false);
}
JsonString& JsonString::append( const JsonString& value ) {
	// Only do something, if not empty
	if (!value.empty()) {
		_jsonText.reserve(_jsonText.size() + value._jsonText.size() + 1);
		if (!_jsonText.empty()) {
			_jsonText += ',';
		}
		_jsonText += value._jsonText;
		if(value.hasKey) {
			hasKey = true;
		}
	}
	return *this;
}
JsonString& JsonString::append( const JsonStringArray& values ) {
	int valueBytes = 0;
	for (auto const& value : values) {
		valueBytes += value.getLength();
	}
	_jsonText.reserve(_jsonText.size() + values.size() + valueBytes + 3);
	if (!_jsonText.empty()) {
		_jsonText += ',';
	}
	_jsonText += '[';
	bool first = true;
	for (auto const& value : values) {
		if (!first) {
			_jsonText += ',';
		}
		_jsonText += value.getJson();
		first = false;
	}
	_jsonText += ']';
	return *this;
}

JsonString& JsonString::clear() {
	_jsonText.clear();
	hasKey = false;
	return *this;
}

bool JsonString::empty() const {
	return _jsonText.empty();
}

const std::string& JsonString::getJsonText() const {
	return _jsonText;
}

size_t JsonString::getLength() const {
	return _jsonText.length() + ((!empty() && !hasKey) ? 0 : 2);
}

std::string JsonString::getJson() const {
	// If not empty with no names (only values), don't add brackets because prob in an array
	if (!empty() && !hasKey) {
		return _jsonText;
	} else {
		std::string result;
		result.reserve(_jsonText.size() + 2);
		result += '{';
		result += _jsonText;
		result += '}';
		return result;
	}
}

JsonString& JsonString::copy( const JsonString& jsonString ) {
	_jsonText = jsonString._jsonText;
	hasKey = jsonString.hasKey;
	return *this;
}

JsonString& JsonString::operator=( const JsonString& jsonString ) {
	return copy(jsonString);
}

int JsonString::compare( const JsonString& jsonString ) const {
	return jsonString._jsonText.compare(_jsonText);
}

bool JsonString::equals( const JsonString& jsonString ) const {
	return (compare(jsonString) == 0);
}


JsonStringArray::JsonStringArray() {
}
JsonStringArray::JsonStringArray( const JsonStringArray& arr) : std::vector<JsonString>(arr.begin(), arr.end()) {
}
JsonStringArray::~JsonStringArray() {
}

JsonStringSetter::JsonStringSetter( JsonString& jsonString, const std::string& name ) : _jsonString(jsonString), _name(name) {
}
