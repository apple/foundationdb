#include "JsonString.h"
#include "Hash3.h"
#include <iostream>
#include "Trace.h"
#include "flow.h"

std::string format( const char* form, ... );

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
	hasKey = true;
	_jsonText += (_jsonText.empty() ? "\"" : ",\n  \"") + name + (quote ? "\": \"" : "\": ") + value;
	if (quote)
		_jsonText += "\"";
	return *this;
}
JsonString& JsonString::appendImpl( const std::string& value, bool quote ) {
	if (quote) {
		_jsonText += (_jsonText.empty() ? "\"" : ", \"") + value + "\"";
	}
	else {
		if (_jsonText.empty())
			_jsonText += ", ";
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
	hasKey = true;
	_jsonText += (_jsonText.empty() ? "\"" : ",\n  \"") + name + "\": { " + value._jsonText + " }";
	return *this;
}
JsonString& JsonString::append( const std::string& name, const JsonStringArray& values ) {
	hasKey = true;
	_jsonText += (_jsonText.empty() ? "\"" : ",\n  \"") + name + "\": [ ";
	size_t counter = 0;
	for (auto const& value : values) {
		if (counter)
			_jsonText += ",\n  ";
		_jsonText += value.getJson();
		counter ++;
	}
	_jsonText += " ]";
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
		if (!_jsonText.empty())
			_jsonText += ",\n  ";
		_jsonText += value._jsonText;
		if(value.hasKey) {
			hasKey = true;
		}
	}
	return *this;
}
JsonString& JsonString::append( const JsonStringArray& values ) {
	_jsonText += _jsonText.empty() ? "[ " : ",\n  [ ";
	size_t counter = 0;
	for (auto const& value : values) {
		if (counter)
			_jsonText += ",\n  ";
		_jsonText += value.getJson();
		counter ++;
	}
	_jsonText += " ]";
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
	return _jsonText.length() + ((!empty() && !hasKey) ? 0 : 4);
}

std::string JsonString::getJson() const {
	// If not empty with no names (only values), don't add brackets because prob in an array
	return (!empty() && !hasKey) ? _jsonText : ("{ " + _jsonText + " }");
}

JsonString& JsonString::copy( const JsonString& jsonString ) {
	_jsonText = jsonString._jsonText;
	hasKey = jsonString.hasKey;
	return *this;
}

JsonString& JsonString::operator=( const JsonString& jsonString ) {
	return copy(jsonString);
}

//TODO: Populate key names member
void JsonString::setJson(const std::string& jsonText) {
	_jsonText = jsonText;
}
JsonString&	JsonString::swapJsonText(std::string& jsonText) {
	_jsonText.swap(jsonText);
	return *this;
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
