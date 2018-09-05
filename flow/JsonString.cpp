#include "JsonString.h"
#include "Hash3.h"
#include <iostream>
#include "Trace.h"
#include "flow.h"

std::string format( const char* form, ... );

JsonString::JsonString() : _jsonText(), _keyNames() {
}
JsonString::JsonString( const JsonString& jsonString) : _jsonText(jsonString._jsonText), _keyNames(jsonString._keyNames) {
}
JsonString::JsonString( const JsonStringArray& jsonArray) : _jsonText(), _keyNames() {
	append(jsonArray);
}
JsonString::JsonString( const std::string& value ) : _jsonText(), _keyNames() {
	append(value);
}
JsonString::JsonString( const char* value ) : _jsonText(), _keyNames() {
	append(value);
}

JsonString::JsonString( const std::string& name, const std::string& value ) : _jsonText(), _keyNames() {
	append(name, value);
}
JsonString::JsonString( const std::string& name, const char* value ) : _jsonText(), _keyNames() {
	append(name, value);
}
JsonString::JsonString( const std::string& name, double value ) : _jsonText(), _keyNames() {
	append(name, value);
}
JsonString::JsonString( const std::string& name, long int value ) : _jsonText(), _keyNames() {
	append(name, value);
}
JsonString::JsonString( const std::string& name, long unsigned int value ) : _jsonText(), _keyNames() {
	append(name, value);
}
JsonString::JsonString( const std::string& name, long long int value ) : _jsonText(), _keyNames() {
	append(name, value);
}
JsonString::JsonString( const std::string& name, long long unsigned int value ) : _jsonText(), _keyNames() {
	append(name, value);
}
JsonString::JsonString( const std::string& name, int value ) : _jsonText(), _keyNames() {
	append(name, value);
}
JsonString::JsonString( const std::string& name, unsigned value ) : _jsonText(), _keyNames() {
	append(name, value);
}
JsonString::JsonString( const std::string& name, bool value ) : _jsonText(), _keyNames() {
	append(name, value);
}
JsonString::JsonString( const std::string& name, const JsonString& value ) : _jsonText(), _keyNames() {
	append(name, value);
}
JsonString::JsonString( const std::string& name, const JsonStringArray& value ) : _jsonText(), _keyNames() {
	append(name, value);
}

uint32_t JsonString::hash32( const std::string& name ) {
	uint32_t a=0, b=0;
	hashlittle2( (const void*) name.c_str(), name.length(), &a, &b );
	return a;
}

bool	JsonString::isPresent(const std::string& name) const {
	return (_keyNames.find(name) != _keyNames.end());
}

JsonString JsonString::makeMessage(const char *name, const char *description) {
	JsonString out;
	out["name"] = name;
	out["description"] = description;
	return out;
}

void JsonString::hashName( const std::string& name) {
	if (isPresent(name)) {
		TraceEvent(g_network && g_network->isSimulated() ? SevError : SevWarnAlways, "JsonError").detail("KeyPresent", name).backtrace();
	}
	_keyNames.insert(name);
}

JsonString& JsonString::appendImpl( const std::string& name, const std::string& value, bool quote ) {
	hashName(name);
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

JsonString& JsonString::append( const std::string& name, const std::string& value ) {
	return appendImpl(name, value, true);
}
JsonString& JsonString::append( const std::string& name, const char* value ) {
	std::string	textValue(value);
	return appendImpl(name, textValue, true);
}
JsonString& JsonString::append( const std::string& name, double value ) {
	return appendImpl(name, format("%g", value), false);
}
JsonString& JsonString::append( const std::string& name, long int value ) {
	return appendImpl(name, format("%ld", value), false);
}
JsonString& JsonString::append( const std::string& name, long unsigned int value ) {
	return appendImpl(name, format("%lu", value), false);
}
JsonString& JsonString::append( const std::string& name, long long int value ) {
	return appendImpl(name, format("%lld", value), false);
}
JsonString& JsonString::append( const std::string& name, long long unsigned int value ) {
	return appendImpl(name, format("%llu", value), false);
}
JsonString& JsonString::append( const std::string& name, int value ) {
	return appendImpl(name, format("%d", value), false);
}
JsonString& JsonString::append( const std::string& name, unsigned value ) {
	return appendImpl(name, format("%u", value), false);
}
JsonString& JsonString::append( const std::string& name, bool value ) {
	return appendImpl(name, value ? "true" : "false", false);
}
JsonString& JsonString::append( const std::string& name, const JsonString& value ) {
	hashName(name);
	_jsonText += (_jsonText.empty() ? "\"" : ",\n  \"") + name + "\": { " + value._jsonText + " }";
	return *this;
}
JsonString& JsonString::append( const std::string& name, const JsonStringArray& values ) {
	hashName(name);
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
	std::string	textValue(value);
	return appendImpl(textValue, true);
}
JsonString& JsonString::append( double value ) {
	return appendImpl(format("%g", value), false);
}
JsonString& JsonString::append( long int value ) {
	return appendImpl(format("%ld", value), false);
}
JsonString& JsonString::append( long unsigned int value ) {
	return appendImpl(format("%lu", value), false);
}
JsonString& JsonString::append( long long int value ) {
	return appendImpl(format("%lld", value), false);
}
JsonString& JsonString::append( long long unsigned int value ) {
	return appendImpl(format("%llu", value), false);
}
JsonString& JsonString::append( int value ) {
	return appendImpl(format("%d", value), false);
}
JsonString& JsonString::append( unsigned value ) {
	return appendImpl(format("%u", value), false);
}
JsonString& JsonString::append( bool value ) {
	return appendImpl(value ? "true" : "false", false);
}
JsonString& JsonString::append( const JsonString& value ) {
	// Only do something, if not empty
	if (!value.empty()) {
		if (!_jsonText.empty())
			_jsonText += ",\n  ";
		_jsonText += value._jsonText;
		_keyNames.insert(value._keyNames.begin(), value._keyNames.end());
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
	_keyNames.clear();
	_jsonText.clear();
	return *this;
}

bool	JsonString::empty() const {
	return _jsonText.empty();
}

const std::string&	JsonString::getJsonText() const {
	return _jsonText;
}

size_t	JsonString::getLength() const {
	return _jsonText.length() + ((!empty() && _keyNames.empty()) ? 0 : 2);
}
size_t	JsonString::getNameTotal() const {
	return _keyNames.size();
}

std::string	JsonString::getJson() const {
	// If not empty with no names (only values), don't add brackets because prob in an array
	return (!empty() && _keyNames.empty()) ? _jsonText : ("{ " + _jsonText + " }");
}

JsonString& JsonString::copy( const JsonString& jsonString ) {
	_jsonText = jsonString._jsonText;
	_keyNames = jsonString._keyNames;
	return *this;
}

JsonString& JsonString::operator=( const JsonString& jsonString ) {
	return copy(jsonString);
}

//TODO: Populate key names member
void	JsonString::setJson(const std::string& jsonText) {
	_keyNames.clear();
	_jsonText = jsonText;
}

JsonStringSetter& JsonString::operator[]( const std::string& name ) {
	JsonStringSetter* stringSetter = new JsonStringSetter(*this, name);
	return *stringSetter;
}

JsonStringSetter& JsonString::operator[]( const char* name ) {
	std::string	textName(name);
	JsonStringSetter* stringSetter = new JsonStringSetter(*this, textName);
	return *stringSetter;
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

JsonStringSetter& JsonStringSetter::operator=( const std::string& value ) {
	_jsonString.append(_name, value);
	return *this;
}
JsonStringSetter& JsonStringSetter::operator=( const char* value ) {
	_jsonString.append(_name, value);
	return *this;
}
JsonStringSetter& JsonStringSetter::operator=( double value ) {
	_jsonString.append(_name, value);
	return *this;
}
JsonStringSetter& JsonStringSetter::operator=( long int value ) {
	_jsonString.append(_name, value);
	return *this;
}
JsonStringSetter& JsonStringSetter::operator=( long unsigned int value ) {
	_jsonString.append(_name, value);
	return *this;
}
JsonStringSetter& JsonStringSetter::operator=( long long int value ) {
	_jsonString.append(_name, value);
	return *this;
}
JsonStringSetter& JsonStringSetter::operator=( long long unsigned int value ) {
	_jsonString.append(_name, value);
	return *this;
}
JsonStringSetter& JsonStringSetter::operator=( int value ) {
	_jsonString.append(_name, value);
	return *this;
}
JsonStringSetter& JsonStringSetter::operator=( unsigned value ) {
	_jsonString.append(_name, value);
	return *this;
}
JsonStringSetter& JsonStringSetter::operator=( bool value ) {
	_jsonString.append(_name, value);
	return *this;
}
JsonStringSetter& JsonStringSetter::operator=( const JsonString& value ) {
	_jsonString.append(_name, value);
	return *this;
}
JsonStringSetter& JsonStringSetter::operator=( const JsonStringArray& value ) {
	_jsonString.append(_name, value);
	return *this;
}
