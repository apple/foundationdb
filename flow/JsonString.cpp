#include "JsonString.h"
#include "Hash3.h"
#include <iostream>

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

bool	JsonString::isPresent(uint32_t nameHash) const {
	return (_keyNames.find(nameHash) != _keyNames.end());
}
bool	JsonString::isPresent(const std::string& name) const {
	return isPresent(hash32(name));
}

JsonString JsonString::makeMessage(const char *name, const char *description) {
	JsonString out;
	out["name"] = name;
	out["description"] = description;
	return out;
}

void JsonString::hashName( const std::string& name) {
	_keyNames.insert(hash32(name));
}

JsonString& JsonString::append( const std::string& name, const std::string& value ) {
	hashName(name);
	_jsonText += (_jsonText.empty() ? "\"" : ",\n  \"") + name + "\": \"" + value + "\"";
	return *this;
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
JsonString& JsonString::append( const std::string& name, const char* value ) {
	std::string	textValue(value);
	return append(name, textValue);
}
JsonString& JsonString::append( const std::string& name, double value ) {
	return append(name, format("%g", value));
}
JsonString& JsonString::append( const std::string& name, long int value ) {
	return append(name, format("%ld", value));
}
JsonString& JsonString::append( const std::string& name, long unsigned int value ) {
	return append(name, format("%lu", value));
}
JsonString& JsonString::append( const std::string& name, long long int value ) {
	return append(name, format("%lld", value));
}
JsonString& JsonString::append( const std::string& name, long long unsigned int value ) {
	return append(name, format("%llu", value));
}
JsonString& JsonString::append( const std::string& name, int value ) {
	return append(name, format("%d", value));
}
JsonString& JsonString::append( const std::string& name, unsigned value ) {
	return append(name, format("%u", value));
}
JsonString& JsonString::append( const std::string& name, bool value ) {
	hashName(name);
	_jsonText += (_jsonText.empty() ? "\"" : ",\n  ") + name + (value ? "\": true" : "\": false");
	return *this;
}

JsonString& JsonString::append( const std::string& value ) {
	_jsonText += (_jsonText.empty() ? "\"" : ", \"") + value + "\"";
	return *this;
}
JsonString& JsonString::append( const char* value ) {
	std::string	textValue(value);
	return append(textValue);
}
JsonString& JsonString::append( double value ) {
	return append(format("%g", value));
}
JsonString& JsonString::append( long int value ) {
	return append(format("%ld", value));
}
JsonString& JsonString::append( long unsigned int value ) {
	return append(format("%lu", value));
}
JsonString& JsonString::append( long long int value ) {
	return append(format("%lld", value));
}
JsonString& JsonString::append( long long unsigned int value ) {
	return append(format("%llu", value));
}
JsonString& JsonString::append( int value ) {
	return append(format("%d", value));
}
JsonString& JsonString::append( unsigned value ) {
	return append(format("%u", value));
}
JsonString& JsonString::append( bool value ) {
	if (!_jsonText.empty())
		_jsonText += ", ";
	_jsonText += value ? "true" : "false";
	return *this;
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

//ahm Remove the leading and ending {}
void	JsonString::setJson(const std::string& jsonText) {
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

inline void _fdbassert(const char* expression, const char* file, int line) {
	fprintf(stderr, "Assertion '%s' failed, file '%s' line '%d'.", expression, file, line);
	abort();
}

#define fdbassert(EXPRESSION) ((EXPRESSION) ? (void)0 : _fdbassert(#EXPRESSION, __FILE__, __LINE__))

int jsonUnitTests() {
	int	status = 0;

	JsonString	jsonString1;
	jsonString1["_valid"] = false;
	jsonString1["error"] = "configurationMissing";

	JsonString	jsonString2("_valid", false);
	jsonString2["error"] = "configurationMissing";

	JsonString	jsonString3("error", "configurationMissing");
	jsonString3["_valid"] = false;

	JsonString	jsonString4("_valid", false);
	jsonString4.append("error", "configurationMissing");

	std::cout << "Json1: " << jsonString1.getJson() << std::endl;
	std::cout << "Json2: " << jsonString2.getJson() << std::endl;
	std::cout << "Json3: " << jsonString3.getJson() << std::endl;
	std::cout << "Json4: " << jsonString4.getJson() << std::endl;

	fdbassert(jsonString1 == jsonString2);
	fdbassert(jsonString1 != jsonString3); // wrong order
	fdbassert(jsonString1 == jsonString4);

	JsonString	jsonObj, testObj;
	JsonStringArray jsonArray;

	jsonObj["alvin"] = "moore";
	jsonObj["count"] = 3;
	testObj["pi"] = 3.14159;
	jsonObj["piobj"] = testObj;

	testObj.clear();
	testObj["val1"] = 7.9;
	testObj["val2"] = 34;
	jsonArray.push_back(testObj);

	testObj.clear();
	testObj["name1"] = "alvin";
	testObj["name2"] = "evan";
	testObj["name3"] = "ben";
	jsonArray.push_back(testObj);
	jsonObj.append("name_vals", jsonArray);

	std::cout << "Json (complex): " << jsonObj.getJson() << std::endl;

	jsonObj.clear();
	jsonObj.append(jsonArray);
	std::cout << "Json (complex array): " << jsonObj.getJson() << std::endl;

	jsonArray.clear();
	jsonArray.push_back("nissan");
	jsonArray.push_back("honda");
	jsonArray.push_back("bmw");
	jsonObj.clear();
	jsonObj.append(jsonArray);
	std::cout << "Array (simple array #1): " << jsonObj.getJson() << std::endl;

	testObj.clear();
	testObj.append("nissan");
	testObj.append("honda");
	testObj.append("bmw");
	jsonArray.clear();
	jsonArray.push_back(testObj);
	jsonObj.clear();
	jsonObj.append(jsonArray);
	std::cout << "Array (simple array #2): " << jsonObj.getJson() << std::endl;

	jsonObj.clear();
	jsonObj["name1"] = "alvin";
	jsonObj["name2"] = "evan";
	jsonObj["name3"] = "ben";
	testObj.clear();
	testObj["val1"] = 7.9;
	testObj["val2"] = 34;
	std::cout << "Merge (obj #1): " << jsonObj.getJson() << std::endl;
	std::cout << "Merge (obj #2): " << testObj.getJson() << std::endl;
	jsonObj.append(testObj);
	std::cout << "Merged: " << jsonObj.getJson() << std::endl;

	return status;
}
