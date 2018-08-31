
#ifndef JSONSTRING_H
#define JSONSTRING_H

#include <string>
#include <set>
#include <vector>

class JsonString;
class JsonStringArray;
class JsonStringSetter;

class JsonString {
	public:
		JsonString();
		JsonString( const JsonString& jsonString);
		JsonString( const JsonStringArray& jsonArray);
		JsonString( const char* value );
		JsonString( const std::string& value );

		JsonString( const std::string& name, const char* value );
		JsonString( const std::string& name, const std::string& value );
		JsonString( const std::string& name, double value );
		JsonString( const std::string& name, long int value );
		JsonString( const std::string& name, long unsigned int value );
		JsonString( const std::string& name, long long int value );
		JsonString( const std::string& name, long long unsigned int value );
		JsonString( const std::string& name, int value );
		JsonString( const std::string& name, unsigned value );
		JsonString( const std::string& name, bool value );
		JsonString( const std::string& name, const JsonString& value );
		JsonString( const std::string& name, const JsonStringArray& value );

		JsonString& append( const std::string& name, const char* value );
		JsonString& append( const std::string& name, const std::string& value );
		JsonString& append( const std::string& name, double value );
		JsonString& append( const std::string& name, long int value );
		JsonString& append( const std::string& name, long unsigned int value );
		JsonString& append( const std::string& name, long long int value );
		JsonString& append( const std::string& name, long long unsigned int value );
		JsonString& append( const std::string& name, int value );
		JsonString& append( const std::string& name, unsigned value );
		JsonString& append( const std::string& name, bool value );
		JsonString& append( const std::string& name, const JsonString& value );
		JsonString& append( const std::string& name, const JsonStringArray& value );

		JsonString& append( const std::string& value );
		JsonString& append( const char* value );
		JsonString& append( const JsonStringArray& value );

		JsonString& append( double value );
		JsonString& append( long int value );
		JsonString& append( long unsigned int value );
		JsonString& append( long long int value );
		JsonString& append( long long unsigned int value );
		JsonString& append( int value );
		JsonString& append( unsigned value );
		JsonString& append( bool value );
		JsonString& append( const JsonString& value );


		JsonStringSetter& operator[]( const std::string& name );
		JsonStringSetter& operator[]( const char* name );

		int compare( const JsonString& jsonString ) const;
		bool equals( const JsonString& jsonString ) const;
		bool operator==( const JsonString& jsonString ) const { return equals(jsonString); }
		bool operator!=( const JsonString& jsonString ) const { return !equals(jsonString); }

		JsonString& copy( const JsonString& jsonString );
		JsonString& operator=( const JsonString& jsonString );

		std::string	getJson() const;
		void	setJson(const std::string& jsonText);

		JsonString& clear();
		bool	empty() const;

		bool	isPresent(uint32_t nameHash) const;
		bool	isPresent(const std::string& name) const;

		static uint32_t hash32( const std::string& name );
		static JsonString makeMessage(const char *name, const char *description);

	protected:
		void hashName( const std::string& name);

	protected:
		std::string		_jsonText;
		std::set<uint32_t>	_keyNames;

		// Uneditted text
		const std::string&	getJsonText() const;
};

// Make protected because no virtual destructor
class JsonStringArray : protected std::vector<JsonString>
{
	typedef JsonString T;
	typedef std::vector<JsonString> vector;
	public:
		using vector::push_back;
		using vector::operator[];
		using vector::begin;
		using vector::end;
		using vector::size;
		using vector::empty;
		using vector::clear;
	public:
		JsonStringArray();
		JsonStringArray( const JsonStringArray& jsonStringArray);
		virtual ~JsonStringArray();
};

class JsonStringSetter {
	public:

	JsonStringSetter( JsonString& jsonString, const std::string& name );

	JsonStringSetter& operator=( const std::string& value );
	JsonStringSetter& operator=( const char* value );
	JsonStringSetter& operator=( double value );
	JsonStringSetter& operator=( long int value );
	JsonStringSetter& operator=( long unsigned int value );
	JsonStringSetter& operator=( long long int value );
	JsonStringSetter& operator=( long long unsigned int value );
	JsonStringSetter& operator=( int value );
	JsonStringSetter& operator=( unsigned value );
	JsonStringSetter& operator=( bool value );
	JsonStringSetter& operator=( const JsonString& value );
	JsonStringSetter& operator=( const JsonStringArray& value );

	protected:
		JsonString&		_jsonString;
		std::string		_name;
};

#endif
