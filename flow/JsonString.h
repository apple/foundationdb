
#ifndef JSONSTRING_H
#define JSONSTRING_H

#include <string>
#include <unordered_set>
#include <vector>

class JsonString;
class JsonStringArray;
class JsonStringSetter;

class JsonString {
	public:
		JsonString();
		JsonString( const JsonString& jsonString);
		explicit JsonString( const JsonStringArray& jsonArray);
		explicit JsonString( const char* value );
		explicit JsonString( const std::string& value );

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

		size_t	getLength() const;
		size_t	getNameTotal() const;

		JsonString& clear();
		bool	empty() const;

		bool	isPresent(const std::string& name) const;

		static JsonString makeMessage(const char *name, const char *description);

	protected:
		void hashName( const std::string& name);
		JsonString& appendImpl( const std::string& name, const std::string& value, bool quote);
		JsonString& appendImpl( const std::string& value, bool quote);

		static std::string	stringify(const char* value);
		static std::string	stringify(double value);
		static std::string	stringify(long int value);
		static std::string	stringify(long unsigned int value);
		static std::string	stringify(long long int value);
		static std::string	stringify(long long unsigned int value);
		static std::string	stringify(int value);
		static std::string	stringify(unsigned value);
		static std::string	stringify(bool value);

	protected:
		std::string		_jsonText;
		std::unordered_set<std::string>	_keyNames;

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

	template <class valClass>
	JsonStringSetter& operator=( const valClass& value ) {
		_jsonString.append(_name, value);
		return *this;
	}

	protected:
		JsonString&		_jsonString;
		std::string		_name;
};

#endif
