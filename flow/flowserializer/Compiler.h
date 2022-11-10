//
// Created by Markus Pilman on 10/15/22.
//

#ifndef FLATBUFFER_COMPILER_H
#define FLATBUFFER_COMPILER_H
#include <ostream>
#include <memory>
#include <any>

#include <boost/unordered_map.hpp>
#include <boost/unordered_set.hpp>
#include <boost/filesystem/path.hpp>

#include "AST.h"
#include "Error.h"

namespace flowserializer {

class Compiler;

class StaticContext;

namespace expression {

enum class MetadataType { deprecated };

struct MetadataEntry {
	MetadataType type;
	std::any value;
};

// the type of the type -- I like silly names
enum class TypeType { Primitive, Enum, Union, Struct, Table };

struct Type {
	std::string name;
	Type() = default;
	explicit Type(std::string name);
	virtual ~Type() = default;
	[[nodiscard]] virtual TypeType typeType() const = 0;
};

struct Enum : Type {
	[[nodiscard]] TypeType typeType() const override { return TypeType::Enum; }
	std::string type;
	std::vector<std::pair<std::string, int64_t>> values;
};

struct Union : Type {
	[[nodiscard]] TypeType typeType() const override { return TypeType::Union; }
	std::vector<std::string> types;
};

struct Field {
	std::string name;
	std::string type;
	bool isArrayType = false;
	std::optional<std::string> defaultValue;
	std::vector<MetadataEntry> metadata;
};

struct StructOrTable : Type {
	std::vector<Field> fields;
};

struct Struct : StructOrTable {
	[[nodiscard]] TypeType typeType() const override { return TypeType::Struct; }
};

struct Table : StructOrTable {
	[[nodiscard]] TypeType typeType() const override { return TypeType::Table; }
};

struct ExpressionTree {
	std::optional<std::vector<std::string>> namespacePath;
	boost::unordered_set<std::string> attributes;
	boost::unordered_set<std::string> rootTypes;
	std::optional<std::string> fileIdentifier;
	std::optional<std::string> fileExtension;
	boost::unordered_map<std::string, Enum> enums;
	boost::unordered_map<std::string, Union> unions;
	boost::unordered_map<std::string, Struct> structs;
	boost::unordered_map<std::string, Table> tables;

	[[nodiscard]] bool typeExists(std::string const& name) const;

	void verifyField(std::string name, bool isStruct, Field const& field) const;

	std::optional<Type const*> findType(std::string const& name) const;

	void verify(StaticContext const& context) const;
};

enum class PrimitiveTypeClass { BoolType, CharType, IntType, FloatType, StringType };

struct PrimitiveType : Type {
	std::string_view nativeName;
	PrimitiveTypeClass typeClass;
	unsigned _size;

	PrimitiveType() = default;
	PrimitiveType(std::string name, std::string_view nativeName, PrimitiveTypeClass typeClass, unsigned _size)
	  : Type(name), nativeName(nativeName), typeClass(typeClass), _size(_size) {}

	[[nodiscard]] unsigned size() const { return _size; }
	[[nodiscard]] TypeType typeType() const override { return TypeType::Primitive; }
};

extern boost::unordered_map<std::string_view, PrimitiveType> primitiveTypes;

} // namespace expression

struct TypeName {
	std::string name;
	std::vector<std::string> path;

	[[nodiscard]] inline bool operator==(TypeName const& rhs) const { return name == rhs.name && path == rhs.path; }
	[[nodiscard]] bool operator!=(TypeName const& rhs) const { return !(*this == rhs); }
};

[[nodiscard]] std::size_t hash_value(TypeName const& v);

class Compiler {
	friend struct expression::Field;
	friend struct expression::StructOrTable;
	friend class StaticContext;
	// compiled files (used to optimize includes). Filepath -> expression tree
	boost::unordered_map<boost::filesystem::path, std::shared_ptr<StaticContext>> files;
	// where to search for included files
	std::vector<std::string> includePaths;
	// Files that got compiled in this run
	boost::unordered_map<boost::filesystem::path, std::shared_ptr<StaticContext>> compiledFiles;

public:
	explicit Compiler(std::vector<std::string> includePaths);

	void compile(std::string const& path);

	// defined in CodeGenerator.cpp
	void generateCode(std::string const& headerDir, std::string const& sourceDir);
	void describeTables() const;
};

} // namespace flowserializer
#endif // FLATBUFFER_COMPILER_H
