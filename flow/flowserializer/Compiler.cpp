//
// Created by Markus Pilman on 10/15/22.
//

#include <string_view>
#include <variant>
#include <functional>
#include <cstdio>
#include <cstdint>
#include <iostream>

#include "fmt/format.h"
#include <fstream>
#include <boost/filesystem/operations.hpp>

#include "Config.h"
#include "Compiler.h"
#include "StaticContext.h"
#include "CodeGenerator.h"
#include "Parser.h"
#include "Error.h"

namespace flowserializer {

using namespace std::string_literals;
using namespace std::string_view_literals;

namespace expression {

// clang-format off
#define DESC(n, t, c)  PrimitiveType(n, #t, PrimitiveTypeClass::c##Type, sizeof(t))
// clang-format on

boost::unordered_map<std::string_view, PrimitiveType> primitiveTypes{
	{ "bool"sv, DESC("bool", bool, Bool) },
	{ "byte"sv, DESC("byte", char, Char) },
	{ "ubyte"sv, DESC("ubyte", unsigned char, Char) },
	{ "short"sv, DESC("short", short, Int) },
	{ "ushort"sv, DESC("ushort", unsigned short, Int) },
	{ "int"sv, DESC("int", int, Int) },
	{ "uint"sv, DESC("uint", unsigned, Int) },
	{ "float"sv, DESC("float", float, Float) },
	{ "long"sv, DESC("long", long, Int) },
	{ "ulong"sv, DESC("ulong", unsigned long, Int) },
	{ "double"sv, DESC("double", double, Float) },
	{ "int8"sv, DESC("int8", int8_t, Int) },
	{ "uint8"sv, DESC("uint8", uint8_t, Int) },
	{ "int16"sv, DESC("int16", int16_t, Int) },
	{ "uint16"sv, DESC("uint16", uint16_t, Int) },
	{ "int32"sv, DESC("int32", int32_t, Int) },
	{ "uint32"sv, DESC("uint32", uint32_t, Int) },
	{ "int64"sv, DESC("int64", int64_t, Int) },
	{ "uint64"sv, DESC("uint64", uint64_t, Int) },
	{ "float32"sv, DESC("float32", float, Float) },
	{ "float64"sv, DESC("float64", double, Float) },
	{ "string"sv, PrimitiveType{ "string", config::stringType, PrimitiveTypeClass::StringType, 4 } },
};
} // namespace expression

namespace {

using namespace expression;

boost::unordered_set<std::string_view> reservedAttributes{
	"id",         "deprecated", "required", "force_align",   "force_align", "bit_flags", "nested_flatbuffer",
	"flexbuffer", "key",        "hash",     "original_order"
};

MetadataEntry globalMetadata(std::string const& name,
                             ast::Metadata::mapped_type const& value,
                             std::string const& errMsg) {
	// should not be used directly
	// fmt::print("Error: Unknown or unsupported metadata type: {}", metadata->toString());
	throw Error("Unknown or unsupported metadata");
}

MetadataEntry fieldMetadata(ast::FieldDeclaration const& field,
                            std::string const& name,
                            ast::Metadata::mapped_type const& value) {
	if (name == "deprecated") {
		if (value) {
			fmt::print(stderr, "Didn't expect value for metadata type {}\n", name);
			throw Error("Unexpected metadata value");
		}
		return MetadataEntry{ .type = MetadataType::deprecated };
	}
	return globalMetadata(name, value, "");
}

bool startsWith(std::string_view str, std::string_view prefix) {
	return str.substr(0, prefix.size()) == prefix;
}

struct CompilerVisitor : ast::Visitor {
	StaticContext& state;

	explicit CompilerVisitor(StaticContext& state) : state(state) {}

	void visit(const struct ast::IncludeDeclaration& declaration) override {
		state.currentFile->includes.push_back(declaration.path);
		Visitor::visit(declaration);
	}
	void visit(const struct ast::NamespaceDeclaration& declaration) override {
		if (state.currentFile->namespacePath.has_value()) {
			fmt::print(stderr, "Error: Unexpected namespace declaration: {}\n", fmt::join(declaration.name, "."));
			fmt::print(
			    stderr, "       {} was declared before\n", fmt::join(state.currentFile->namespacePath.value(), "."));
			throw Error("Unexpected namespace");
		} else {
			state.currentFile->namespacePath = declaration.name;
		}
	}
	void visit(const struct ast::AttributeDeclaration& declaration) override {
		if (state.currentFile->attributes.count(declaration.attribute) > 0) {
			fmt::print(stderr, "Error: Attribute {} defined multiple times\n", declaration.attribute);
			throw Error("Multiple attributes");
		} else if (reservedAttributes.count(declaration.attribute) > 0 ||
		           startsWith(declaration.attribute, "native_")) {
			fmt::print(stderr, "Error: Attribute {} is reserved\n", declaration.attribute);
			throw Error("Reserved attribute");
		} else {
			state.currentFile->attributes.insert(declaration.attribute);
		}
	}
	void visit(const struct ast::RootDeclaration& declaration) override {
		if (primitiveTypes.count(declaration.rootType) > 0) {
			fmt::print(stderr, "Error: Primitive type {} can't be declared as root type\n", declaration.rootType);
			throw Error("Primitive root type");
		} else {
			state.currentFile->rootTypes.insert(declaration.rootType);
		}
	}
	void visit(const struct ast::FileExtensionDeclaration& declaration) override {
		if (state.currentFile->fileExtension.has_value()) {
			fmt::print(stderr,
			           "Multiple file extensions \"{}\" and \"{}\"\n",
			           state.currentFile->fileExtension.value(),
			           declaration.extension);
			throw Error("Multiple file extensions");
		} else {
			state.currentFile->fileExtension = declaration.extension;
		}
	}
	void visit(const struct ast::FileIdentifierDeclaration& declaration) override {
		if (declaration.identifier.size() != 4) {
			fmt::print(stderr,
			           "File identifiers need to be exactly 4 bytes long but \"{}\" is {} bytes long\n",
			           declaration.identifier,
			           declaration.identifier.size());
			throw Error("File identifier too long");
		} else if (state.currentFile->fileIdentifier.has_value()) {
			fmt::print(stderr,
			           "Multiple file identifiers \"{}\" and \"{}\"\n",
			           state.currentFile->fileExtension.value(),
			           declaration.identifier);
			throw Error("Multiple file extensions");
		} else {
			state.currentFile->fileIdentifier = declaration.identifier;
		}
	}
	void visit(const struct ast::EnumDeclaration& declaration) override {
		if (state.currentFile->typeExists(declaration.identifier)) {
			fmt::print(stderr, "Error: Type {} already exists\n", declaration.identifier);
			throw Error("Duplicate type");
		} else if (primitiveTypes.count(declaration.type) == 0 ||
		           (primitiveTypes[declaration.type].typeClass != PrimitiveTypeClass::IntType &&
		            primitiveTypes[declaration.type].typeClass != PrimitiveTypeClass::CharType)) {
			fmt::print(stderr, "Error: Type {} can't be used as base for an enum\n", declaration.type);
			throw Error("Incompatible enum type");
		}
		expression::Enum newEnum;
		newEnum.name = declaration.identifier;
		newEnum.type = declaration.type;
		int64_t lastVal = -1;
		boost::unordered_set<std::string_view> usedIdentifiers;
		boost::unordered_set<int64_t> usedValues;
		for (auto const& val : declaration.enumerations) {
			int64_t value;
			if (val.second.has_value()) {
				value = val.second.value();
				lastVal = value;
			} else {
				value = ++lastVal;
			}
			if (usedValues.count(value) > 0) {
				fmt::print(stderr, "Error: Duplicate value for enum {}: {}\n", declaration.identifier, value);
				throw Error("Duplicate enum value");
			} else if (usedIdentifiers.count(val.first) > 0) {
				fmt::print(stderr, "Error: Duplicate identifier for enum {}: {}\n", declaration.identifier, val.first);
				throw Error("Duplicate enum identifier");
			}
			usedValues.insert(value);
			newEnum.values.emplace_back(val.first, value);
		}
		state.currentFile->enums.emplace(newEnum.name, std::move(newEnum));
	}
	void visit(const struct ast::UnionDeclaration& declaration) override {
		if (state.currentFile->typeExists(declaration.identifier)) {
			fmt::print(stderr, "Error: Type {} already exists\n", declaration.identifier);
			throw Error("Duplicate type");
		}
		expression::Union newUnion;
		newUnion.name = declaration.identifier;
		for (auto const& val : declaration.enumerations) {
			if (val.second.has_value()) {
				fmt::print(stderr,
				           "Can't assign values to union members (union=\"{}\", member=\"{}\", value=\"{}\")\n",
				           newUnion.name,
				           val.first,
				           val.second.value());
				throw Error("Union with default value");
			}
			newUnion.types.push_back(val.first);
		}
		state.currentFile->unions.emplace(newUnion.name, std::move(newUnion));
	}

	void constructStructOrTable(expression::StructOrTable& res,
	                            std::vector<ast::FieldDeclaration> const& fields) const {
		if (state.currentFile->typeExists(res.name)) {
			fmt::print(stderr, "Error: Type {} already exists\n", res.name);
			throw Error("Duplicate type");
		}
		boost::unordered_set<std::string_view> prevFields;
		for (auto const& field : fields) {
			expression::Field f;
			if (prevFields.count(field.identifier) > 0) {
				fmt::print("Error: duplicate field {} in {}\n", field.identifier, res.name);
				throw Error("Duplicate field");
			}
			f.name = field.identifier;
			f.type = field.type.type();
			f.isArrayType = field.type.isArray();
			if (field.value) {
				f.defaultValue = field.value.value().toString();
			}
			for (auto const& m : field.metadata) {
				f.metadata.push_back(fieldMetadata(field, m.first, m.second));
			}
			res.fields.push_back(f);
		}
	}

	void visit(const struct ast::StructDeclaration& declaration) override {
		expression::Struct res;
		res.name = declaration.identifier;
		constructStructOrTable(res, declaration.fields);
		state.currentFile->structs.emplace(res.name, std::move(res));
	}
	void visit(const struct ast::TableDeclaration& declaration) override {
		expression::Table res;
		res.name = declaration.identifier;
		constructStructOrTable(res, declaration.fields);
		state.currentFile->tables.emplace(res.name, std::move(res));
	}
};

} // namespace

namespace expression {

bool ExpressionTree::typeExists(const std::string& name) const {
	return primitiveTypes.count(name) > 0 || enums.count(name) > 0 || unions.count(name) > 0 ||
	       structs.count(name) > 0 || tables.count(name) > 0;
}
void ExpressionTree::verifyField(std::string name, bool isStruct, const Field& field) const {
	std::string typeLiteral = field.type;
	if (field.isArrayType) {
		typeLiteral = fmt::format("[{}]", field.type);
	}
	if (!typeExists(field.type)) {
		fmt::print(stderr,
		           "{} {} defines field {} of type {}, but {} can't be found\n",
		           isStruct ? "Struct" : "Table",
		           name,
		           field.name,
		           typeLiteral,
		           field.type);
		throw Error("Type not found");
	}
	if (field.defaultValue && field.isArrayType) {
		fmt::print(stderr,
		           "Field {} in {} {}: Can't assign value to array type {}\n",
		           field.name,
		           isStruct ? "struct" : "st",
		           name,
		           typeLiteral);
		throw Error("Assign value to array type");
	}
	if (field.defaultValue) {
		if (field.defaultValue && enums.count(field.type) > 0) {
			auto const& e = enums.at(field.type);
			bool found = false;
			for (auto const& [n, _] : e.values) {
				found = found || n == field.defaultValue.value();
			}
			if (!found) {
				fmt::print(stderr,
				           "Error: Field {} in {} {}: invalid enum value {}\n",
				           field.name,
				           isStruct ? "struct" : "st",
				           name,
				           field.defaultValue.value());
				std::vector<std::string> values;
				std::transform(e.values.begin(), e.values.end(), std::back_inserter(values), [](auto const& p) {
					return p.first;
				});
				fmt::print("       possible values are: [{}]\n", fmt::join(values, ", "));
				throw Error("Invalid enum value");
			}
		} else if (primitiveTypes.count(field.type) == 0) {
			fmt::print(stderr,
			           "Error: Field {} in {} {}: Can't assign value to user defined type {}\n",
			           field.name,
			           isStruct ? "struct" : "st",
			           name,
			           typeLiteral);
			throw Error("Assign value to user defined type");
		}
	}
}

void ExpressionTree::verify(StaticContext const& context) const {
	auto assertTypeIsUnique = [&context](std::string const& name) {
		if (context.resolve(name, true)) {
			std::cerr << fmt::format("Error: Duplicate type: {}", name);
			throw Error("Duplicate type");
		}
	};
	// verify all enums are unique types
	for (auto const& [_, e] : enums) {
		assertTypeIsUnique(e.name);
	}
	// verify all root types exist in current file and are tables
	for (auto const& t : rootTypes) {
		if (tables.count(t) == 0) {
			fmt::print(stderr, "Error: Type {} was declared to be root but either doesn't exist or is not a st\n", t);
			throw Error("Root type not a st");
		}
	}
	// verify that all unions reference tables
	for (auto const& [name, u] : unions) {
		assertTypeIsUnique(u.name);
		for (auto const& t : u.types) {
			if (tables.count(t) == 0) {
				fmt::print(
				    stderr, "Error: Type {} was used in union {} but either doesn't exist or is not a st\n", t, name);
				throw Error("Union member not a st");
			}
		}
	}
	// verify that structs and tables reference proper types
	for (auto const& s : structs) {
		assertTypeIsUnique(s.second.name);
		for (auto const& field : s.second.fields) {
			verifyField(s.first, true, field);
		}
	}
	for (auto const& s : tables) {
		assertTypeIsUnique(s.second.name);
		for (auto const& field : s.second.fields) {
			verifyField(s.first, false, field);
		}
	}
}

std::optional<Type const*> ExpressionTree::findType(const std::string& name) const {
	std::optional<const Type*> res;
	if (auto eIter = enums.find(name); eIter != enums.end()) {
		res = &eIter->second;
	} else if (auto uIter = unions.find(name); uIter != unions.end()) {
		res = &uIter->second;
	} else if (auto sIter = structs.find(name); sIter != structs.end()) {
		res = &sIter->second;
	} else if (auto tIter = tables.find(name); tIter != tables.end()) {
		res = &tIter->second;
	}
	return res;
}
} // namespace expression

Compiler::Compiler(std::vector<std::string> includePaths) : includePaths(std::move(includePaths)) {}

void Compiler::compile(std::string const& inputPath) {
	boost::filesystem::path path = boost::filesystem::canonical(inputPath);
	if (auto iter = files.find(path); iter != files.end()) {
		compiledFiles[path] = iter->second;
	}
	auto res = std::make_shared<StaticContext>(*this);
	std::ifstream ifs(path.c_str());
	std::string content((std::istreambuf_iterator<char>(ifs)), (std::istreambuf_iterator<char>()));
	auto schema = parseSchema(content);
	CompilerVisitor visitor(*res);
	schema.accept(visitor);
	res->currentFile->verify(*res);
	files[path] = res;
	compiledFiles[path] = res;
}

void Compiler::generateCode(const std::string& headerDir, const std::string& sourceDir) {
	namespace fs = boost::filesystem;
	for (auto const& [name, context] : compiledFiles) {
		auto stem = fs::path(name).stem().string();
		auto headerFileName = stem + ".h";
		auto sourceFileName = stem + ".cpp";
		auto header = fs::path(headerDir) / headerFileName;
		auto source = fs::path(sourceDir) / sourceFileName;
		CodeGenerator(context.get()).emit(stem, header, source);
	}
}
void Compiler::describeTables() const {
	for (auto const& [path, context] : compiledFiles) {
		fmt::print("Discribing tables in {}:\n", path.string());
		fmt::print("================================================\n");
		for (auto const& [_, table] : context->currentFile->tables) {
			fmt::print("Describing st {}\n", table.name);
			context->describeTable(table.name);
			fmt::print("------------------------------------------------\n");
		}
	}
}

[[nodiscard]] std::size_t hash_value(TypeName const& v) {
	std::size_t seed = 0;
	boost::hash_combine(seed, v.name);
	boost::hash_combine(seed, v.path);
	return seed;
}

} // namespace flowserializer
