//
// Created by Markus Pilman on 10/15/22.
//

#include <fstream>
#include <string>
#include <any>

#include <boost/filesystem.hpp>
#include <boost/algorithm/string.hpp>

#include "Config.h"
#include "Compiler.h"
#include "StaticContext.h"
#include "CodeGenerator.h"

using namespace std::string_literals;

namespace flowserializer {

struct Streams {
	std::ostream& header;
	std::ostream& source;
};

template <class T>
void operator<<(Streams& streams, T const& value) {
	streams.header << value;
	streams.source << value;
}

#define EMIT(out, str, ...) out << fmt::format(str "\n", ##__VA_ARGS__)

namespace {

struct Defer {
	std::vector<std::function<void()>> functions;
	~Defer() {
		for (auto iter = functions.rbegin(); iter != functions.rend(); ++iter) {
			(*iter)();
		}
	}
	void operator()(std::function<void()> fun) { functions.push_back(std::move(fun)); }
};

std::string convertType(std::string const& t) {
	if (auto iter = expression::primitiveTypes.find(t); iter != expression::primitiveTypes.end()) {
		return std::string(iter->second.nativeName);
	}
	std::vector<std::string> path;
	boost::split(path, t, boost::is_any_of("."));
	return fmt::format("{}", fmt::join(path, "::"));
}

using DependencyMap = boost::unordered_map<std::string, boost::unordered_set<std::string>>;

bool isInternalDependency(std::string const& type, expression::ExpressionTree const& tree) {
	return tree.unions.count(type) > 0 || tree.tables.count(type) > 0 || tree.structs.count(type) > 0;
}

DependencyMap buildDependencyMap(expression::ExpressionTree const& tree) {
	DependencyMap res;
	// enums don't have dependencies. Therefore, we can ignore them
	for (auto const& [n, u] : tree.unions) {
		// make sure types with only primitive types still get inserted
		auto [iter, _] = res.emplace(n, DependencyMap::mapped_type());
		for (auto const& t : u.types) {
			if (isInternalDependency(t, tree)) {
				iter->second.insert(t);
			}
		}
	}
	auto processStructOrTable = [&res, &tree](expression::StructOrTable const& s) {
		auto [iter, _] = res.emplace(s.name, DependencyMap::mapped_type());
		for (auto const& f : s.fields) {
			if (isInternalDependency(f.type, tree)) {
				iter->second.insert(f.type);
			}
		}
	};
	for (auto const& [_, s] : tree.structs) {
		processStructOrTable(s);
	}
	for (auto const& [_, t] : tree.tables) {
		processStructOrTable(t);
	}
	return res;
}

/**
 * C++ doesn't allow us to use a type before it is declared. This function goes through all available types
 * and orders them by their dependencies. It will throw an error if this is not possible.
 *
 * @param tree
 * @return A list of types in a conflict-free order
 */
std::vector<std::string> establishEmitOrder(expression::ExpressionTree const& tree) {
	std::vector<std::string> res;
	boost::unordered_set<std::string> resolvedTypes;
	DependencyMap dependencies = buildDependencyMap(tree);
	while (!dependencies.empty()) {
		auto before = res.size();
		// 0. remove all resolved types from all dependencies
		for (auto& [name, deps] : dependencies) {
			for (auto const& t : resolvedTypes) {
				deps.erase(t);
			}
		}
		// 1. collect all types with no dependencies
		for (auto const& [name, deps] : dependencies) {
			if (deps.empty()) {
				res.push_back(name);
				resolvedTypes.insert(name);
			}
		}
		if (res.size() - before == 0) {
			throw Error("Cyclic dependencies");
		}
		// 2, Delete types with no dependencies from the map
		for (auto i = before; i < res.size(); ++i) {
			dependencies.erase(res[i]);
		}
	}
	return res;
}

std::string headerGuard(std::string const& stem) {
	auto res = "FLOWFLAT_"s;
	res.reserve(res.size() + stem.size() + 2);
	std::transform(stem.begin(), stem.end(), std::back_inserter(res), [](auto c) { return std::toupper(c); });
	res.push_back('_');
	res.push_back('H');
	return res;
}

void emitIncludes(Streams& out, std::vector<std::string> const& includes) {
	EMIT(out.header, "#include \"flow/serialize.h\"");
	EMIT(out.header, "#include \"FlatbuffersTypes.h\"");
	for (auto const& p : includes) {
		out.header << fmt::format("#include \"{}.h\"\n", p.substr(0, p.size() - ".fbs"s.size()));
	}
	EMIT(out.header, "");
}

void emitDeserializeField(StaticContext* context,
                          Streams& out,
                          std::string qualifiedName,
                          TypeName typeName,
                          expression::Type const& type);

void emitDeserializeArray(StaticContext* context,
                          Streams& out,
                          std::string qualifiedName,
                          TypeName typeName,
                          expression::Type const& type) {
	EMIT(out.source, "\t\tauto offset = *reinterpret_cast<const int32_t*>(data + tableOffset + vtable[idx]);");
	EMIT(out.source, "\t\t// concert offset to offset starting from 0");
	EMIT(out.source, "\t\toffset += tableOffset + vtable[idx];");
	EMIT(out.source, "\t\tauto sz = *reinterpret_cast<const uint32_t*>(data + offset);");
	EMIT(out.source, "\t\t{}.reserve(sz);", qualifiedName);
	EMIT(out.source, "\t\tdata += sizeof(uint32_t);");
	EMIT(out.source, "\t\tfor (uint32_t i = 0; i < sz; ++i) {{");
	EMIT(out.source, "\t\t{} value;", typeName.fullyQualifiedCppName(type));
	emitDeserializeField(context, out, "value", typeName, type);
	EMIT(out.source, "\t\t\tdata += {};", context->inlinedSizeOf(type));
	EMIT(out.source, "\t\t}}");
}

void emitDeserializeString(Streams& out, std::string qualifiedName) {
	EMIT(out.source, "\t\tauto offset = *reinterpret_cast<const int32_t*>(data + tableOffset + vtable[idx]);");
	EMIT(out.source, "\t\t// concert offset to offset starting from 0");
	EMIT(out.source, "\t\toffset += tableOffset + vtable[idx];");
	EMIT(out.source, "\t\tauto sz = *reinterpret_cast<const uint32_t*>(data + offset);");
	EMIT(out.source,
	     "\t\t{} = std::string(reinterpret_cast<const char*>(data + offset + sizeof(uint32_t)), sz);",
	     qualifiedName);
}

void emitDeserializeField(StaticContext* context,
                          Streams& out,
                          std::string qualifiedName,
                          TypeName typeName,
                          expression::Type const& type);

void emitDeserializeStruct(StaticContext* context,
                           Streams& out,
                           std::string qualifiedName,
                           expression::Struct const& s) {
	// we need to back up the current data pointer since the caller won't expect it to change
	EMIT(out.source, "\t\tauto oldData = data;");
	for (auto const& f : s.fields) {
		if (f.isArrayType) {
			throw Error("Arrays in structs not supported");
		}
		auto fType = assertTrue(context->resolve(f.type));
		emitDeserializeField(context, out, fmt::format("{}.{}", qualifiedName, f.name), fType->first, *fType->second);
		EMIT(out.source, "data += {};", context->inlinedSizeOf(*fType->second));
	}
	EMIT(out.source, "\t\tdata = oldData;");
}

void emitDeserializeField(StaticContext* context,
                          Streams& out,
                          std::string qualifiedName,
                          TypeName typeName,
                          expression::Type const& type) {
	switch (type.typeType()) {
	case expression::TypeType::Primitive: {
		auto t = dynamic_cast<const expression::PrimitiveType&>(type);
		if (t.typeClass == expression::PrimitiveTypeClass::StringType) {
			emitDeserializeString(out, qualifiedName);
		} else {
			EMIT(out.source, "\t\tmemcpy(&{}, data, {});", qualifiedName, t.size());
		}
		break;
	}
	case expression::TypeType::Enum: {
		auto enumType = dynamic_cast<const expression::Enum&>(type);
		auto eType = assertTrue(context->resolve(enumType.type));
		if (eType->second->typeType() != expression::TypeType::Primitive) {
			throw Error(fmt::format(
			    "Underlying type {} of enum {} is not a primitive type", eType->second->name, qualifiedName));
		}
		auto t = dynamic_cast<const expression::PrimitiveType*>(eType->second);
		EMIT(out.source, "\t\tmemcpy(&value.{}, data, {});", qualifiedName, t->size());
		break;
	}
	case expression::TypeType::Union: {
		throw Error("Unexpected Union");
	}
	case expression::TypeType::Struct: {
		// structs are inlined
		auto const& s = dynamic_cast<expression::Struct const&>(type);
		emitDeserializeStruct(context, out, qualifiedName, s);
		break;
	}
	case expression::TypeType::Table: {
		auto qualifiedTypeName = typeName.fullyQualifiedCppName(type);
		EMIT(out.source, "\t\t// table {} of field {}", qualifiedTypeName, qualifiedName);
		EMIT(out.source, "\t\t{}._loadFromOffset(reader, *reinterpret_cast<const uint32_t*>(data));", qualifiedName);
		break;
	}
	}
}

void emitDeserializeUnion(StaticContext* context,
                          Streams& out,
                          std::string qualifiedName,
                          TypeName typeName,
                          expression::Type const& type) {
	auto const& utype = dynamic_cast<expression::Union const&>(type);
	EMIT(out.source, "\t\t// a union is a 1-byte integer followed by a type reference");
	EMIT(out.source, "\t\tuint8_t utype;");
	EMIT(out.source, "\t\tmemcpy(&utype, data + tableOffset + vtable[idx++], 1);");
	EMIT(out.source, "\t\tswitch (utype) {{");
	for (int i = 1; i < utype.types.size(); ++i) {
		auto variantType = assertTrue(context->resolve(utype.types[i]));
		EMIT(out.source, "\t\tcase {}: {{", i);
		if (variantType->second->typeType() != expression::TypeType::Table) {
			throw Error(fmt::format("We currently only support unions of table, but {} in {} is a {}",
			                        variantType->first.fullyQualifiedCppName(*variantType->second),
			                        typeName.fullyQualifiedCppName(type),
			                        expression::toString(variantType->second->typeType())));
		}
		EMIT(out.source, "\t\t\t{} varValue;", variantType->first.fullyQualifiedCppName(*variantType->second));
		EMIT(out.source, "\t\t\tuint32_t varTableOffset;");
		EMIT(out.source, "\t\t\tmemcpy(&varTableOffset, data + tableOffset + vtable[idx], sizeof(uint32_t));");
		EMIT(out.source, "\t\t\tvarValue._loadFromOffset(reader, varTableOffset);");
		EMIT(out.source, "\t\t\t{} = varValue;", qualifiedName);
		EMIT(out.source, "\t\t\tbreak;");
		EMIT(out.source, "\t\t}}");
	}
	EMIT(out.source, "\t\tdefault:");
	EMIT(out.source, "\t\t\tbreak;// not set or unknown, keep default initialized");
	EMIT(out.source, "\t\t}}");
	EMIT(out.source, "\t\t");
}

void emitDeserializeMember(StaticContext* context,
                           Streams& out,
                           std::string qualifiedName,
                           expression::Field const& field) {
	auto fieldType = assertTrue(context->resolve(field.type));
	EMIT(out.source, "\tif (idx >= vsize) {{");
	EMIT(out.source, "\t\t// Some fields are not present, we'll leave them default-initialized");
	EMIT(out.source, "\t\tbreak;");
	EMIT(out.source, "\t}}");
	EMIT(out.source, "\tif (vtable[idx] != 0) {{");
	EMIT(out.source, "\t\t// field is present");
	if (fieldType->second->typeType() == expression::TypeType::Union) {
		emitDeserializeUnion(context, out, qualifiedName, fieldType->first, *fieldType->second);
	} else if (field.isArrayType) {
		EMIT(out.source, "\t\tdata = data + tableOffset + vtable[idx];");
		emitDeserializeArray(context, out, qualifiedName, fieldType->first, *fieldType->second);
	} else {
		EMIT(out.source, "\t\tdata = data + tableOffset + vtable[idx];");
		emitDeserializeField(context, out, qualifiedName, fieldType->first, *fieldType->second);
	}
	EMIT(out.source, "\t}}");
}

void emitDeserialize(StaticContext* context, Streams& out, expression::Table const& table) {
	EMIT(out.header, "\t[[nodiscard]] static {} read(ObjectReader& reader);", table.name);
	EMIT(out.header, "\t[[nodiscard]] static {} read(ArenaObjectReader& reader);", table.name);
	EMIT(out.header, "\t// These two methods are intended to be used by flowserializer exclusively");
	EMIT(out.header, "\tvoid _loadFromOffset(ObjectReader& reader, uint32_t tableOffset);");
	EMIT(out.header, "\tvoid _loadFromOffset(ArenaObjectReader& reader, uint32_t tableOffset);");
	EMIT(out.header, "");

	EMIT(out.source, "namespace {{");

	EMIT(out.source, "template <class Ar>");
	EMIT(out.source, "inline void read{0}(Ar& reader, {0}& value, uoffset_t tableOffset) {{", table.name);
	if (table.fields.size() > 0) {
		EMIT(out.source, "\tconst uint8_t* data = reader.data();");
		EMIT(out.source, "\tauto vtableOffset = *reinterpret_cast<const soffset_t*>(data + tableOffset);");
		EMIT(out.source,
		     "\tconst voffset_t* vtable = reinterpret_cast<const voffset_t*>(data + tableOffset - vtableOffset);");
		EMIT(out.source, "\tvoffset_t vsize = vtable[0] / sizeof(voffset_t);");
		EMIT(out.source, "\tunsigned idx = 1;");
		EMIT(out.source, "do {{");
		for (const auto& f : table.fields) {
			emitDeserializeMember(context, out, fmt::format("value.{}", f.name), f);
			EMIT(out.source, "\t++idx;");
		}
		// we use a goto to make sure we have the option to add post-deserialization functionality. But we still need
		// the option to abort early. A do-while loop would be the alternative to a goto.
		EMIT(out.source, "}} while (false);");
	}
	EMIT(out.source, "}}");
	EMIT(out.source, "");

	EMIT(out.source, "template <class Ar>");
	EMIT(out.source, "{0} read{0}(Ar& reader) {{", table.name);
	EMIT(out.source, "\t{} res;", table.name);
	EMIT(out.source, "\tuoffset_t tableOffset = *reinterpret_cast<const uoffset_t*>(reader.data());");
	EMIT(out.source, "\tread{}(reader, res, tableOffset);", table.name);
	EMIT(out.source, "\treturn res;");
	EMIT(out.source, "}}");
	EMIT(out.source, "}} // namespace");
	EMIT(out.source, "");

	EMIT(out.source, "{0} {0}::read(ObjectReader& reader) {{", table.name);
	EMIT(out.source, "\treturn read{}(reader);", table.name);
	EMIT(out.source, "}}");
	EMIT(out.source, "{0} {0}::read(ArenaObjectReader& reader) {{", table.name);
	EMIT(out.source, "\treturn read{}(reader);", table.name);
	EMIT(out.source, "}}");
	EMIT(out.source, "void {}::_loadFromOffset(ObjectReader& reader, uint32_t tableOffset) {{", table.name);
	EMIT(out.source, "\tread{}(reader, *this, tableOffset);", table.name);
	EMIT(out.source, "}}");
	EMIT(out.source, "void {}::_loadFromOffset(ArenaObjectReader& reader, uint32_t tableOffset) {{", table.name);
	EMIT(out.source, "\tread{}(reader, *this, tableOffset);", table.name);
	EMIT(out.source, "}}");
	EMIT(out.source, "");
}

std::vector<std::string> oldReaders = { "BinaryReader"s, "ArenaReader"s };
std::vector<std::string> oldWriters = { "BinaryWriter"s, "PacketWriter"s };

} // namespace

struct OldSerializers {
	expression::StructOrTable const& st;
};

CodeGenerator::CodeGenerator(StaticContext* context) : context(context) {}

void CodeGenerator::emit(Streams& out, expression::Enum const& f) const {
	auto tableTypeName = assertTrue(context->resolve(f.name))->first;
	auto fullName = fmt::format("{}::{}", fmt::join(tableTypeName.path, "::"), tableTypeName.name);
	auto underlying = convertType(f.type);
	// 0. generate the enum
	{
		Defer defer;
		out.header << fmt::format("enum class {} : {} {{\n", f.name, underlying);
		defer([&out]() { out.header << "};\n\n"; });
		std::vector<std::string> definitions;
		for (auto const& [k, v] : f.values) {
			definitions.push_back(fmt::format("\t{} = {}", k, v));
		}
		out.header << fmt::format("{}\n", fmt::join(definitions, ",\n"));
	}
	// 1. Generate code for old serializers
	// 1.1. Readers
	EMIT(out, "// {} functions for old serializer", fullName);
	for (auto const& ar : oldReaders) {
		out.header << fmt::format("void load({}& ar, {}& out);\n", ar, fullName);

		out.source << fmt::format("void load({}& ar, {}& out) {{\n", ar, fullName);
		out.source << fmt::format("\t{} value;\n", underlying);
		out.source << fmt::format("\tar >> value;\n");
		out.source << fmt::format("\tout = static_cast<{}>(value);\n", fullName);
		out.source << fmt::format("}}\n");
	}
	for (auto const& ar : oldWriters) {
		out.header << fmt::format("void save({}& ar, {} const& in);\n", ar, f.name);
		out.source << fmt::format("void save({}& ar, {} const& in) {{\n", ar, fullName);
		out.source << fmt::format("\t{0} value = static_cast<{0}>(in);\n", underlying);
		out.source << fmt::format("\tar << value;\n");
		out.source << fmt::format("}}\n");
	}
	// 2. Generate the helper functions
	{
		out.header << fmt::format("// {} helper functions\n", f.name);
		out.source << fmt::format("// {} helper functions\n", fullName);
		// toString
		out.header << fmt::format("{0} toString({1});\n", config::stringType, f.name);
		out.source << fmt::format("{0} toString({1} e) {{\n", config::stringType, fullName);
		out.source << "\tswitch (e) {\n";
		for (auto const& [k, _] : f.values) {
			out.source << fmt::format("\tcase {0}::{1}:\n", fullName, k);
			out.source << fmt::format("\t\treturn \"{0}\"{1};\n", k, config::stringLiteral);
		}
		out.source << "\t}\n";
		out.source << "}\n\n";
		// fromString and fromStringView

		auto fromString = [out, fullName, &f](auto stringType, auto stringLiteral) {
			out.header << fmt::format("void fromString({0}& out, {1} const& str);\n", f.name, stringType);
			out.source << fmt::format("void fromString({0}& out, {1} const& str) {{\n", fullName, stringType);
			bool first = true;
			for (auto const& [k, _] : f.values) {
				out.source << fmt::format(
				    "\t{0} (str == \"{1}\"{2}) {{\n", first ? "if" : "} else if", k, stringLiteral);
				out.source << fmt::format("\t\tout = {}::{};\n", fullName, k);
				first = false;
			}
			out.source << "\t} else {\n";
			out.source << fmt::format("\t\t{};\n", config::parseException);
			out.source << "\t}\n";
			out.source << "}\n";
		};
		fromString(config::stringType, config::stringLiteral);
		fromString(config::stringViewType, config::stringViewLiteral);
		out.header << '\n';
		out.source << '\n';
	}
}

void CodeGenerator::emit(Streams& out, expression::Union const& u) const {
	std::vector<std::string> types;
	types.reserve(u.types.size());
	std::transform(
	    u.types.begin(), u.types.end(), std::back_inserter(types), [](auto const& t) { return convertType(t); });
	out.header << fmt::format("using {} = std::variant<{}>;\n", u.name, fmt::join(types, ", "));
	// generate code for old serializer
	out << "// Functions for old serializer\n";
	for (auto const& ar : oldReaders) {
		out.header << fmt::format("void load({}& ar, {}& value);\n", ar, u.name);

		out.source << fmt::format("void load({}& ar, {}& value) {{\n", ar, u.name);
		out.source << fmt::format("\tint idx;\n");
		out.source << fmt::format("\tar >> idx;\n");
		out.source << fmt::format("\tswitch (idx) {{\n");
		for (int i = 0; i < types.size(); ++i) {
			out.source << fmt::format("\tcase {}:\n", i);
			out.source << fmt::format("\t{{\n");
			out.source << fmt::format("\t\t{} v;\n", types[i]);
			out.source << fmt::format("\t\tar >> v;\n");
			out.source << fmt::format("\t\tvalue = v;\n");
			out.source << fmt::format("\t\tbreak;\n");
			out.source << fmt::format("\t}}\n");
		}
		out.source << fmt::format("\tdefault:\n");
		out.source << fmt::format("\t\tUNSTOPPABLE_ASSERT(false);\n");
		out.source << fmt::format("\t}}\n");
		out.source << fmt::format("}}\n");
	}
	for (auto const& ar : oldWriters) {
		out.header << fmt::format("void save({}& ar, {} const& value);\n", ar, u.name);

		out.source << fmt::format("void save({}& ar, {} const& value) {{\n", ar, u.name);
		out.source << fmt::format("\tint idx = value.index();\n");
		out.source << fmt::format("\tar << idx;\n");
		out.source << fmt::format("\tswitch (idx) {{\n");
		for (int i = 0; i < types.size(); ++i) {
			out.source << fmt::format("\tcase {}:\n", i);
			out.source << fmt::format("\t{{\n");
			out.source << fmt::format("\t\tar << std::get<{}>(value);\n", i);
			out.source << fmt::format("\t\tbreak;\n");
			out.source << fmt::format("\t}}\n");
		}
		out.source << fmt::format("\tdefault:\n");
		out.source << fmt::format("\t\tUNSTOPPABLE_ASSERT(false);\n");
		out.source << fmt::format("\t}}\n");
		out.source << fmt::format("}}\n");
	}
}

void CodeGenerator::emit(Streams& out, expression::Field const& f) const {
	std::string assignment;
	auto type = std::string(convertType(f.type));
	if (f.isArrayType) {
		type = fmt::format("std::vector<{}>", type == "bool" ? "uint8_t" : type.c_str());
	}
	if (f.defaultValue) {
		if (expression::primitiveTypes.count(type) > 0) {
			if (expression::primitiveTypes[type].typeClass == expression::PrimitiveTypeClass::StringType) {
				assignment = fmt::format(" = \"{}\"", f.defaultValue.value());
			} else {
				assignment = fmt::format(" = {}", f.defaultValue.value());
			}
		} else {
			// at this point we know this is an enum type
			assignment = fmt::format(" = {}::{}", type, f.defaultValue.value());
		}
	}
	out.header << fmt::format("\t{} {}{};\n", type, f.name, assignment);
}

void CodeGenerator::emit(Streams& out, expression::Struct const& st) const {
	EMIT(out.header, "struct {} {{", st.name);
	EMIT(out.header, "\t[[nodiscard]] flowserializer::Type flowSerializerType() const {{");
	EMIT(out.header, "\t\treturn flowserializer::Type::Struct;");
	EMIT(out.header, "\t}}\n");
	for (auto const& f : st.fields) {
		emit(out, f);
	}
	EMIT(out.header, "}};");
	emit(out, OldSerializers{ st });
	EMIT(out.header, "");
}

void CodeGenerator::emit(struct Streams& out, const OldSerializers& s) const {
	auto tableTypeName = assertTrue(context->resolve(s.st.name))->first;
	auto fullName = fmt::format("{}::{}", fmt::join(tableTypeName.path, "::"), tableTypeName.name);
	for (auto const& w : oldWriters) {
		out.header << fmt::format("void save({}& reader, {} const& in);\n", w, s.st.name);
	}
	for (auto const& r : oldReaders) {
		out.header << fmt::format("void load({}& reader, {}& out);\n", r, s.st.name);
	}

	// write serialization code for old serializers (load and save)
	// 1. Implement the generic functions
	out.source << fmt::format("template<class Ar>\n");
	out.source << fmt::format("void loadImpl(Ar& ar, {}& in) {{\n", fullName);
	for (auto const& f : s.st.fields) {
		out.source << fmt::format("\tar >> in.{};\n", f.name);
	}
	out.source << fmt::format("}}\n\n");
	out.source << fmt::format("template<class Ar>\n");
	out.source << fmt::format("void saveImpl(Ar& ar, {} const& out) {{\n", fullName);
	for (auto const& f : s.st.fields) {
		out.source << fmt::format("\tar << out.{};\n", f.name);
	}
	out.source << fmt::format("}}\n\n");
	// 2. Implement the specializations -- this forces the compiler to instantiate all templates in the current
	//    compilation unit. So we won't do this once per compilation unit
	for (auto const& ar : oldReaders) {
		out.source << fmt::format("void load({}& ar, {}& out) {{\n", ar, fullName);
		out.source << fmt::format("\tloadImpl(ar, out);\n");
		out.source << fmt::format("}}\n");
	}
	out.source << fmt::format("\n");
	for (auto const& ar : oldWriters) {
		out.source << fmt::format("void save({}& ar, {} const& in) {{\n", ar, fullName);
		out.source << fmt::format("\tsaveImpl(ar, in);\n");
		out.source << fmt::format("}}\n");
	}
	out.source << fmt::format("\n");
}

void CodeGenerator::emit(Streams& out, expression::Table const& table) const {
	out.header << fmt::format("struct {} {{\n", table.name);
	out.header << fmt::format("\t[[nodiscard]] flowserializer::Type flowSerializerType() const {{ return "
	                          "flowserializer::Type::Table; }};\n\n");
	out.header << fmt::format("\tstd::pair<uint8_t*, int> write(flowserializer::Writer& w) const;\n");
	emitDeserialize(context, out, table);

	for (auto const& f : table.fields) {
		emit(out, f);
	}
	auto tableTypeName = assertTrue(context->resolve(table.name))->first;
	EMIT(out.source,
	     "std::pair<uint8_t*, int> {}::{}::write(flowserializer::Writer& w) const {{",
	     fmt::join(tableTypeName.path, "::"),
	     table.name);
	// the code to allocate the memory has to be called first, but we can already generate code to write the
	// statically known data
	// TODO: Only generate serialization for root_type
	std::stringstream writer;
	auto serMap = context->serializationInformation(table.name);
	boost::unordered_map<TypeName, int> vtableOffsets;
	int curr = 8;
	// 0. write all vtables
	for (auto const& [typeName, serInfo] : serMap) {
		fmt::print("serMapPath: {}, serMapTypeName: {}, serMapSize: {}, vtable empty: {}\n",
		           fmt::join(typeName.path, "::"),
		           typeName.name,
		           serMap.size(),
		           serInfo.vtable->empty());
		if (serInfo.vtable->empty()) {
			continue;
		}
		writer << fmt::format("\t// vtable for {}{}{}\n",
		                      fmt::join(typeName.path, "::"),
		                      typeName.path.empty() ? "" : "::",
		                      typeName.name);
		for (auto o : *serInfo.vtable) {
			writer << fmt::format("\t*reinterpret_cast<voffset_t*>(buffer + {}) = {};\n", curr, o);
			curr += 2;
		}
		vtableOffsets[typeName] = curr;
	}

	// 1.0. Determine size of all tables
	auto vtable = serMap.at(tableTypeName).vtable;
	int dataSize = curr;
	if (!vtable->empty()) {
		dataSize += vtable.value()[1];
	}

	// 1.5. Iterate through all fields and generate all types simultaneously
	std::stringstream appendData;
	EMIT(writer, "\n\t// table (data or offsets to data)");
	EMIT(writer,
	     "\t*reinterpret_cast<soffset_t*>(buffer + {}) = 0b{:b}; // two's complement offset "
	     "(subtracted from current address to get vtable address)",
	     curr,
	     curr - 8);
	for (int i = 0; i < table.fields.size(); ++i) {
		const auto field = table.fields[i];
		auto fieldType = assertTrue(context->resolve(field.type));
		if (vtable->empty()) {
			continue;
		}
		switch (fieldType->second->typeType()) {
		case expression::TypeType::Primitive: {
			if (field.isArrayType) {
				// TODO: Implement
			} else if (field.type == "int" || field.type == "short" || field.type == "long") {
				EMIT(writer,
				     "\tstd::memcpy(buffer + {}, &{}, sizeof({}));",
				     curr + vtable.value()[i + 2],
				     field.name,
				     field.type);
			} else if (field.type == "string") {
				auto type = dynamic_cast<expression::PrimitiveType const*>(fieldType->second);
				voffset_t offset = dataSize;
				// Offset to string is the offset from where the address the offset is written at!
				EMIT(writer,
				     "\t*reinterpret_cast<uoffset_t*>(buffer + {}) = {};\n",
				     curr + vtable.value()[i + 2],
				     offset - (curr + vtable.value()[i + 2]));
				EMIT(appendData, "\t*reinterpret_cast<uoffset_t*>(buffer + {}) = {}.size();", dataSize, field.name);
				EMIT(appendData,
				     "\tstd::memcpy(buffer + {0} + {1}, {2}.data(), {2}.size());",
				     dataSize,
				     sizeof(uoffset_t),
				     field.name);
				EMIT(appendData,
				     "\t*reinterpret_cast<unsigned char*>(buffer + {} + {} + {}.size() + 1) = 0;",
				     offset,
				     sizeof(uoffset_t),
				     field.name);
				dataSize += 4 + 1 + type->_size;
			}
			break;
		}
		case expression::TypeType::Enum: {
			EMIT(writer,
			     "\t*reinterpret_cast<unsigned char*>(buffer + {}) = static_cast<unsigned char>({});",
			     curr + vtable.value()[i + 2],
			     field.name);
			break;
		}
		case expression::TypeType::Union: {
			// throw Error("NOT IMPLEMENTED");
			break;
		}
		default: {
			// throw Error("NOT IMPLEMENTED");
			break;
		}
		}
	}

	EMIT(out.source, "\tuint8_t* buffer = new uint8_t[{}];\n", dataSize);
	out.source << writer.str();

	std::string appendStr = appendData.str();
	if (!appendStr.empty()) {
		out.source << appendStr;
	}

	// 2. Write header
	EMIT(out.source, "\n\t// header");
	EMIT(out.source, "\t// offset to root table");
	EMIT(out.source, "\t*reinterpret_cast<uoffset_t*>(buffer) = {};", curr);
	EMIT(out.source, "\t// file identifier");
	EMIT(out.source, "\t*reinterpret_cast<uoffset_t*>(buffer + 4) = {};", 0);

	EMIT(out.source, "\treturn std::make_pair(buffer, {});", dataSize);

	EMIT(out.source, "}}");
	EMIT(out.header, "}};");
	emit(out, OldSerializers{ table });
}

void CodeGenerator::emit(Streams& out, expression::ExpressionTree const& tree) const {
	Defer defer;
	if (tree.namespacePath) {
		out.header << fmt::format("namespace {} {{\n", fmt::join(tree.namespacePath.value(), "::"));
		defer([&out, &tree]() {
			out.header << fmt::format("}} // namespace {}\n", fmt::join(tree.namespacePath.value(), "::"));
		});
		out.source << fmt::format("namespace {} {{\n", fmt::join(tree.namespacePath.value(), "::"));
		defer([&out, &tree]() {
			out.source << fmt::format("}} // namespace {}\n", fmt::join(tree.namespacePath.value(), "::"));
		});
	}
	// enums have no dependencies, so we will emit them first
	for (auto const& [_, e] : tree.enums) {
		emit(out, e);
		out.header << "\n";
	}
	auto types = establishEmitOrder(tree);
	for (auto const& t : types) {
		if (auto uIter = tree.unions.find(t); uIter != tree.unions.end()) {
			emit(out, uIter->second);
		} else if (auto sIter = tree.structs.find(t); sIter != tree.structs.end()) {
			emit(out, sIter->second);
		} else if (auto tIter = tree.tables.find(t); tIter != tree.tables.end()) {
			emit(out, tIter->second);
		} else {
			throw Error("BUG");
		}
		out.header << "\n";
	}
}

void CodeGenerator::emit(std::string const& stem,
                         const boost::filesystem::path& header,
                         const boost::filesystem::path& source) const {
	std::ofstream headerStream(header.c_str(), std::ios_base::out | std::ios_base::trunc);
	std::ofstream sourceStream(source.c_str(), std::ios_base::out | std::ios_base::trunc);
	Streams streams{ headerStream, sourceStream };

	auto guard = headerGuard(stem);
	EMIT(headerStream, "// THIS FILE WAS GENERATED BY FLOWFLATC, DO NOT EDIT!");
	EMIT(headerStream, "#ifndef {0}\n#define {0}", guard);
	emitIncludes(streams, context->currentFile->includes);
	Defer defer;
	defer([&headerStream, &guard]() { EMIT(headerStream, "\n#endif // #ifndef {}", guard); });

	EMIT(sourceStream, "// THIS FILE WAS GENERATED BY FLOWFLATC, DO NOT EDIT!");
	EMIT(sourceStream, "#include \"{}\"", header.filename().c_str());
	EMIT(sourceStream, "#include <utility>");
	EMIT(sourceStream, "using namespace flowserializer;");

	emit(streams, *context->currentFile);
}

} // namespace flowserializer
