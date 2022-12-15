//
// Created by Markus Pilman on 10/16/22.
//

#include <map>

#include "fmt/format.h"
#include <boost/algorithm/string/split.hpp>
#include <boost/algorithm/string/classification.hpp>

#include "Compiler.h"
#include "StaticContext.h"

namespace flowserializer {

using namespace std::string_view_literals;

#pragma clang diagnostic push
#pragma ide diagnostic ignored "cppcoreguidelines-narrowing-conversions"
// calculate the vtable for a
std::vector<flowserializer::voffset_t> generateVTable(
    std::vector<std::pair<unsigned, unsigned>> const& alignmentAndSize) {
	// std::multimap is stable which is a desired property (although not strictly a requirement)
	// 0. Build a mapping alignment -> size -> index
	std::map<unsigned, std::multimap<unsigned, int, std::greater<>>, std::greater<>> map;
	// indexes in the vtable start at 2. 0 is the size of the vtable, idx 1 is the size of the inlined data for the
	// object.
	int idx = 1;
	for (auto p : alignmentAndSize) {
		map[p.first].emplace(p.second, ++idx);
	}
	std::vector<voffset_t> result(alignmentAndSize.size() + 2, voffset_t(0));
	result[0] = 2 * (result.size());
	// order the elements by size
	flowserializer::voffset_t curr = 4;
	for (auto const& [a, m] : map) {
		for (auto [s, i] : m) {
			auto padding = (a - (curr % a)) % a;
			curr += padding;
			result[i] = curr;
			if (s == 0) {
				result[i] = 0;
			}
			curr += s;
		}
	}
	return result;
}
#pragma clang diagnostic pop

expression::Type::Type(std::string name) : name(std::move(name)) {}

StaticContext::StaticContext(Compiler& compiler)
  : compiler(compiler), currentFile(std::make_shared<expression::ExpressionTree>()) {}

boost::unordered_map<TypeName, SerializationInfo> StaticContext::serializationInformation(
    std::string const& name) const {
	auto t = resolve(name);
	assertTrue(t);
	boost::unordered_map<TypeName, SerializationInfo> result;
	serializationInformation(result, *t);
	return result;
}

template <class Iter, class Fun>
auto map(Iter first, Iter last, Fun f) -> std::vector<std::remove_cv_t<decltype(f(*first))>> {
	std::vector<std::remove_cv_t<decltype(f(*first))>> res;
	for (; first != last; ++first) {
		res.push_back(f(*first));
	}
	return res;
}

#pragma clang diagnostic push
#pragma ide diagnostic ignored "cppcoreguidelines-narrowing-conversions"
void StaticContext::serializationInformation(boost::unordered_map<TypeName, SerializationInfo>& state,
                                             const StaticContext::TypeDescr& t) const {
	switch (t.second->typeType()) {
	case expression::TypeType::Primitive: {
		auto type = dynamic_cast<expression::PrimitiveType const*>(t.second);
		state[t.first] = SerializationInfo{ .alignment = type->_size, .staticSize = type->_size };
		break;
	}
	case expression::TypeType::Enum: {
		auto type = dynamic_cast<expression::Enum const*>(t.second);
		// we know that type->type (the underlying type of the enum) has to be a primitive type
		auto p = dynamic_cast<expression::PrimitiveType const*>(assertTrue(resolve(type->type))->second);
		state[t.first] = SerializationInfo{ .alignment = p->_size, .staticSize = p->_size };
		break;
	}
	case expression::TypeType::Union: {
		auto type = dynamic_cast<expression::Union const*>(t.second);
		// first we need to make sure that type information for every possible type is available
		for (auto const& typeName : type->types) {
			auto child = *assertTrue(resolve(typeName));
			if (state.find(child.first) == state.end()) {
				serializationInformation(state, child);
			}
		}
		// a union is serialized using two fields: a choice (2 byte number) and an offset to the actual object (another
		// 2 byte number)
		state[t.first] = SerializationInfo{ .alignment = 4, .staticSize = 8 };
		break;
	}
	case expression::TypeType::Struct:
	case expression::TypeType::Table:
		auto type = dynamic_cast<expression::StructOrTable const*>(t.second);
		// our alignment will simply be the
		unsigned alignment = 4;
		// for tables, we need to store the alignment and the size of each object
		std::vector<std::pair<unsigned, unsigned>> alignmentAndSize;
		bool hasDynamicSize = false;
		std::vector<StaticContext::TypeDescr> fieldTypes;
		fieldTypes.reserve(type->fields.size());
		for (auto const& field : type->fields) {
			auto fieldType = *assertTrue(resolve(field.type));
			if (state.find(fieldType.first) == state.end()) {
				serializationInformation(state, fieldType);
				assertTrue(state.find(fieldType.first) != state.end());
			}
			if (field.isArrayType) {
				alignmentAndSize.emplace_back(4u, 4u);
				hasDynamicSize = true;
			} else {
				auto serInfo = state[fieldType.first];
				auto sz = serInfo.staticSize;
				auto it =
				    std::find_if(field.metadata.begin(), field.metadata.end(), [](expression::MetadataEntry entry) {
					    return entry.type == expression::MetadataType::deprecated;
				    });
				if (it != field.metadata.end()) {
					// Deprecated
					sz = 0;
				}
				alignmentAndSize.emplace_back(serInfo.alignment, sz);
				alignment = std::max(alignment, serInfo.alignment);
			}
			fieldTypes.push_back(std::move(fieldType));
		}
		if (type->typeType() == expression::TypeType::Table) {
			auto vtable = generateVTable(alignmentAndSize);
			if (fieldTypes.size() == 0) {
				vtable[1] = 2 * sizeof(short); // the two entries of the vtable
			} else {
				flowserializer::voffset_t maxOffset = 0;
				int maxIndex = 0;
				for (int i = 2; i < vtable.size(); ++i) {
					if (vtable[i] > maxOffset) {
						maxOffset = vtable[i];
						maxIndex = i - 2;
					}
				}
				assertTrue(maxIndex < fieldTypes.size());
				auto lastElement = state[fieldTypes[maxIndex].first];
				unsigned totalSize = maxOffset + lastElement.staticSize;
				vtable[1] = totalSize;
			}
			state[t.first] = SerializationInfo{ .alignment = alignment, .staticSize = 4, .vtable = std::move(vtable) };
		} else {
			unsigned totalSize = 0;
			for (auto const& field : fieldTypes) {
				auto serInfo = state[field.first];
				auto padding = (serInfo.alignment - (totalSize % serInfo.alignment)) % serInfo.alignment;
				totalSize += padding + serInfo.staticSize;
			}
			state[t.first] = SerializationInfo{ .alignment = alignment, .staticSize = totalSize };
		}
	}
	assertTrue(state.find(t.first) != state.end());
}
#pragma clang diagnostic pop

std::optional<std::pair<TypeName, const expression::Type*>> StaticContext::resolve(TypeName const& name) const {
	using Res = std::pair<TypeName, const expression::Type*>;
	for (auto const& [_, tree] : compiler.files) {
		if (tree->currentFile->namespacePath && *tree->currentFile->namespacePath == name.path) {
			auto res = tree->currentFile->findType(name.name);
			if (res) {
				return Res(name, *res);
			}
		}
	}
	return {};
}

std::optional<std::pair<TypeName, const expression::Type*>> StaticContext::resolve(
    const std::string& name,
    bool excludeCurrent /* = false */) const {
	using Res = std::pair<TypeName, const expression::Type*>;
	if (auto iter = expression::primitiveTypes.find(name); iter != expression::primitiveTypes.end()) {
		return Res(TypeName{ .name = name, .path = std::vector<std::string>() }, &iter->second);
	}
	if (name.find('.') == std::string::npos) {
		// unqualified name -- check whether this is a "local" type (defined in current file
		if (!excludeCurrent) {
			auto res = currentFile->findType(name);
			if (res) {
				return Res(TypeName{ .name = name,
				                     .path = currentFile->namespacePath ? currentFile->namespacePath.value()
				                                                        : std::vector<std::string>() },
				           *res);
			}
		}
		// this type is not defined in the current file -- so either it was defined in another file which defines
		// the same namespace, or it is in the global namespace.

		if (currentFile->namespacePath && !currentFile->namespacePath->empty()) {
			// if a type of this name exists in the global namespace AND in the current namespace, we will return the
			// one from the current namespace. So we have to check there first.
			auto result =
			    resolve(fmt::format("{}.{}", fmt::join(*currentFile->namespacePath, "."), name), excludeCurrent);
			if (result) {
				return result;
			}
		} else {
			// We checked the current namespace by calling ourselves recursively. So now we check whether we can find
			// this type in the global namespace
			for (auto const& [_, tree] : compiler.files) {
				if (!tree->currentFile->namespacePath) {
					auto res = tree->currentFile->findType(name);
					if (res) {
						return Res(TypeName{ .name = name }, *res);
					}
				}
			}
		}
	} else {
		// this is a qualified name, find files in that namespace and check each for this type
		std::vector<std::string> parts;
		boost::algorithm::split(parts, name, boost::is_any_of(","));
		TypeName t{ .name = parts.back() };
		parts.pop_back();
		t.path = std::move(parts);
		return resolve(t);
	}
	return {};
}

std::string multiplyChar(int lhs, char rhs) {
	std::string res;
	res.reserve(lhs);
	for (int i = 0; i < rhs; ++i) {
		res.push_back(rhs);
	}
	return res;
}

std::string operator*(int x, std::string_view str) {
	std::string res;
	res.reserve(str.size() * x);
	for (int i = 0; i < x; ++i) {
		res.append(str.begin(), str.end());
	}
	return res;
}

std::string operator*(int x, std::string const& str) {
	return x * std::string_view(str);
}

void StaticContext::describeTable(std::string const& name) const {
	auto serMap = serializationInformation(name);
	boost::unordered_map<TypeName, int> vtableOffsets;
	int curr = 4;
	for (auto const& [typeName, serInfo] : serMap) {
		if (serInfo.vtable->empty()) {
			continue;
		}
		vtableOffsets[typeName] = curr;
		curr += serInfo.vtable->size() * 2;
	}
	auto root = serMap[assertTrue(resolve(name))->first];
	if (root.alignment == 8) {
		curr += 4; // we align to the element after the vtable offset
	}
	int padding = (root.alignment - (curr % root.alignment)) % root.alignment;
	curr += padding;
	fmt::print("// Start of the buffer\n");
	fmt::print("0: uint32_t {} // Offset to the root table\n", curr);
	int printOffset = 4;
	for (auto const& [typeName, serInfo] : serMap) {
		if (serInfo.vtable->empty()) {
			continue;
		}
		fmt::print("// vtable for {}.{}\n", fmt::join(typeName.path, "::"), typeName.name);
		fmt::print("{}: uint16_t {} // size of table starting from here\n", printOffset, (*serInfo.vtable)[0]);
		printOffset += 2;
		fmt::print("{}: uint16_t {} // Size of object inline data\n", printOffset, (*serInfo.vtable)[1]);
		printOffset += 2;
		auto const& table = dynamic_cast<expression::Table const&>(*assertTrue(resolve(typeName))->second);
		int i = 2;
		for (auto const& field : table.fields) {
			fmt::print("{}: uint16_t {} // Offset to field \"{}\"\n", printOffset, (*serInfo.vtable)[i], field.name);
			printOffset += 2;
			++i;
		}
	}
	if (padding > 0) {
		fmt::print("{}: uint8_t{} // {} bytes padding\n", printOffset, padding * " 0"sv, padding);
		printOffset += padding;
	}
}
} // namespace flowserializer
