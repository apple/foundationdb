//
// Created by Markus Pilman on 10/14/22.
//

#include <iostream>

#include <boost/fusion/adapted.hpp>
#include <boost/spirit/home/x3.hpp>
#include <boost/spirit/home/x3/support/utility/error_reporting.hpp>
#include <boost/spirit/home/x3/support/utility/annotate_on_success.hpp>

#include "AST.h"
#include "Parser.h"

BOOST_FUSION_ADAPT_STRUCT(flowserializer::ast::EnumDeclaration,
                          (std::string, identifier),
                          (std::string, type),
                          (flowserializer::ast::Metadata, metadata),
                          (std::vector<flowserializer::ast::EnumValue>, enumerations))
BOOST_FUSION_ADAPT_STRUCT(flowserializer::ast::UnionDeclaration,
                          (std::string, identifier),
                          (flowserializer::ast::Metadata, metadata),
                          (std::vector<flowserializer::ast::EnumValue>, enumerations))
BOOST_FUSION_ADAPT_STRUCT(flowserializer::ast::FieldDeclaration,
                          (std::string, identifier),
                          (flowserializer::ast::Type, type),
                          (std::optional<flowserializer::ast::SingleValue>, value),
                          (flowserializer::ast::Metadata, metadata))
BOOST_FUSION_ADAPT_STRUCT(flowserializer::ast::StructDeclaration,
                          (std::string, identifier),
                          (flowserializer::ast::Metadata, metadata),
                          (std::vector<flowserializer::ast::FieldDeclaration>, fields))
BOOST_FUSION_ADAPT_STRUCT(flowserializer::ast::TableDeclaration,
                          (std::string, identifier),
                          (flowserializer::ast::Metadata, metadata),
                          (std::vector<flowserializer::ast::FieldDeclaration>, fields))
BOOST_FUSION_ADAPT_STRUCT(flowserializer::ast::ArrayType, (std::string, type))
BOOST_FUSION_ADAPT_STRUCT(flowserializer::ast::IncludeDeclaration, (std::string, path))
BOOST_FUSION_ADAPT_STRUCT(flowserializer::ast::NamespaceDeclaration, (flowserializer::ast::NamespacePath, name))
BOOST_FUSION_ADAPT_STRUCT(flowserializer::ast::AttributeDeclaration, (std::string, attribute))
BOOST_FUSION_ADAPT_STRUCT(flowserializer::ast::RootDeclaration, (std::string, rootType))
BOOST_FUSION_ADAPT_STRUCT(flowserializer::ast::FileExtensionDeclaration, (std::string, extension))
BOOST_FUSION_ADAPT_STRUCT(flowserializer::ast::FileIdentifierDeclaration, (std::string, identifier))
BOOST_FUSION_ADAPT_STRUCT(flowserializer::ast::SchemaDeclaration,
                          (std::vector<flowserializer::ast::IncludeDeclaration>, includes),
                          (std::vector<flowserializer::ast::Declaration>, declarations))

namespace flowserializer {

namespace x3 = boost::spirit::x3;

namespace grammar {

struct FBErrorHandler {
	template <typename Iterator, typename Exception, typename Context>
	x3::error_handler_result on_error(Iterator& first,
	                                  Iterator const& last,
	                                  Exception const& x,
	                                  Context const& context) {
		auto& error_handler = x3::get<x3::error_handler_tag>(context).get();
		std::string message = "Error! Expecting: " + x.which() + " here:";
		error_handler(x.where(), message);
		return x3::error_handler_result::fail;
	}
};

using x3::bool_;
using x3::char_;
using x3::float_;
using x3::int_;
using x3::lexeme;
using x3::lit;

struct struct_decl_class;
struct table_decl_class;
x3::rule<struct_decl_class, ast::StructDeclaration> struct_decl = "struct";
x3::rule<table_decl_class, ast::TableDeclaration> table_decl = "st";

auto semicolon = x3::lit(';');
auto ident = x3::rule<class ident, std::string>{} = lexeme[char_("a-zA-Z_") >> *char_("a-zA-Z0-9_")];
auto string_constant = x3::rule<class string_constant, std::string>{} = '"' >> lexeme[*(~char_('"'))] >> '"';

auto array_type = x3::rule<class array_type, ast::ArrayType>{} = '[' >> ident >> ']';
auto type = x3::rule<class type, ast::Type>{} = ident | array_type;
auto scalar = x3::rule<class scalar, ast::Scalar>{} = bool_ | int_ | float_;
auto single_value = x3::rule<class single_value, ast::SingleValue>{} = scalar | string_constant;

auto metadata_entry = x3::rule<class metadata_entry, std::pair<std::string, std::optional<ast::SingleValue>>>{} =
    ident >> -(':' >> single_value);
auto metadata = x3::rule<class metadata, std::map<std::string, std::optional<ast::SingleValue>>>{} =
    -('(' >> *(metadata_entry % ',') >> ')');

auto enum_val_decl = x3::rule<class enum_val_decl, ast::EnumValue>{} = ident >> -('=' >> int_);
auto enum_decls = x3::rule<class enum_decls, std::vector<ast::EnumValue>>{} = enum_val_decl % ',';
auto enum_decl = x3::rule<class enum_decl, ast::EnumDeclaration>{} =
    "enum" > ident > ':' > ident > metadata > '{' > enum_decls > '}';
auto union_decl = x3::rule<class union_decl, ast::UnionDeclaration>{} =
    "union" >> ident >> metadata >> '{' >> enum_decls >> '}';

auto field_value = x3::rule<class position, std::optional<ast::SingleValue>>{} = -('=' > (string_constant | ident | scalar));
auto field_decl = x3::rule<class field_decl, ast::FieldDeclaration>{} =
    (ident >> ':' >> type >> field_value >> metadata) > ';';
auto const struct_decl_def = "struct" > ident >> metadata > '{' >> +(field_decl) > '}';
auto const table_decl_def = "st" > ident >> metadata > '{' >> +(field_decl) > '}';

auto include_stmt = x3::rule<class include_stmt, ast::IncludeDeclaration>{} =
    lit("include") > string_constant > semicolon;
auto namespace_path = x3::rule<class namespace_path, std::vector<std::string>>{} = ident % '.';
auto namespace_decl = x3::rule<class namespace_decl, ast::NamespaceDeclaration>{} =
    "namespace" >> namespace_path >> semicolon;
auto attribute_decl = x3::rule<class attribute_decl, ast::AttributeDeclaration>{} =
    lit("attribute") >> ident >> semicolon;
auto root_decl = x3::rule<class root_decl, ast::RootDeclaration>{} = lit("root_type") >> ident >> semicolon;
auto file_extension_decl = x3::rule<class file_extension_decl, ast::FileExtensionDeclaration>{} =
    lit("file_extension") >> string_constant >> semicolon;
auto file_identifier_decl = x3::rule<class file_identifier_decl, ast::FileIdentifierDeclaration>{} =
    lit("file_identifier") >> string_constant >> semicolon;

auto declaration = x3::rule<class declarations, ast::Declaration>{} = namespace_decl | attribute_decl | root_decl |
                                                                      file_extension_decl | file_identifier_decl |
                                                                      enum_decl | union_decl | struct_decl | table_decl;
auto schema_decl = x3::rule<class schema_decl, ast::SchemaDeclaration>{} = *include_stmt >> *declaration;

// Skipper -- we want to skip whitespace and comments
auto single_line_comment = "//" >> *(char_ - x3::eol) >> (x3::eol | x3::eoi);
x3::rule<class block_comment> block_comment = "block_comment";
auto block_comment_def = x3::rule<class block_comment>{} = "/*" >> *(block_comment | (char_ - "*/")) >> "*/";
BOOST_SPIRIT_DEFINE(block_comment, struct_decl, table_decl);

struct struct_decl_class : FBErrorHandler, x3::annotate_on_success {};
struct table_decl_class : FBErrorHandler, x3::annotate_on_success {};

auto skipper = x3::space | single_line_comment | block_comment;
} // namespace grammar

} // namespace flowserializer

struct ScalarPrint : boost::static_visitor<std::string> {
	std::string operator()(int i) const { return fmt::format("int: {}", i); }
	std::string operator()(float i) const { return fmt::format("float: {}", i); }
	std::string operator()(bool i) const { return fmt::format("bool: {}", i); }
};
struct SingleValuePrint : boost::static_visitor<std::string> {
	std::string operator()(std::string const& str) const { return fmt::format("String Constant: \"{}\"", str); }
	std::string operator()(flowserializer::ast::Scalar const& sv) const {
		auto s = boost::apply_visitor(ScalarPrint(), sv);
		return fmt::format("Single value: {}", s);
	}
};

namespace {

struct PrintVisitor : flowserializer::ast::Visitor {
	int level = 0;
	[[nodiscard]] std::string prefix() const { return fmt::format("{:\t>{}}", "", level); }
	~PrintVisitor() override = default;
	void visit(const flowserializer::ast::IncludeDeclaration& declaration) override {
		fmt::print("{}Include path={}\n", prefix(), declaration.path);
		++level;
	}
	void visit(const flowserializer::ast::NamespaceDeclaration& declaration) override {
		fmt::print("{}Namespace path={}\n", prefix(), fmt::join(declaration.name, "/"));
		++level;
	}
	void visit(const flowserializer::ast::AttributeDeclaration& declaration) override {
		fmt::print("{}Attribute: {}\n", prefix(), declaration.attribute);
		++level;
	}
	void visit(const flowserializer::ast::SchemaDeclaration& declaration) override {
		fmt::print("{}Schema:\n", prefix());
		++level;
	}
	void endVisit(const flowserializer::ast::IncludeDeclaration& declaration) override { --level; }
	void endVisit(const flowserializer::ast::NamespaceDeclaration& declaration) override { --level; }
	void endVisit(const flowserializer::ast::AttributeDeclaration& declaration) override { --level; }
	// void endVisit(const struct Declaration& declaration) override { --level; }
	void endVisit(const flowserializer::ast::SchemaDeclaration& declaration) override { --level; }
	void visit(const flowserializer::ast::RootDeclaration& declaration) override {
		fmt::print("{}Root type: {}\n", prefix(), declaration.rootType);
		++level;
	}
	void endVisit(const flowserializer::ast::RootDeclaration& declaration) override { --level; }
	void visit(const flowserializer::ast::FileExtensionDeclaration& declaration) override {
		fmt::print("{}File extension: {}\n", prefix(), declaration.extension);
	}
	void visit(const struct flowserializer::ast::FileIdentifierDeclaration& declaration) override {
		fmt::print("{}File identifier: {}\n", prefix(), declaration.identifier);
	}

	void printMetadata(flowserializer::ast::Metadata const& metadata) {
		fmt::print("{}Metadata\n", prefix());
		++level;
		for (auto const& [key, value] : metadata) {
			fmt::print("{}key={} value={}\n",
			           prefix(),
			           key,
			           value ? boost::apply_visitor(SingleValuePrint(), value.value()) : "[UNSET]");
		}
		--level;
	}

	void printEnumeration(std::vector<flowserializer::ast::EnumValue> const& enumerations) {
		fmt::print("{}Enum Values\n", prefix());
		++level;
		for (auto const& val : enumerations) {
			fmt::print("{}k={} = {}\n",
			           prefix(),
			           val.first,
			           val.second ? std::to_string(val.second.value()) : std::string("[NOT SET]"));
		}
		--level;
	}

	void visit(const struct flowserializer::ast::EnumDeclaration& declaration) override {
		fmt::print("{}Enum: Name={} Type={}\n", prefix(), declaration.identifier, declaration.type);
		++level;
		printMetadata(declaration.metadata);
		printEnumeration(declaration.enumerations);
		--level;
	}
	void visit(const struct flowserializer::ast::UnionDeclaration& declaration) override {
		fmt::print("{}Union: Name={}\n", prefix(), declaration.identifier);
		++level;
		printMetadata(declaration.metadata);
		printEnumeration(declaration.enumerations);
		--level;
	}
	void endVisit(const struct flowserializer::ast::EnumDeclaration& declaration) override {}
};

} // namespace

namespace flowserializer {

ast::SchemaDeclaration parseSchema(const std::string& input) {
	ast::SchemaDeclaration result;
	auto begin = input.begin(), end = input.end();
	using error_handler_type = boost::spirit::x3::error_handler<std::string::const_iterator>;
	error_handler_type error_handler(begin, end, std::cerr);
	auto parser = x3::with<x3::error_handler_tag>(std::ref(error_handler))[grammar::schema_decl];
	auto success = boost::spirit::x3::phrase_parse_main(begin, input.end(), parser, grammar::skipper, result);
	fmt::print("Parsed file {}\n", success ? "successfully" : "usuccessfully");
	if (begin != input.end()) {
		fmt::print("Could not parse: {}", std::string(begin, input.end()));
	}
	if (success && begin == input.end()) {
		return result;
	}
	throw std::runtime_error("Parser Error");
}

void printSchema(const ast::SchemaDeclaration& schemaDeclaration) {
	PrintVisitor visitor;
	schemaDeclaration.accept(visitor);
}

} // namespace flowserializer