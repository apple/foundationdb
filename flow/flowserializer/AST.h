//
// Created by Markus Pilman on 10/14/22.
//

#ifndef FLATBUFFER_AST_H
#define FLATBUFFER_AST_H

#include <boost/spirit/home/x3/support/ast/variant.hpp>
#include <boost/spirit/home/x3/support/ast/position_tagged.hpp>

#include "fmt/format.h"

namespace flowserializer::ast {

class Visitor {
public:
	virtual ~Visitor() = default;
	virtual void visit(struct IncludeDeclaration const&){};
	virtual void endVisit(struct IncludeDeclaration const&){};
	virtual void visit(struct NamespaceDeclaration const&){};
	virtual void endVisit(struct NamespaceDeclaration const&){};
	virtual void visit(struct AttributeDeclaration const&){};
	virtual void endVisit(struct AttributeDeclaration const&){};
	virtual void visit(struct Declaration const&) {}
	virtual void endVisit(struct Declaration const&) {}
	virtual void visit(struct SchemaDeclaration const&) {}
	virtual void endVisit(struct SchemaDeclaration const&) {}
	virtual void visit(struct RootDeclaration const&) {}
	virtual void endVisit(struct RootDeclaration const&) {}
	virtual void visit(struct FileExtensionDeclaration const&) {}
	virtual void endVisit(struct FileExtensionDeclaration const&) {}
	virtual void visit(struct FileIdentifierDeclaration const&) {}
	virtual void endVisit(struct FileIdentifierDeclaration const&) {}
	virtual void visit(struct ArrayType const&) {}
	virtual void endVisit(struct ArrayType const&) {}
	virtual void visit(struct EnumDeclaration const&) {}
	virtual void endVisit(struct EnumDeclaration const&) {}
	virtual void visit(struct UnionDeclaration const&) {}
	virtual void endVisit(struct UnionDeclaration const&) {}
	virtual void visit(struct FieldDeclaration const&) {}
	virtual void endVisit(struct FieldDeclaration const&) {}
	virtual void visit(struct StructDeclaration const&) {}
	virtual void endVisit(struct StructDeclaration const&) {}
	virtual void visit(struct TableDeclaration const&) {}
	virtual void endVisit(struct TableDeclaration const&) {}
};

using EnumValue = std::pair<std::string, std::optional<int>>;

template <class Base>
struct DefaultAccept : boost::spirit::x3::position_tagged {
	void accept(Visitor& visitor) const {
		visitor.visit(static_cast<Base const&>(*this));
		visitor.endVisit(static_cast<Base const&>(*this));
	}
};

struct IncludeDeclaration : DefaultAccept<IncludeDeclaration> {
	std::string path;
};

using NamespacePath = std::vector<std::string>;

struct NamespaceDeclaration : DefaultAccept<NamespaceDeclaration> {
	std::vector<std::string> name;
};

struct AttributeDeclaration : DefaultAccept<AttributeDeclaration> {
	std::string attribute;
};

struct RootDeclaration : DefaultAccept<RootDeclaration> {
	std::string rootType;
};

struct FileExtensionDeclaration : DefaultAccept<FileExtensionDeclaration> {
	std::string extension;
};

struct FileIdentifierDeclaration : DefaultAccept<FileIdentifierDeclaration> {
	std::string identifier;
};

struct ArrayType : DefaultAccept<ArrayType> {
	std::string type;
};

struct Type : boost::spirit::x3::variant<std::string, ArrayType>, DefaultAccept<Type> {
	using base_type::base_type;
	using base_type::operator=;

	struct type_visitor : boost::static_visitor<std::string const&> {
		std::string const& operator()(ArrayType const& t) const { return t.type; }
		std::string const& operator()(std::string const& s) const { return s; }
	};

	struct is_array_visitor : boost::static_visitor<bool> {
		bool operator()(ArrayType const& t) const { return true; }
		bool operator()(std::string const& s) const { return false; }
	};

	[[nodiscard]] std::string const& type() const { return boost::apply_visitor(type_visitor(), *this); }

	[[nodiscard]] bool isArray() const { return boost::apply_visitor(is_array_visitor(), *this); }
};

struct Scalar : boost::spirit::x3::variant<int, float, bool>, boost::spirit::x3::position_tagged {
	using base_type::base_type;
	using base_type::operator=;

	[[nodiscard]] std::string toString() const {
		struct StringVisitor : boost::static_visitor<std::string> {
			std::string operator()(int i) const { return std::to_string(i); }
			std::string operator()(float i) const { return std::to_string(i); }
			std::string operator()(bool i) const { return i ? "true" : "false"; }
		};
		return boost::apply_visitor(StringVisitor(), *this);
	}
};

struct SingleValue : boost::spirit::x3::variant<Scalar, std::string>, boost::spirit::x3::position_tagged {
	using base_type::base_type;
	using base_type::operator=;

	[[nodiscard]] std::string toString() const {
		struct StringVisitor : boost::static_visitor<std::string> {
			std::string operator()(Scalar const& s) const { return s.toString(); }
			std::string operator()(std::string const& s) const { return s; }
		};
		return boost::apply_visitor(StringVisitor(), *this);
	}
};

using Metadata = std::map<std::string, std::optional<SingleValue>>;

struct EnumDeclaration : DefaultAccept<EnumDeclaration> {
	std::string identifier;
	std::string type;
	Metadata metadata;
	std::vector<EnumValue> enumerations;
};

struct UnionDeclaration : DefaultAccept<UnionDeclaration> {
	std::string identifier;
	Metadata metadata;
	std::vector<EnumValue> enumerations;
};

struct FieldDeclaration : DefaultAccept<FieldDeclaration> {
	std::string identifier;
	Type type;
	std::optional<SingleValue> value;
	Metadata metadata;
};

struct StructDeclaration : boost::spirit::x3::position_tagged {
	std::string identifier;
	Metadata metadata;
	std::vector<FieldDeclaration> fields;

	void accept(Visitor& visitor) const {
		visitor.visit(*this);
		for (auto const& field : fields) {
			field.accept(visitor);
		}
		visitor.endVisit(*this);
	}
};

struct TableDeclaration : boost::spirit::x3::position_tagged {
	std::string identifier;
	Metadata metadata;
	std::vector<FieldDeclaration> fields;

	void accept(Visitor& visitor) const {
		visitor.visit(*this);
		for (auto const& field : fields) {
			field.accept(visitor);
		}
		visitor.endVisit(*this);
	}
};

struct Declaration : boost::spirit::x3::variant<NamespaceDeclaration,
                                                AttributeDeclaration,
                                                RootDeclaration,
                                                FileExtensionDeclaration,
                                                FileIdentifierDeclaration,
                                                EnumDeclaration,
                                                UnionDeclaration,
                                                StructDeclaration,
                                                TableDeclaration> {
	using base_type::base_type;
	using base_type::operator=;

	struct variant_visitor : boost::static_visitor<> {
		Visitor& v;

		explicit variant_visitor(Visitor& v) : v(v) {}

		void operator()(NamespaceDeclaration const& decl) const { decl.accept(v); }
		void operator()(AttributeDeclaration const& decl) const { decl.accept(v); }
		void operator()(RootDeclaration const& decl) const { decl.accept(v); }
		void operator()(FileExtensionDeclaration const& decl) const { decl.accept(v); }
		void operator()(FileIdentifierDeclaration const& decl) const { decl.accept(v); }
		void operator()(EnumDeclaration const& decl) const { decl.accept(v); }
		void operator()(UnionDeclaration const& decl) const { decl.accept(v); }
		void operator()(StructDeclaration const& decl) const { decl.accept(v); }
		void operator()(TableDeclaration const& decl) const { decl.accept(v); }
	};

	void accept(Visitor& visitor) const {
		visitor.visit(*this);
		boost::apply_visitor(variant_visitor(visitor), *this);
		visitor.endVisit(*this);
	}
};

struct SchemaDeclaration {
	std::vector<IncludeDeclaration> includes;
	std::vector<Declaration> declarations;

	void accept(Visitor& visitor) const {
		visitor.visit(*this);
		for (auto const& incl : includes) {
			incl.accept(visitor);
		}
		for (auto const& decl : declarations) {
			decl.accept(visitor);
		}
		visitor.endVisit(*this);
	}
};
} // namespace flowserializer::ast
#endif // FLATBUFFER_AST_H
