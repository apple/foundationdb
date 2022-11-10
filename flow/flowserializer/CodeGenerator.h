//
// Created by Markus Pilman on 10/16/22.
//

#ifndef FLATBUFFER_CODEGENERATOR_H
#define FLATBUFFER_CODEGENERATOR_H

#include "Compiler.h"

namespace flowserializer {

class CodeGenerator {
	StaticContext* context;
	void emit(struct Streams& out, expression::ExpressionTree const& tree) const;
	void emit(struct Streams& out, expression::Enum const& anEnum) const;
	void emit(struct Streams& out, expression::Union const& anUnion) const;
	void emit(struct Streams& out, expression::Field const& field) const;
	void emit(struct Streams& out, expression::Struct const& st) const;
	void emit(struct Streams& out, expression::Table const& table) const;

public:
	explicit CodeGenerator(StaticContext* context);
	void emit(std::string const& stem,
	          boost::filesystem::path const& header,
	          boost::filesystem::path const& source) const;
};

} // namespace flowserializer

#endif // FLATBUFFER_CODEGENERATOR_H
