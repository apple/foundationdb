//
// Created by Markus Pilman on 10/14/22.
//

#ifndef FLATBUFFER_PARSER_H
#define FLATBUFFER_PARSER_H
#include "AST.h"

namespace flowserializer {

ast::SchemaDeclaration parseSchema(std::string const& input);
// debuging function
[[maybe_unused]] void printSchema(ast::SchemaDeclaration const& schemaDeclaration);

} // namespace flowserializer
#endif // FLATBUFFER_PARSER_H
