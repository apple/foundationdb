//
// Created by Markus Pilman on 10/16/22.
//

#ifndef FLATBUFFER_CONFIG_H
#define FLATBUFFER_CONFIG_H
#include <string_view>
#include <string>

namespace flowserializer::config {
using namespace std::string_view_literals;

constexpr std::string_view usingLiterals =
    "using namespace std::string_literals;\nusing namespace std::string_view_literals;"sv;

constexpr std::string_view stringType = "std::string"sv;
constexpr std::string_view stringViewType = "std::string_view"sv;
constexpr std::string_view stringViewLiteral = "sv"sv;
constexpr std::string_view stringLiteral = "s";

constexpr std::string_view parseException = R"(throw std::runtime_error("parser error"))";
constexpr std::string_view assertFalse = "std::terminate();"sv;

} // namespace flowserializer::config

#endif // FLATBUFFER_CONFIG_H
