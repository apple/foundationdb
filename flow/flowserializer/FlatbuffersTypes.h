//
// Created by Markus Pilman on 10/16/22.
//

#ifndef FLOWSERIALIZER_FLATBUFFERS_TYPES_H
#define FLOWSERIALIZER_FLATBUFFERS_TYPES_H
#include <cstdint>

namespace flowserializer {

enum class Type { Struct, Table };

using uoffset_t = uint32_t;
using soffset_t = int32_t;
using voffset_t = int16_t;

class Writer {};

} // namespace flowserializer

#endif // FLOWSERIALIZER_FLATBUFFERS_TYPES_H