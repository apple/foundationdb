#include "fdbclient/MappedRange.h"

const int MATCH_INDEX_ALL = 0;
const int MATCH_INDEX_NONE = 1;
const int MATCH_INDEX_MATCHED_ONLY = 2;
const int MATCH_INDEX_UNMATCHED_ONLY = 3;

/*
Request Schema:
The first byte is always version.
Each field is a type code followed by the contents.
V = 2:
    byte[0] = version
    byte[1] = type code of int *matchIndex*
    byte[2-5] = *matchIndex* content in little endian
    byte[6] = type code of bool *fetchLocalOnly*
    byte[7] = *fetchLocalOnly* content
*/

std::unordered_map<int, int> versionToLength = { std::make_pair(2, 5) };

std::unordered_map<int, int> versionToPosMatchIndex = { std::make_pair(2, 1) };
std::unordered_map<int, int> versionToPosFetchLocalOnly = { std::make_pair(2, 3) };
std::set<int> supportedVersions{ 2 };

const int code_uint8 = 1;
const int code_bool = 2;

// bytes array in the response of GetMappedRange have certain length for each version
// for version 2, the length is 3
const int MAPPED_KEY_VALUE_RESPONSE_BYTES_LENGTH_V2 = 3;

/*
Reply Schema:
The first byte is always version.
Each field is a type code followed by the contents.
V = 2:
    byte[0] = version
    byte[1] = type code of int *local*
    byte[2] = *local* content
*/

std::unordered_map<int, int> versionToPosLocal = { std::make_pair(2, 1) };
