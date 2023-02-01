#ifndef MAPPEDRANGE_H
#define MAPPEDRANGE_H

#include <unordered_map>
#include <set>

extern const int MATCH_INDEX_ALL;
extern const int MATCH_INDEX_NONE;
extern const int MATCH_INDEX_MATCHED_ONLY;
extern const int MATCH_INDEX_UNMATCHED_ONLY;

extern const int code_uint8;
extern const int code_bool;

/*
Request section:

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

extern std::unordered_map<int, int> versionToLength;

extern std::unordered_map<int, int> versionToPosMatchIndex;
extern std::unordered_map<int, int> versionToPosFetchLocalOnly;
extern std::set<int> supportedVersions;

/*
Reply section

Reply Schema:
The first byte is always version.
Each field is a type code followed by the contents.
V = 2:
    byte[0] = version
    byte[1] = type code of int *local*
    byte[2] = *local* content
*/

extern std::unordered_map<int, int> versionToPosLocal;

// bytes array in the response of GetMappedRange have certain length for each version
// for version 2, the length is 3
extern const int MAPPED_KEY_VALUE_RESPONSE_BYTES_LENGTH_V2;

#endif