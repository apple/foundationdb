/*
 * Decode.cpp
 *
 * This source file is part of the FoundationDB open source project
 *
 * Copyright 2013-2024 Apple Inc. and the FoundationDB project authors
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

#include "fdbbackup/Decode.h"

#include <iostream>

// Decode an ASCII string, e.g., "\x15\x1b\x19\x04\xaf\x0c\x28\x0a",
// into the binary string. Set "err" to true if the format is invalid.
// Note ',' '\' '," ';' are escaped by '\'. Normal characters can be
// unencoded into HEX, but not recommended.
std::string decode_hex_string(std::string line, bool& err) {
	size_t i = 0;
	std::string ret;

	while (i <= line.length()) {
		switch (line[i]) {
		case '\\':
			if (i + 2 > line.length()) {
				std::cerr << "Invalid hex string at: " << i << "\n";
				err = true;
				return ret;
			}
			switch (line[i + 1]) {
				char ent, save;
			case '"':
			case '\\':
			case ' ':
			case ';':
				line.erase(i, 1);
				break;
			case 'x':
				if (i + 4 > line.length()) {
					std::cerr << "Invalid hex string at: " << i << "\n";
					err = true;
					return ret;
				}
				char* pEnd;
				save = line[i + 4];
				line[i + 4] = 0;
				ent = char(strtoul(line.data() + i + 2, &pEnd, 16));
				if (*pEnd) {
					std::cerr << "Invalid hex string at: " << i << "\n";
					err = true;
					return ret;
				}
				line[i + 4] = save;
				line.replace(i, 4, 1, ent);
				break;
			default:
				std::cerr << "Invalid hex string at: " << i << "\n";
				err = true;
				return ret;
			}
		default:
			i++;
		}
	}

	return line.substr(0, i);
}