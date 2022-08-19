/*
 * JsonBuilder.cpp
 *
 * This source file is part of the FoundationDB open source project
 *
 * Copyright 2013-2022 Apple Inc. and the FoundationDB project authors
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

#include "fdbclient/JsonBuilder.h"
#include <iostream>

JsonBuilderObject JsonBuilder::makeMessage(const char* name, const char* description) {
	JsonBuilderObject out;
	out["name"] = name;
	out["description"] = description;
	return out;
}

// dst must have at least len + 3 bytes available (".e" becomes "0.0e0")
// Returns bytes written, or 0 on failure.
int JsonBuilder::coerceAsciiNumberToJSON(const char* s, int len, char* dst) {
	if (len == 0) {
		return 0;
	}

	const char* send = s + len;
	char* wptr = dst;
	bool dot = false;

	// Allow one optional sign
	if (*s == '-') {
		*wptr++ = *s++;

		// Output not yet valid so return failure
		if (s == send) {
			return 0;
		}
	}

	// 'inf' becomes 1e99
	if (*s == 'i') {
		if (len >= 3 && (strncmp(s, "inf", 3) == 0)) {
			strcpy(wptr, "1e99");
			return 4 + wptr - dst;
		}
		// Anything else starting with 'i' is a failure
		return 0;
	}

	// Skip leading zeroes
	while (*s == '0') {
		++s;

		// If found end, number is valid and zero
		if (s == send) {
			*wptr++ = '0';
			return wptr - dst;
		}
	}

	// If a dot is found, write a zero before it
	if (*s == '.') {
		dot = true;
		*wptr++ = '0';
		*wptr++ = *s++;

		// If found end, add a zero and return
		if (s == send) {
			*wptr++ = '0';
			return wptr - dst;
		}

		// If there is no digit after the dot, write a zero
		if (!isdigit(*s)) {
			*wptr++ = '0';
		}
	}

	// Write all digits found
	while (isdigit(*s)) {
		*wptr++ = *s++;

		// If found end, number is valid so return
		if (s == send) {
			return wptr - dst;
		}
	}
	// If there is a dot, return unless its the first
	if (*s == '.') {
		if (dot) {
			return wptr - dst;
		}
		*wptr++ = *s++;

		// If found end, add a zero and return
		if (s == send) {
			*wptr++ = '0';
			return wptr - dst;
		}

		// If there are more digits write them, else write a 0
		if (isdigit(*s)) {
			do {
				*wptr++ = *s++;

				// If found end, number is valid so return
				if (s == send) {
					return wptr - dst;
				}

			} while (isdigit(*s));
		} else {
			*wptr++ = '0';
		}
	}
	// Now we can have an e or E, else stop
	if (*s == 'e' || *s == 'E') {
		*wptr++ = *s++;

		// If found end, add a zero and return
		if (s == send) {
			*wptr++ = '0';
			return wptr - dst;
		}

		// Allow one optional sign
		if (*s == '-' || *s == '+') {
			*wptr++ = *s++;
		}

		// If found end, add a zero and return
		if (s == send) {
			*wptr++ = '0';
			return wptr - dst;
		}

		// If there are more digits write then, else write a 0
		if (isdigit(*s)) {
			do {
				*wptr++ = *s++;

				// If found end, number is valid so return
				if (s == send) {
					return wptr - dst;
				}

			} while (isdigit(*s));
		} else {
			*wptr++ = '0';
		}
	}

	return wptr - dst;
}
