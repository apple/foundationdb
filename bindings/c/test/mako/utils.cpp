/*
 * utils.cpp
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

#include "utils.hpp"
#include "mako.hpp"
#include <cstdio>
#include <cstdlib>
#include <cstring>
#include <fmt/format.h>

namespace mako {

/* return the last key to be inserted */
/* divide val equally among threads */
int computeThreadPortion(int val, int p_idx, int t_idx, int total_p, int total_t) {
	int interval = val / total_p / total_t;
	int remaining = val - (interval * total_p * total_t);
	if ((p_idx * total_t + t_idx) < remaining) {
		return interval + 1;
	} else if (interval == 0) {
		return -1;
	}
	/* else */
	return interval;
}

/* number of digits */
int digits(int num) {
	int digits = 0;
	while (num > 0) {
		num /= 10;
		digits++;
	}
	return digits;
}

} // namespace mako
