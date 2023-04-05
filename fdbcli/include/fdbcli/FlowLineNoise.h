/*
 * FlowLineNoise.h
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

#ifndef FLOWLINENOISE_H
#define FLOWLINENOISE_H
#pragma once

#include "flow/flow.h"
#include <functional>

struct LineNoise : NonCopyable {
	// Wraps the linenoise library so that it can be called from asynchronous Flow code
	// Only create one of these at a time; the linenoise library only supports one history
	//
	// The current implementation does not support calling read concurrently with any other
	// function (or itself).

	struct Hint {
		std::string text;
		int color;
		bool bold;
		bool valid;
		Hint() : text(), color(), bold(), valid() {}
		Hint(std::string const& text, int color, bool bold) : text(text), color(color), bold(bold), valid(true) {}
	};

	LineNoise(std::function<void(std::string const&, std::vector<std::string>&)> completion_callback,
	          std::function<Hint(std::string const&)> hint_callback,
	          int maxHistoryLines,
	          bool multiline);
	~LineNoise();

	Future<Optional<std::string>> read(std::string const& prompt); // Returns "nothing" on EOF
	void historyAdd(std::string const& line);

	void historyLoad(std::string const& filename);
	void historySave(std::string const& filename);

	static Future<Void> onKeyboardInterrupt(); // Returns when Ctrl-C is next pressed (i.e. SIGINT)

	Reference<class IThreadPool> threadPool;
	struct LineNoiseReader* reader;
};

#endif