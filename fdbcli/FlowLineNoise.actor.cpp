/*
 * FlowLineNoise.actor.cpp
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

#include "fdbcli/FlowLineNoise.h"
#include "flow/IThreadPool.h"

#define BOOST_SYSTEM_NO_LIB
#define BOOST_DATE_TIME_NO_LIB
#define BOOST_REGEX_NO_LIB
#include "boost/asio.hpp"

#include "flow/ThreadHelper.actor.h"

#if __unixish__
#define HAVE_LINENOISE 1
#include "fdbcli/linenoise/linenoise.h"
#else
#define HAVE_LINENOISE 0
#endif
#include "flow/actorcompiler.h" // This must be the last #include.

struct LineNoiseReader final : IThreadPoolReceiver {
	void init() override {}

	struct Read final : TypedAction<LineNoiseReader, Read> {
		std::string prompt;
		ThreadReturnPromise<Optional<std::string>> result;

		double getTimeEstimate() const override { return 0.0; }
		explicit Read(std::string const& prompt) : prompt(prompt) {}
	};

	void action(Read& r) {
		try {
			r.result.send(read(r.prompt));
		} catch (Error& e) {
			r.result.sendError(e);
		} catch (...) {
			r.result.sendError(unknown_error());
		}
	}

private:
	Optional<std::string> read(std::string const& prompt) {
#if HAVE_LINENOISE
		errno = 0;
		char* line = linenoise(prompt.c_str());
		if (line) {
			std::string s(line);
			free(line);
			return s;
		} else {
			if (errno == EAGAIN) // Ctrl-C
				return std::string();
			return Optional<std::string>();
		}
#else
		std::string line;
		std::fputs(prompt.c_str(), stdout);
		if (!std::getline(std::cin, line).eof()) {
			return line;
		} else
			return Optional<std::string>();
#endif
	}
};

LineNoise::LineNoise(std::function<void(std::string const&, std::vector<std::string>&)> _completion_callback,
                     std::function<Hint(std::string const&)> _hint_callback,
                     int maxHistoryLines,
                     bool multiline)
  : threadPool(createGenericThreadPool()) {
	reader = new LineNoiseReader();

#if HAVE_LINENOISE
	// It should be OK to call these functions from this thread, since read() can't be called yet
	// The callbacks passed to linenoise*() will be invoked from the thread pool, and use onMainThread() to safely
	// invoke the callbacks we've been given

	// linenoise doesn't provide any form of data parameter to callbacks, so we have to use static variables
	static std::function<void(std::string const&, std::vector<std::string>&)> completion_callback;
	static std::function<Hint(std::string const&)> hint_callback;
	completion_callback = _completion_callback;
	hint_callback = _hint_callback;

	linenoiseHistorySetMaxLen(maxHistoryLines);
	linenoiseSetMultiLine(multiline);
	linenoiseSetCompletionCallback([](const char* line, linenoiseCompletions* lc) {
		// This code will run in the thread pool
		std::vector<std::string> completions;
		onMainThread([line, &completions]() -> Future<Void> {
			completion_callback(line, completions);
			return Void();
		}).getBlocking();
		for (auto const& c : completions)
			linenoiseAddCompletion(lc, c.c_str());
	});
	linenoiseSetHintsCallback([](const char* line, int* color, int* bold) -> char* {
		Hint h = onMainThread([line]() -> Future<Hint> { return hint_callback(line); }).getBlocking();
		if (!h.valid)
			return nullptr;
		*color = h.color;
		*bold = h.bold;
		return strdup(h.text.c_str());
	});
	linenoiseSetFreeHintsCallback(free);
#endif

	threadPool->addThread(reader, "fdb-linenoise");
}

LineNoise::~LineNoise() {
	threadPool->stop();
}

Future<Optional<std::string>> LineNoise::read(std::string const& prompt) {
	auto r = new LineNoiseReader::Read(prompt);
	auto f = r->result.getFuture();
	threadPool->post(r);
	return f;
}

ACTOR Future<Void> waitKeyboardInterrupt(boost::asio::io_service* ios) {
	state boost::asio::signal_set signals(*ios, SIGINT);
	Promise<Void> result;
	signals.async_wait([result](const boost::system::error_code& error, int signal_number) {
		if (error) {
			result.sendError(io_error());
		} else {
			result.send(Void());
		}
	});

	wait(result.getFuture());
	return Void();
}

Future<Void> LineNoise::onKeyboardInterrupt() {
	boost::asio::io_service* ios = (boost::asio::io_service*)g_network->global(INetwork::enASIOService);
	if (!ios)
		return Never();
	return waitKeyboardInterrupt(ios);
}

void LineNoise::historyAdd(std::string const& line) {
#if HAVE_LINENOISE
	linenoiseHistoryAdd(line.c_str());
#endif
}
void LineNoise::historyLoad(std::string const& filename) {
#if HAVE_LINENOISE
	if (linenoiseHistoryLoad(filename.c_str()) != 0) {
		throw io_error();
	}
#endif
}
void LineNoise::historySave(std::string const& filename) {
#if HAVE_LINENOISE
	if (linenoiseHistorySave(filename.c_str()) != 0) {
		throw io_error();
	}
#endif
}
