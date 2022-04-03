/*
 * FileConverter.h
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

#ifndef FDBBACKUP_FILECONVERTER_H
#define FDBBACKUP_FILECONVERTER_H
#pragma once

#include <cinttypes>
#include "flow/SimpleOpt.h"
#include "flow/TLSConfig.actor.h"

namespace file_converter {

// File format convertion constants
enum {
	OPT_CONTAINER,
	OPT_BEGIN_VERSION,
	OPT_BLOB_CREDENTIALS,
	OPT_CRASHONERROR,
	OPT_END_VERSION,
	OPT_TRACE,
	OPT_TRACE_DIR,
	OPT_TRACE_FORMAT,
	OPT_TRACE_LOG_GROUP,
	OPT_INPUT_FILE,
	OPT_BUILD_FLAGS,
	OPT_LIST_ONLY,
	OPT_KEY_PREFIX,
	OPT_HEX_KEY_PREFIX,
	OPT_BEGIN_VERSION_FILTER,
	OPT_END_VERSION_FILTER,
	OPT_KNOB,
	OPT_HELP
};

CSimpleOpt::SOption gConverterOptions[] = { { OPT_CONTAINER, "-r", SO_REQ_SEP },
	                                        { OPT_CONTAINER, "--container", SO_REQ_SEP },
	                                        { OPT_BEGIN_VERSION, "-b", SO_REQ_SEP },
	                                        { OPT_BEGIN_VERSION, "--begin", SO_REQ_SEP },
	                                        { OPT_CRASHONERROR, "--crash", SO_NONE },
	                                        { OPT_END_VERSION, "-e", SO_REQ_SEP },
	                                        { OPT_END_VERSION, "--end", SO_REQ_SEP },
	                                        { OPT_TRACE, "--log", SO_NONE },
	                                        { OPT_TRACE_DIR, "--logdir", SO_REQ_SEP },
	                                        { OPT_TRACE_FORMAT, "--trace-format", SO_REQ_SEP },
	                                        { OPT_TRACE_LOG_GROUP, "--loggroup", SO_REQ_SEP },
	                                        { OPT_INPUT_FILE, "-i", SO_REQ_SEP },
	                                        { OPT_INPUT_FILE, "--input", SO_REQ_SEP },
	                                        { OPT_BLOB_CREDENTIALS, "--blob-credentials", SO_REQ_SEP },
#ifndef TLS_DISABLED
	                                        TLS_OPTION_FLAGS
#endif
	                                        { OPT_BUILD_FLAGS, "--build-flags", SO_NONE },
	                                        { OPT_LIST_ONLY, "--list-only", SO_NONE },
	                                        { OPT_KEY_PREFIX, "-k", SO_REQ_SEP },
	                                        { OPT_HEX_KEY_PREFIX, "--hex-prefix", SO_REQ_SEP },
	                                        { OPT_BEGIN_VERSION_FILTER, "--begin-version-filter", SO_REQ_SEP },
	                                        { OPT_END_VERSION_FILTER, "--end-version-filter", SO_REQ_SEP },
	                                        { OPT_KNOB, "--knob-", SO_REQ_SEP },
	                                        { OPT_HELP, "-?", SO_NONE },
	                                        { OPT_HELP, "-h", SO_NONE },
	                                        { OPT_HELP, "--help", SO_NONE },
	                                        SO_END_OF_OPTIONS };

} // namespace file_converter

#endif // FDBBACKUP_FILECONVERTER_H
