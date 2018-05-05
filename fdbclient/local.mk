#
# local.mk
#
# This source file is part of the FoundationDB open source project
#
# Copyright 2013-2018 Apple Inc. and the FoundationDB project authors
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
#

# -*- mode: makefile; -*-

fdbclient_CFLAGS := $(fdbrpc_CFLAGS)

fdbclient_GENERATED_SOURCES += fdbclient/FDBOptions.g.h

fdbclient/FDBOptions.g.cpp: fdbclient/FDBOptions.g.h
fdbclient/FDBOptions.g.h: bin/vexillographer.exe fdbclient/vexillographer/fdb.options fdbclient/FDBOptions.h
	@echo "Building       $@"
	@$(MONO) bin/vexillographer.exe fdbclient/vexillographer/fdb.options cpp fdbclient/FDBOptions.g

lib/libfdbclient.a: bin/coverage.fdbclient.xml
