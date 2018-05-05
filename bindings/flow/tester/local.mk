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

fdb_flow_tester_CFLAGS := -Ibindings/c $(fdbrpc_CFLAGS)
fdb_flow_tester_LDFLAGS := -Llib $(fdbrpc_LDFLAGS) -lfdb_c
fdb_flow_tester_LIBS := lib/libfdb_flow.a lib/libflow.a lib/libfdb_c.$(DLEXT)

fdb_flow_tester: lib/libfdb_c.$(DLEXT)
	@mkdir -p bindings/flow/bin
	@rm -f bindings/flow/bin/fdb_flow_tester
	@cp bin/fdb_flow_tester bindings/flow/bin/fdb_flow_tester

fdb_flow_tester_clean: _fdb_flow_tester_clean

_fdb_flow_tester_clean:
	@rm -rf bindings/flow/bin

ifeq ($(PLATFORM),linux)
  fdb_flow_tester_LIBS += -ldl -lpthread -lrt
  fdb_flow_tester_LDFLAGS += -static-libstdc++ -static-libgcc
else ifeq ($(PLATFORM),osx)
  fdb_flow_tester_LDFLAGS += -lc++
endif
