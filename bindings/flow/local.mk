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

fdb_flow_CFLAGS := -Ibindings/c $(fdbrpc_CFLAGS)
fdb_flow_LDFLAGS := -Llib -lfdb_c $(fdbrpc_LDFLAGS)
fdb_flow_LIBS :=

packages/fdb-flow-$(FLOWVER)-$(PLATFORM)-$(ARCH).tar.gz: fdb_flow
	@echo "Packaging      fdb_flow"
	@rm -rf packages/fdb-flow-$(FLOWVER)-$(PLATFORM)-$(ARCH)
	@mkdir -p packages/fdb-flow-$(FLOWVER)-$(PLATFORM)-$(ARCH)/lib packages/fdb-flow-$(FLOWVER)-$(PLATFORM)-$(ARCH)/include/bindings/flow packages/fdb-flow-$(FLOWVER)-$(PLATFORM)-$(ARCH)/include/bindings/c/foundationdb
	@cp lib/libfdb_flow.a packages/fdb-flow-$(FLOWVER)-$(PLATFORM)-$(ARCH)/lib
	@find bindings/flow -name '*.h' -not -path 'bindings/flow/tester/*' -exec cp {} packages/fdb-flow-$(FLOWVER)-$(PLATFORM)-$(ARCH)/include/bindings/flow \;
	@find bindings/c/foundationdb -name '*.h' -exec cp {} packages/fdb-flow-$(FLOWVER)-$(PLATFORM)-$(ARCH)/include/bindings/c/foundationdb \;
	@tar czf packages/fdb-flow-$(FLOWVER)-$(PLATFORM)-$(ARCH).tar.gz -C packages fdb-flow-$(FLOWVER)-$(PLATFORM)-$(ARCH)
	@rm -rf packages/fdb-flow-$(FLOWVER)-$(PLATFORM)-$(ARCH)

FDB_FLOW: packages/fdb-flow-$(FLOWVER)-$(PLATFORM)-$(ARCH).tar.gz

FDB_FLOW_clean:
	@echo "Cleaning       fdb_flow package"
	@rm -rf packages/fdb-flow-*.tar.gz

packages: FDB_FLOW
packages_clean: FDB_FLOW_clean
