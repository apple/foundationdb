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

flow_CFLAGS := -I$(BOOSTDIR) -I. -Iflow -DUSE_UCONTEXT
flow_LDFLAGS :=

ifeq ($(PLATFORM),osx)
  flow_CFLAGS += -fasynchronous-unwind-tables -fno-omit-frame-pointer
  flow_LDFLAGS += -framework CoreFoundation -framework IOKit
endif

GENERATED_SOURCES += flow/hgVersion.h versions.h

flow/hgVersion.h: FORCE
	@echo "Checking       hgVersion.h"
	@echo "const char *hgVersion = \"$(VERSION_ID)\";" > flow/hgVersion.h.new
	@([ -e flow/hgVersion.h ] && diff -q flow/hgVersion.h flow/hgVersion.h.new >/dev/null && rm flow/hgVersion.h.new) || mv flow/hgVersion.h.new flow/hgVersion.h

lib/libflow.a: bin/coverage.flow.xml

ifeq ($(RELEASE),true)
  FLOWVER = $(VERSION)
else
  FLOWVER = $(VERSION)-PRERELEASE
endif

packages/flow-$(FLOWVER)-$(PLATFORM)-$(ARCH).tar.gz: flow
	@echo "Packaging      flow"
	@rm -rf packages/flow-$(FLOWVER)-$(PLATFORM)-$(ARCH)
	@mkdir -p packages/flow-$(FLOWVER)-$(PLATFORM)-$(ARCH)/bin packages/flow-$(FLOWVER)-$(PLATFORM)-$(ARCH)/lib packages/flow-$(FLOWVER)-$(PLATFORM)-$(ARCH)/include/flow
	@cp lib/libflow.a packages/flow-$(FLOWVER)-$(PLATFORM)-$(ARCH)/lib
	@cp bin/actorcompiler.exe packages/flow-$(FLOWVER)-$(PLATFORM)-$(ARCH)/bin
	@find flow -name '*.h' -exec cp {} packages/flow-$(FLOWVER)-$(PLATFORM)-$(ARCH)/include/flow \;
	@tar czf packages/flow-$(FLOWVER)-$(PLATFORM)-$(ARCH).tar.gz -C packages flow-$(FLOWVER)-$(PLATFORM)-$(ARCH)
	@rm -rf packages/flow-$(FLOWVER)-$(PLATFORM)-$(ARCH)

FLOW: packages/flow-$(FLOWVER)-$(PLATFORM)-$(ARCH).tar.gz

FLOW_clean:
	@echo "Cleaning       flow"
	@rm -rf packages/flow-*.tar.gz

packages: FLOW
packages_clean: FLOW_clean
