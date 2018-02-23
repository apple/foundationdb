#
# include.mk
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

# -*- mode: makefile-gmake; -*-

TARGETS += fdb_node fdb_node_npm
CLEAN_TARGETS += fdb_node_clean fdb_node_npm_clean

NODE_VERSIONS := 0.8.22 0.10.0

NODE_DIST_URL ?= https://nodejs.org/dist
NODE_REGISTRY_URL ?= https://registry.npmjs.org/

ifeq ($(RELEASE),true)
  NPMVER = $(VERSION)
else
  NPMVER = $(VERSION)-PRERELEASE
endif

packages: fdb_node_npm

packages_clean: fdb_node_npm_clean

fdb_node: fdb_c bindings/nodejs/fdb_node.stamp

bindings/nodejs/fdb_node.stamp: bindings/nodejs/src/FdbOptions.g.cpp bindings/nodejs/src/*.cpp bindings/nodejs/src/*.h bindings/nodejs/binding.gyp lib/libfdb_c.$(DLEXT) bindings/nodejs/package.json
	@echo "Building       $@"
	@rm -f $@
	@cd bindings/nodejs && \
	mkdir -p modules && \
	rm -rf modules/* && \
	for ver in $(NODE_VERSIONS); do \
		MMVER=`echo $$ver | sed -e 's,\., ,g' | awk '{print $$1 "." $$2}'` && \
		mkdir modules/$$MMVER && \
		node-gyp configure --dist-url=$(NODE_DIST_URL) --target=$$ver && \
		node-gyp -v build && \
		cp build/Release/fdblib.node modules/$${MMVER} ; \
	done
	@touch $@

bindings/nodejs/src/FdbOptions.g.cpp: bin/vexillographer.exe fdbclient/vexillographer/fdb.options
	@echo "Building       $@"
	@$(MONO) bin/vexillographer.exe fdbclient/vexillographer/fdb.options nodejs $@

fdb_node_clean:
	@echo "Cleaning       fdb_node"
	@rm -f bindings/nodejs/src/FdbOptions.g.cpp
	@rm -rf bindings/nodejs/modules
	@(cd bindings/nodejs && node-gyp clean)
	@rm -f bindings/nodejs/fdb_node.stamp

bindings/nodejs/package.json: bindings/nodejs/package.json.in $(ALL_MAKEFILES) versions.target
	@m4 -DVERSION=$(NPMVER) $< > $@
	@echo "Updating       Node dependencies"
	@cd bindings/nodejs && \
	npm config set registry "$(NODE_REGISTRY_URL)" && \
	npm update

fdb_node_npm: fdb_node versions.target bindings/nodejs/README.md bindings/nodejs/lib/*.js bindings/nodejs/src/* bindings/nodejs/binding.gyp LICENSE
	@echo "Packaging      NPM"
	@mkdir -p packages
	@rm -f packages/fdb-node-*
	@rm -rf packages/nodejs.tmp
	@mkdir -p packages/nodejs.tmp/nodejs
	@cp LICENSE packages/nodejs.tmp/nodejs/LICENSE
	@tar -C bindings -czf packages/fdb-node-$(NPMVER)-$(PLATFORM)-$(ARCH).tar.gz nodejs/lib nodejs/modules nodejs/package.json nodejs/README.md -C ../packages/nodejs.tmp nodejs/LICENSE
	@rm -rf packages/nodejs.tmp
ifeq ($(PLATFORM),linux)
	@echo "Packaging      NPM (unbuilt)"
	@rm -rf packages/nodejs.tmp
	@mkdir -p packages/nodejs.tmp/npmsrc/nodejs
	@cat bindings/nodejs/package.json | grep -v private | grep -v engineStrict | awk '/"semver"/ {print "        \"bindings\": \"*\""; next} {print}' > packages/nodejs.tmp/npmsrc/nodejs/package.json
	@cp -r bindings/nodejs/lib bindings/nodejs/src bindings/nodejs/README.md LICENSE packages/nodejs.tmp/npmsrc/nodejs
	@cp bindings/nodejs/binding.gyp.npmsrc packages/nodejs.tmp/npmsrc/nodejs/binding.gyp
	@cp bindings/nodejs/fdbModule.js.npmsrc packages/nodejs.tmp/npmsrc/nodejs/lib/fdbModule.js
	@tar -C packages/nodejs.tmp/npmsrc -czf packages/fdb-node-$(NPMVER).tar.gz nodejs
	@rm -rf packages/nodejs.tmp
endif

fdb_node_npm_clean:
	@echo "Cleaning       NPM"
	@rm -f packages/fdb-node-* bindings/nodejs/package.json
