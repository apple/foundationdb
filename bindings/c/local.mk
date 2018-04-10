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

fdb_c_CFLAGS := $(fdbclient_CFLAGS)
fdb_c_LDFLAGS := $(fdbrpc_LDFLAGS)
fdb_c_LIBS := lib/libfdbclient.a lib/libfdbrpc.a lib/libflow.a
fdb_c_tests_LIBS := -Llib -lfdb_c
fdb_c_tests_HEADERS := -Ibindings/c

CLEAN_TARGETS += fdb_c_tests_clean

ifeq ($(PLATFORM),linux)
  fdb_c_LIBS += lib/libstdc++.a -lm -lpthread -lrt -ldl
  fdb_c_LDFLAGS += -Wl,--version-script=bindings/c/fdb_c.map -static-libgcc -Wl,-z,nodelete
  fdb_c_tests_LIBS += -lpthread
endif

ifeq ($(PLATFORM),osx)
  fdb_c_LDFLAGS += -lc++ -Xlinker -exported_symbols_list -Xlinker bindings/c/fdb_c.symbols
  fdb_c_tests_LIBS += -lpthread

  lib/libfdb_c.dylib: bindings/c/fdb_c.symbols

  bindings/c/fdb_c.symbols: bindings/c/foundationdb/fdb_c.h $(ALL_MAKEFILES)
	@awk '{sub(/^[ \t]+/, "");} /^#/ {next;} /DLLEXPORT\ .*[^ ]\(/ {sub(/\(.*/, ""); print "_" $$NF; next;} /DLLEXPORT/ { DLLEXPORT=1; next;} DLLEXPORT==1 {sub(/\(.*/, ""); print "_" $$0; DLLEXPORT=0}' $< | sort | uniq > $@

  fdb_c_clean: fdb_c_symbols_clean

  fdb_c_symbols_clean:
	@rm -f bindings/c/fdb_c.symbols

  fdb_javac_release: lib/libfdb_c.$(DLEXT)
	mkdir -p lib
	rm -f lib/libfdb_c.$(java_DLEXT)-*
	cp lib/libfdb_c.$(DLEXT) lib/libfdb_c.$(DLEXT)-$(VERSION_ID)
	cp lib/libfdb_c.$(DLEXT)-debug lib/libfdb_c.$(DLEXT)-debug-$(VERSION_ID)

  fdb_javac_release_clean:
	rm -f lib/libfdb_c.$(DLEXT)-*
	rm -f lib/libfdb_c.$(javac_DLEXT)-*

  # OS X needs to put its java lib in packages
  packages: fdb_javac_lib_package

  fdb_javac_lib_package: lib/libfdb_c.dylib
	mkdir -p packages
	cp lib/libfdb_c.$(DLEXT) packages/libfdb_c.$(DLEXT)-$(VERSION_ID)
	cp lib/libfdb_c.$(DLEXT)-debug packages/libfdb_c.$(DLEXT)-debug-$(VERSION_ID)
endif

fdb_c_GENERATED_SOURCES += bindings/c/foundationdb/fdb_c_options.g.h bindings/c/fdb_c.g.S bindings/c/fdb_c_function_pointers.g.h

bindings/c/%.g.S bindings/c/%_function_pointers.g.h: bindings/c/%.cpp bindings/c/generate_asm.py $(ALL_MAKEFILES)
	@echo "Scanning       $<"
	@bindings/c/generate_asm.py $(PLATFORM) bindings/c/fdb_c.cpp bindings/c/fdb_c.g.S bindings/c/fdb_c_function_pointers.g.h

.PRECIOUS: bindings/c/fdb_c_function_pointers.g.h

fdb_c_BUILD_SOURCES += bindings/c/fdb_c.g.S

bindings/c/foundationdb/fdb_c_options.g.h: bin/vexillographer.exe fdbclient/vexillographer/fdb.options $(ALL_MAKEFILES)
	@echo "Building       $@"
	@$(MONO) bin/vexillographer.exe fdbclient/vexillographer/fdb.options c $@

bin/fdb_c_performance_test: bindings/c/test/performance_test.c bindings/c/test/test.h fdb_c
	@echo "Compiling      fdb_c_performance_test"
	@$(CC) $(CFLAGS) $(fdb_c_tests_LIBS) $(fdb_c_tests_HEADERS) -o $@ bindings/c/test/performance_test.c

bin/fdb_c_ryw_benchmark: bindings/c/test/ryw_benchmark.c bindings/c/test/test.h fdb_c
	@echo "Compiling      fdb_c_ryw_benchmark"
	@$(CC) $(CFLAGS) $(fdb_c_tests_LIBS) $(fdb_c_tests_HEADERS) -o $@ bindings/c/test/ryw_benchmark.c

packages/fdb-c-tests-$(VERSION)-$(PLATFORM).tar.gz: bin/fdb_c_performance_test bin/fdb_c_ryw_benchmark
	@echo "Packaging      $@"
	@rm -rf packages/fdb-c-tests-$(VERSION)-$(PLATFORM)
	@mkdir -p packages/fdb-c-tests-$(VERSION)-$(PLATFORM)/bin
	@cp bin/fdb_c_performance_test packages/fdb-c-tests-$(VERSION)-$(PLATFORM)/bin
	@cp bin/fdb_c_ryw_benchmark packages/fdb-c-tests-$(VERSION)-$(PLATFORM)/bin
	@tar -C packages -czvf $@ fdb-c-tests-$(VERSION)-$(PLATFORM) > /dev/null
	@rm -rf packages/fdb-c-tests-$(VERSION)-$(PLATFORM)

fdb_c_tests: packages/fdb-c-tests-$(VERSION)-$(PLATFORM).tar.gz

fdb_c_tests_clean:
	@rm -f packages/fdb-c-tests-$(VERSION)-$(PLATFORM).tar.gz
