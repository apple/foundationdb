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

fdb_java_LDFLAGS := -Llib
fdb_java_CFLAGS := $(fdbclient_CFLAGS) -Ibindings/c

# We only override if the environment didn't set it (this is used by
# the fdbwebsite documentation build process)
JAVADOC_DIR ?= bindings/java

fdb_java_LIBS := lib/libfdb_c.$(DLEXT)

ifeq ($(RELEASE),true)
  JARVER = $(VERSION)
  APPLEJARVER = $(VERSION)
else
  JARVER = $(VERSION)-PRERELEASE
  APPLEJARVER = $(VERSION)-SNAPSHOT
endif

ifeq ($(PLATFORM),linux)
  JAVA_HOME ?= /usr/lib/jvm/java-8-openjdk-amd64
  fdb_java_CFLAGS += -I$(JAVA_HOME)/include -I$(JAVA_HOME)/include/linux
  fdb_java_LDFLAGS += -static-libgcc

  java_ARCH := amd64
else ifeq ($(PLATFORM),osx)
  JAVA_HOME ?= $(shell /usr/libexec/java_home)
  fdb_java_CFLAGS += -I$(JAVA_HOME)/include -I$(JAVA_HOME)/include/darwin

  java_ARCH := x86_64
endif

JAVA_GENERATED_SOURCES := bindings/java/src/main/java/com/apple/foundationdb/NetworkOptions.java bindings/java/src/main/java/com/apple/foundationdb/ClusterOptions.java bindings/java/src/main/java/com/apple/foundationdb/DatabaseOptions.java bindings/java/src/main/java/com/apple/foundationdb/TransactionOptions.java bindings/java/src/main/java/com/apple/foundationdb/StreamingMode.java bindings/java/src/main/java/com/apple/foundationdb/ConflictRangeType.java bindings/java/src/main/java/com/apple/foundationdb/MutationType.java bindings/java/src/main/java/com/apple/foundationdb/FDBException.java

JAVA_SOURCES := $(JAVA_GENERATED_SOURCES) bindings/java/src/main/java/com/apple/foundationdb/*.java bindings/java/src/main/java/com/apple/foundationdb/async/*.java bindings/java/src/main/java/com/apple/foundationdb/tuple/*.java bindings/java/src/main/java/com/apple/foundationdb/directory/*.java bindings/java/src/main/java/com/apple/foundationdb/subspace/*.java bindings/java/src/test/java/com/apple/foundationdb/test/*.java bindings/java/src/test/java/com/apple/foundationdb/tuple/*.java

fdb_java: $(JAVA_GENERATED_SOURCES)
	@mvn clean install -DskipTests -f ./bindings/java/pom.xml

fdb_java_run_tests:$(JAVA_GENERATED_SOURCES)
	@mvn clean install -f ./bindings/java/pom.xml

fdb_java_target_clean:
	@rm -rf $(JAVA_GENERATED_SOURCES)
	@rm -rf bindings/java/target

# Redefinition of a target already defined in generated.mk, but it's "okay" and the way things were done before.
fdb_java_clean: fdb_java_target_clean

bindings/java/src/main/java/com/apple/foundationdb/StreamingMode.java: bin/vexillographer.exe fdbclient/vexillographer/fdb.options
	@echo "Building       Java options"
	@$(MONO) bin/vexillographer.exe fdbclient/vexillographer/fdb.options java $(@D)

bindings/java/src/main/java/com/apple/foundationdb/MutationType.java: bindings/java/src/main/java/com/apple/foundationdb/StreamingMode.java
	@true

bindings/java/src/main/java/com/apple/foundationdb/ConflictRangeType.java: bindings/java/src/main/java/com/apple/foundationdb/StreamingMode.java
	@true

bindings/java/src/main/java/com/apple/foundationdb/FDBException.java: bindings/java/src/main/java/com/apple/foundationdb/StreamingMode.java
	@true

bindings/java/src/main/java/com/apple/foundationdb/%Options.java: bindings/java/src/main/java/com/apple/foundationdb/StreamingMode.java
	@true

bindings/java/src/main/overview.html: bindings/java/src/main/overview.html.in $(ALL_MAKEFILES) versions.target
	@m4 -DVERSION=$(VERSION) $< > $@

javadoc: $(JAVA_SOURCES) bindings/java/src/main/overview.html
	@echo "Generating     Javadocs"
	@mkdir -p $(JAVADOC_DIR)/javadoc/
	@javadoc -quiet -public -notimestamp -source 1.8 -sourcepath bindings/java/src/main/java \
		-overview bindings/java/src/main/overview.html -d $(JAVADOC_DIR)/javadoc/ \
		-windowtitle "FoundationDB Java Client API" \
		-doctitle "FoundationDB Java Client API" \
		-link "http://docs.oracle.com/javase/8/docs/api" \
		com.apple.foundationdb com.apple.foundationdb.async com.apple.foundationdb.tuple com.apple.foundationdb.directory com.apple.foundationdb.subspace

javadoc_clean:
	@rm -rf $(JAVADOC_DIR)/javadoc
	@rm -f bindings/java/src/main/overview.html

ifeq ($(PLATFORM),linux)

  # We only need javadoc from one source
  TARGETS += javadoc
  CLEAN_TARGETS += javadoc_clean

  # _release builds the lib on macOS and the jars (including the macOS lib) on Linux
  TARGETS += fdb_java_release
  CLEAN_TARGETS += fdb_java_release_clean

  ifneq ($(FATJAR),)
	packages/fdb-java-$(JARVER).jar: $(MAC_OBJ_JAVA) $(WINDOWS_OBJ_JAVA)
  endif

  bindings/java/foundationdb-tests.jar:
	@echo "Building       $@"
	@jar cf $@ -C bindings/java/target/test-classes/ com/apple/foundationdb

  bindings/java/foundationdb-client.jar: lib/libfdb_java.$(DLEXT)
	@echo "Building       $@"
	@rm -rf bindings/java/target/classes/lib/$(PLATFORM)/$(java_ARCH)
	@mkdir -p bindings/java/target/classes/lib/$(PLATFORM)/$(java_ARCH)
	@cp lib/libfdb_java.$(DLEXT) bindings/java/target/classes/lib/$(PLATFORM)/$(java_ARCH)/libfdb_java.$(java_DLEXT)
	@jar cf $@ -C bindings/java/target/classes/ com/apple/foundationdb -C bindings/java/target/classes/ lib


  bindings/java/fdb-java-$(APPLEJARVER).pom: bindings/java/pom.xml
	@echo "Copying        $@"
	sed -e 's/-PRERELEASE/-SNAPSHOT/g' bindings/java/pom.xml > "$@"

  packages/fdb-java-$(JARVER).jar: fdb_java versions.target bindings/java/foundationdb-client.jar
	@echo "Building      $@"
	@rm -f $@
	@rm -rf packages/jar_regular
	@mkdir -p packages/jar_regular
	@cd packages/jar_regular && unzip -qq $(TOPDIR)/bindings/java/foundationdb-client.jar
  ifneq ($(FATJAR),)
	@mkdir -p packages/jar_regular/lib/windows/amd64
	@mkdir -p packages/jar_regular/lib/osx/x86_64
	@cp $(MAC_OBJ_JAVA) packages/jar_regular/lib/osx/x86_64/libfdb_java.jnilib
	@cp $(WINDOWS_OBJ_JAVA) packages/jar_regular/lib/windows/amd64/fdb_java.dll
  endif
	@cd packages/jar_regular && jar cf $(TOPDIR)/$@ *
	@rm -r packages/jar_regular
	@cd bindings && jar uf $(TOPDIR)/$@ ../LICENSE

  packages/fdb-java-$(JARVER)-tests.jar: fdb_java versions.target bindings/java/foundationdb-tests.jar
	@echo "Building       $@"
	@rm -f $@
	@cp $(TOPDIR)/bindings/java/foundationdb-tests.jar packages/fdb-java-$(JARVER)-tests.jar

  packages/fdb-java-$(JARVER)-sources.jar: $(JAVA_GENERATED_SOURCES) versions.target
	@echo "Building       $@"
	@rm -f $@
	@jar cf $(TOPDIR)/$@ -C bindings/java/src/main/java com/apple/foundationdb

  packages/fdb-java-$(JARVER)-javadoc.jar: javadoc versions.target
	@echo "Building       $@"
	@rm -f $@
	@cd $(JAVADOC_DIR)/javadoc/ && jar cf $(TOPDIR)/$@ *
	@cd bindings && jar uf $(TOPDIR)/$@ ../LICENSE

  packages/fdb-java-$(JARVER)-bundle.jar: packages/fdb-java-$(JARVER).jar packages/fdb-java-$(JARVER)-javadoc.jar packages/fdb-java-$(JARVER)-sources.jar bindings/java/fdb-java-$(APPLEJARVER).pom versions.target
	@echo "Building       $@"
	@rm -f $@
	@rm -rf packages/bundle_regular
	@mkdir -p packages/bundle_regular
	@cp packages/fdb-java-$(JARVER).jar packages/fdb-java-$(JARVER)-javadoc.jar packages/fdb-java-$(JARVER)-sources.jar bindings/java/fdb-java-$(APPLEJARVER).pom packages/bundle_regular
	@cp bindings/java/pom.xml packages/bundle_regular/pom.xml
	@cd packages/bundle_regular && jar cf $(TOPDIR)/$@ *
	@rm -rf packages/bundle_regular

  fdb_java_release: packages/fdb-java-$(JARVER)-bundle.jar packages/fdb-java-$(JARVER)-tests.jar

  fdb_java_release_clean:
	@echo "Cleaning       Java release"
	@rm -rf packages/ bindings/java/fdb-java-$(APPLEJARVER).pom bindings/java/foundationdb-tests.jar bindings/java/foundationdb-client.jar

  # Linux is where we build all the java packages
  packages: fdb_java_release
  packages_clean: fdb_java_release_clean

  ifneq ($(FATJAR),)
	MAC_OBJ_JAVA := lib/libfdb_java.jnilib-$(VERSION_ID)
	WINDOWS_OBJ_JAVA := lib/fdb_java.dll-$(VERSION_ID)
  endif

else ifeq ($(PLATFORM),osx)

  TARGETS += fdb_java_release
  CLEAN_TARGETS += fdb_java_release_clean

  fdb_java_release: lib/libfdb_java.$(DLEXT)
	@mkdir -p lib
	@rm -f lib/libfdb_java.$(java_DLEXT)-*
	@cp lib/libfdb_java.$(DLEXT) lib/libfdb_java.$(java_DLEXT)-$(VERSION_ID)
	@cp lib/libfdb_java.$(DLEXT)-debug lib/libfdb_java.$(java_DLEXT)-debug-$(VERSION_ID)

  fdb_java_release_clean:
	@rm -f lib/libfdb_java.$(DLEXT)-*
	@rm -f lib/libfdb_java.$(java_DLEXT)-*

  # macOS needs to put its java lib in packages
  packages: fdb_java_lib_package

  fdb_java_lib_package: fdb_java_release
	mkdir -p packages
	cp lib/libfdb_java.$(java_DLEXT)-$(VERSION_ID) packages
	cp lib/libfdb_java.$(java_DLEXT)-debug-$(VERSION_ID) packages

endif
