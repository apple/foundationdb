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
fdb_java_LDFLAGS += -Llib

ifeq ($(RELEASE),true)
  JARVER = $(VERSION)
  APPLEJARVER = $(VERSION)
else
  JARVER = $(VERSION)-PRERELEASE
  APPLEJARVER = $(VERSION)-SNAPSHOT
endif

define add_java_binding_targets

  JAVA$(1)_GENERATED_SOURCES := bindings/java/src$(1)/main/com/apple/cie/foundationdb/NetworkOptions.java bindings/java/src$(1)/main/com/apple/cie/foundationdb/ClusterOptions.java bindings/java/src$(1)/main/com/apple/cie/foundationdb/DatabaseOptions.java bindings/java/src$(1)/main/com/apple/cie/foundationdb/TransactionOptions.java bindings/java/src$(1)/main/com/apple/cie/foundationdb/StreamingMode.java bindings/java/src$(1)/main/com/apple/cie/foundationdb/ConflictRangeType.java bindings/java/src$(1)/main/com/apple/cie/foundationdb/MutationType.java bindings/java/src$(1)/main/com/apple/cie/foundationdb/FDBException.java

  JAVA$(1)_SOURCES := $$(JAVA$(1)_GENERATED_SOURCES) bindings/java/src$(1)/main/com/apple/cie/foundationdb/*.java bindings/java/src$(1)/main/com/apple/cie/foundationdb/async/*.java bindings/java/src$(1)/main/com/apple/cie/foundationdb/tuple/*.java bindings/java/src$(1)/main/com/apple/cie/foundationdb/directory/*.java bindings/java/src$(1)/main/com/apple/cie/foundationdb/subspace/*.java bindings/java/src$(1)/test/com/apple/cie/foundationdb/test/*.java

  fdb_java$(1): bindings/java/foundationdb-client$(1).jar bindings/java/foundationdb-tests$(1).jar

  bindings/java/foundationdb-tests$(1).jar: bindings/java/.classstamp$(1)
	@echo "Building       $$@"
	@jar cf $$@ -C bindings/java/classes$(1)/test com/apple/cie/foundationdb

  bindings/java/foundationdb-client$(1).jar: bindings/java/.classstamp$(1) lib/libfdb_java.$(DLEXT)
	@echo "Building       $$@"
	@rm -rf bindings/java/classes$(1)/main/lib/$$(PLATFORM)/$$(java_ARCH)
	@mkdir -p bindings/java/classes$(1)/main/lib/$$(PLATFORM)/$$(java_ARCH)
	@cp lib/libfdb_java.$$(DLEXT) bindings/java/classes$(1)/main/lib/$$(PLATFORM)/$$(java_ARCH)/libfdb_java.$$(java_DLEXT)
	@jar cf $$@ -C bindings/java/classes$(1)/main com/apple/cie/foundationdb -C bindings/java/classes$(1)/main lib

  fdb_java$(1)_jar_clean:
	@rm -rf $$(JAVA$(1)_GENERATED_SOURCES)
	@rm -rf bindings/java/classes$(1)
	@rm -f bindings/java/foundationdb-client$(1).jar bindings/java/foundationdb-tests$(1).jar bindings/java/.classstamp$(1)

  # Redefinition of a target already defined in generated.mk, but it's "okay" and the way things were done before.
  fdb_java_clean: fdb_java$(1)_jar_clean

  bindings/java/src$(1)/main/com/apple/cie/foundationdb/StreamingMode.java: bin/vexillographer.exe fdbclient/vexillographer/fdb.options
	@echo "Building       Java options"
	@$$(MONO) bin/vexillographer.exe fdbclient/vexillographer/fdb.options java $$(@D)

  bindings/java/src$(1)/main/com/apple/cie/foundationdb/MutationType.java: bindings/java/src$(1)/main/com/apple/cie/foundationdb/StreamingMode.java
	@true

  bindings/java/src$(1)/main/com/apple/cie/foundationdb/ConflictRangeType.java: bindings/java/src$(1)/main/com/apple/cie/foundationdb/StreamingMode.java
	@true

  bindings/java/src$(1)/main/com/apple/cie/foundationdb/FDBException.java: bindings/java/src$(1)/main/com/apple/cie/foundationdb/StreamingMode.java
	@true

  bindings/java/src$(1)/main/com/apple/cie/foundationdb/%Options.java: bindings/java/src$(1)/main/com/apple/cie/foundationdb/StreamingMode.java
	@true

  bindings/java/src$(1)/main/overview.html: bindings/java/src$(1)/main/overview.html.in $$(ALL_MAKEFILES) versions.target
	@m4 -DVERSION=$$(VERSION) $$< > $$@

  bindings/java/.classstamp$(1): $$(JAVA$(1)_SOURCES)
	@echo "Compiling      Java$(1) source"
	@rm -rf bindings/java/classes$(1)
	@mkdir -p bindings/java/classes$(1)/main
	@mkdir -p bindings/java/classes$(1)/test
	@$$(JAVAC) $$(JAVA$(1)FLAGS) -d bindings/java/classes$(1)/main bindings/java/src$(1)/main/com/apple/cie/foundationdb/*.java bindings/java/src$(1)/main/com/apple/cie/foundationdb/async/*.java bindings/java/src$(1)/main/com/apple/cie/foundationdb/tuple/*.java bindings/java/src$(1)/main/com/apple/cie/foundationdb/directory/*.java bindings/java/src$(1)/main/com/apple/cie/foundationdb/subspace/*.java
	@$$(JAVAC) $$(JAVA$(1)FLAGS) -cp bindings/java/classes$(1)/main -d bindings/java/classes$(1)/test bindings/java/src$(1)/test/com/apple/cie/foundationdb/test/*.java
	@echo timestamp > bindings/java/.classstamp$(1)

  javadoc$(1): $$(JAVA$(1)_SOURCES) bindings/java/src$(1)/main/overview.html
	@echo "Generating     Javadocs"
	@mkdir -p $$(JAVADOC_DIR)/javadoc$(1)/
	@javadoc -quiet -public -notimestamp -source 1.8 -sourcepath bindings/java/src$(1)/main \
		-overview bindings/java/src$(1)/main/overview.html -d $$(JAVADOC_DIR)/javadoc$(1)/ \
		-windowtitle "FoundationDB Java Client API" \
		-doctitle "FoundationDB Java Client API" \
		-link "http://docs.oracle.com/javase/8/docs/api" \
		com.apple.cie.foundationdb com.apple.cie.foundationdb.async com.apple.cie.foundationdb.tuple com.apple.cie.foundationdb.directory com.apple.cie.foundationdb.subspace

  javadoc$(1)_clean:
	@rm -rf $$(JAVADOC_DIR)/javadoc$(1)
	@rm bindings/java/src$(1)/main/overview.html

  ifeq ($$(PLATFORM),linux)
	# We only need javadoc from one source
	TARGETS += javadoc$(1)
	CLEAN_TARGETS += javadoc$(1)_clean

	# _release builds the lib on OS X and the jars (including the OS X lib) on Linux
	TARGETS += fdb_java$(1)_release
	CLEAN_TARGETS += fdb_java$(1)_release_clean

    ifneq ($$(FATJAR),)
		packages/fdb-java$(1)-$$(JARVER).jar: $$(MAC_OBJ_JAVA) $$(WINDOWS_OBJ_JAVA)
    endif

    bindings/java/pom$(1).xml: bindings/java/pom.xml.in $$(ALL_MAKEFILES) versions.target
	  @echo "Generating     $$@"
	  @m4 -DVERSION=$$(JARVER) -DNAME=fdb-java$(1) $$< > $$@

    bindings/java/fdb-java$(1)-$(APPLEJARVER).pom: bindings/java/pom$(1).xml
	  @echo "Copying     $$@"
	  sed -e 's/-PRERELEASE/-SNAPSHOT/g' bindings/java/pom$(1).xml > "$$@"

    packages/fdb-java$(1)-$$(JARVER).jar: fdb_java$(1) versions.target
	  @echo "Building       $$@"
	  @rm -f $$@
	  @rm -rf packages/jar$(1)_regular
	  @mkdir -p packages/jar$(1)_regular
	  @cd packages/jar$(1)_regular && unzip -qq $$(TOPDIR)/bindings/java/foundationdb-client$(1).jar
      ifneq ($$(FATJAR),)
	    @mkdir -p packages/jar$(1)_regular/lib/windows/amd64
	    @mkdir -p packages/jar$(1)_regular/lib/osx/x86_64
	    @cp $$(MAC_OBJ_JAVA) packages/jar$(1)_regular/lib/osx/x86_64/libfdb_java.jnilib
	    @cp $$(WINDOWS_OBJ_JAVA) packages/jar$(1)_regular/lib/windows/amd64/fdb_java.dll
      endif
	  @cd packages/jar$(1)_regular && jar cf $$(TOPDIR)/$$@ *
	  @rm -r packages/jar$(1)_regular
	  @cd bindings && jar uf $$(TOPDIR)/$$@ ../LICENSE

    packages/fdb-java$(1)-$$(JARVER)-tests.jar: fdb_java$(1) versions.target
	  @echo "Building       $$@"
	  @rm -f $$@
	  @cp $$(TOPDIR)/bindings/java/foundationdb-tests$(1).jar packages/fdb-java$(1)-$$(JARVER)-tests.jar

    packages/fdb-java$(1)-$$(JARVER)-sources.jar: $$(JAVA$(1)_GENERATED_SOURCES) versions.target
	  @echo "Building       $$@"
	  @rm -f $$@
	  @jar cf $(TOPDIR)/$$@ -C bindings/java/src$(1)/main com/apple/cie/foundationdb

    packages/fdb-java$(1)-$$(JARVER)-javadoc.jar: javadoc$(1) versions.target
	  @echo "Building       $$@"
	  @rm -f $$@
	  @cd $$(JAVADOC_DIR)/javadoc$(1)/ && jar cf $$(TOPDIR)/$$@ *
	  @cd bindings && jar uf $$(TOPDIR)/$$@ ../LICENSE

    packages/fdb-java$(1)-$$(JARVER)-bundle.jar: packages/fdb-java$(1)-$$(JARVER).jar packages/fdb-java$(1)-$$(JARVER)-javadoc.jar packages/fdb-java$(1)-$$(JARVER)-sources.jar bindings/java/pom$(1).xml bindings/java/fdb-java$(1)-$$(APPLEJARVER).pom versions.target
	  @echo "Building       $$@"
	  @rm -f $$@
	  @rm -rf packages/bundle$(1)_regular
	  @mkdir -p packages/bundle$(1)_regular
	  @cp packages/fdb-java$(1)-$$(JARVER).jar packages/fdb-java$(1)-$$(JARVER)-javadoc.jar packages/fdb-java$(1)-$$(JARVER)-sources.jar bindings/java/fdb-java$(1)-$$(APPLEJARVER).pom packages/bundle$(1)_regular
	  @cp bindings/java/pom$(1).xml packages/bundle$(1)_regular/pom.xml
	  @cd packages/bundle$(1)_regular && jar cf $(TOPDIR)/$$@ *
	  @rm -rf packages/bundle$(1)_regular

    fdb_java$(1)_release: packages/fdb-java$(1)-$$(JARVER)-bundle.jar packages/fdb-java$(1)-$$(JARVER)-tests.jar

    fdb_java$(1)_release_clean:
	  @echo "Cleaning       Java release"
	  @rm -f packages/fdb-java$(1)-*.jar packages/fdb-java$(1)-*-sources.jar bindings/java/pom$(1).xml bindings/java/fdb-java$(1)-$$(APPLEJARVER).pom

  endif

endef

$(eval $(call add_java_binding_targets,))
ifeq ($(JAVAVERMAJOR).$(JAVAVERMINOR),1.8)
  $(eval $(call add_java_binding_targets,-completable))
endif

ifeq ($(PLATFORM),linux)

  fdb_java_CFLAGS += -I/usr/lib/jvm/java-8-openjdk-amd64/include -I/usr/lib/jvm/java-8-openjdk-amd64/include/linux
  fdb_java_LDFLAGS += -static-libgcc

  # Linux is where we build all the java packages
  packages: fdb_java_release fdb_java-completable_release
  packages_clean: fdb_java_release_clean fdb_java-completable_release_clean

  java_ARCH := amd64

  ifneq ($(FATJAR),)
	MAC_OBJ_JAVA := lib/libfdb_java.jnilib-$(VERSION_ID)
	WINDOWS_OBJ_JAVA := lib/fdb_java.dll-$(VERSION_ID)
  endif

else ifeq ($(PLATFORM),osx)
  TARGETS += fdb_java_release
  CLEAN_TARGETS += fdb_java_release_clean
  java_ARCH := x86_64

  fdb_java_release: lib/libfdb_java.$(DLEXT)
	@mkdir -p lib
	@rm -f lib/libfdb_java.$(java_DLEXT)-*
	@cp lib/libfdb_java.$(DLEXT) lib/libfdb_java.$(java_DLEXT)-$(VERSION_ID)
	@cp lib/libfdb_java.$(DLEXT)-debug lib/libfdb_java.$(java_DLEXT)-debug-$(VERSION_ID)

  fdb_java_release_clean:
	@rm -f lib/libfdb_java.$(DLEXT)-*
	@rm -f lib/libfdb_java.$(java_DLEXT)-*

  # FIXME: Surely there is a better way to grab the JNI headers on any version of OS X.
  fdb_java_CFLAGS += -I/System/Library/Frameworks/JavaVM.framework/Versions/A/Headers -I/Applications/Xcode.app/Contents/Developer/Platforms/MacOSX.platform/Developer/SDKs/MacOSX10.11.sdk/System/Library/Frameworks/JavaVM.framework/Versions/A/Headers

  # OS X needs to put its java lib in packages
  packages: fdb_java_lib_package

  fdb_java_lib_package: fdb_java_release
	mkdir -p packages
	cp lib/libfdb_java.$(java_DLEXT)-$(VERSION_ID) packages
	cp lib/libfdb_java.$(java_DLEXT)-debug-$(VERSION_ID) packages
endif
