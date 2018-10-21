export
PLATFORM := $(shell uname)
ARCH := $(shell uname -m)

TOPDIR := $(shell pwd)

ifeq ($(ARCH),x86_64)
  ARCH := x64
else
  $(error Not prepared to compile on $(ARCH))
endif

MONO := $(shell which mono)
ifeq ($(MONO),)
  MONO := /usr/bin/mono
endif

DMCS := $(shell which dmcs)
MCS := $(shell which mcs)
ifneq ($(DMCS),)
  MCS := $(DMCS)
endif
ifeq ($(MCS),)
  MCS := /usr/bin/dmcs
endif

CFLAGS := -Werror -Wno-error=format -fPIC -DNO_INTELLISENSE -fvisibility=hidden -DNDEBUG=1 -Wreturn-type -fno-omit-frame-pointer
ifeq ($(RELEASE),true)
	CFLAGS += -DFDB_CLEAN_BUILD
endif
ifeq ($(NIGHTLY),true)
	CFLAGS += -DFDB_CLEAN_BUILD
endif

ifeq ($(PLATFORM),Linux)
  PLATFORM := linux

  CC ?= gcc
  CXX ?= g++

  CXXFLAGS += -std=c++0x

  BOOSTDIR ?= /opt/boost_1_52_0
  TLS_LIBDIR ?= /usr/local/lib
  DLEXT := so
  java_DLEXT := so
  TARGET_LIBC_VERSION ?= 2.11
else ifeq ($(PLATFORM),Darwin)
  PLATFORM := osx

  CC := /usr/bin/clang
  CXX := /usr/bin/clang

  CFLAGS += -mmacosx-version-min=10.7 -stdlib=libc++
  CXXFLAGS += -mmacosx-version-min=10.7 -std=c++11 -stdlib=libc++ -msse4.2 -Wno-undefined-var-template -Wno-unknown-warning-option

  .LIBPATTERNS := lib%.dylib lib%.a

  BOOSTDIR ?= $(HOME)/boost_1_52_0
  TLS_LIBDIR ?= /usr/local/lib
  DLEXT := dylib
  java_DLEXT := jnilib
else
  $(error Not prepared to compile on platform $(PLATFORM))
endif

CCACHE := $(shell which ccache)
ifneq ($(CCACHE),)
  CCACHE_CC := $(CCACHE) $(CC)
  CCACHE_CXX := $(CCACHE) $(CXX)
else
  CCACHE_CC := $(CC)
  CCACHE_CXX := $(CXX)
endif

ACTORCOMPILER := bin/actorcompiler.exe

# UNSTRIPPED := 1

# Normal optimization level
CFLAGS += -O2

# Or turn off optimization entirely
# CFLAGS += -O0

# Debugging symbols are a good thing (and harmless, since we keep them
# in external debug files)
CFLAGS += -g

# valgrind-compatibile builds are enabled by uncommenting lines in valgind.mk

# Define the TLS compilation and link variables
ifdef TLS_DISABLED
CFLAGS += -DTLS_DISABLED
FDB_TLS_LIB :=
TLS_LIBS :=
else
FDB_TLS_LIB := lib/libFDBLibTLS.a
TLS_LIBS += $(addprefix $(TLS_LIBDIR)/,libtls.a libssl.a libcrypto.a)
endif

CXXFLAGS += -Wno-deprecated
LDFLAGS :=
LIBS :=
STATIC_LIBS :=

# Add library search paths (that aren't -Llib) to the VPATH
VPATH += $(addprefix :,$(filter-out lib,$(patsubst -L%,%,$(filter -L%,$(LDFLAGS)))))

CS_PROJECTS := flow/actorcompiler flow/coveragetool fdbclient/vexillographer
CPP_PROJECTS := flow fdbrpc fdbclient fdbbackup fdbserver fdbcli bindings/c bindings/java fdbmonitor bindings/flow/tester bindings/flow FDBLibTLS
OTHER_PROJECTS := bindings/python bindings/ruby bindings/go

CS_MK_GENERATED := $(CS_PROJECTS:=/generated.mk)
CPP_MK_GENERATED := $(CPP_PROJECTS:=/generated.mk)

MK_GENERATED := $(CS_MK_GENERATED) $(CPP_MK_GENERATED)

# build/valgrind.mk needs to be included before any _MK_GENERATED (which in turn includes local.mk)
MK_INCLUDE := build/scver.mk build/valgrind.mk $(CS_MK_GENERATED) $(CPP_MK_GENERATED) $(OTHER_PROJECTS:=/include.mk) build/packages.mk

ALL_MAKEFILES := Makefile $(MK_INCLUDE) $(patsubst %/generated.mk,%/local.mk,$(MK_GENERATED))

TARGETS =

.PHONY: clean all Makefiles

default: fdbserver fdbbackup fdbcli fdb_c fdb_python fdb_python_sdist

all: $(CS_PROJECTS) $(CPP_PROJECTS) $(OTHER_PROJECTS)

# These are always defined and ready to use. Any target that uses them and needs them up to date
#  should depend on versions.target
VERSION := $(shell cat versions.target | grep '<Version>' | sed -e 's,^[^>]*>,,' -e 's,<.*,,')
PACKAGE_NAME := $(shell cat versions.target | grep '<PackageName>' | sed -e 's,^[^>]*>,,' -e 's,<.*,,')

versions.h: Makefile versions.target
	@rm -f $@
ifeq ($(RELEASE),true)
	@echo "#define FDB_VT_VERSION \"$(VERSION)\"" >> $@
else
	@echo "#define FDB_VT_VERSION \"$(VERSION)-PRERELEASE\"" >> $@
endif
	@echo "#define FDB_VT_PACKAGE_NAME \"$(PACKAGE_NAME)\"" >> $@

bindings: fdb_c fdb_python fdb_ruby fdb_java fdb_flow fdb_flow_tester fdb_go fdb_go_tester fdb_c_tests

Makefiles: $(MK_GENERATED)

$(CS_MK_GENERATED): build/csprojtom4.py build/csproj.mk Makefile
	@echo "Creating       $@"
	@python build/csprojtom4.py $(@D)/*.csproj | m4 -DGENDIR="$(@D)" -DGENNAME=`basename $(@D)/*.csproj .csproj` - build/csproj.mk > $(@D)/generated.mk

$(CPP_MK_GENERATED): build/vcxprojtom4.py build/vcxproj.mk Makefile
	@echo "Creating       $@"
	@python build/vcxprojtom4.py $(@D)/*.vcxproj | m4 -DGENDIR="$(@D)" -DGENNAME=`basename $(@D)/*.vcxproj .vcxproj` - build/vcxproj.mk > $(@D)/generated.mk

DEPSDIR := .deps
OBJDIR := .objs

include $(MK_INCLUDE)

clean: $(CLEAN_TARGETS) docpreview_clean
	@echo "Cleaning       toplevel"
	@rm -rf $(OBJDIR)
	@rm -rf $(DEPSDIR)
	@rm -rf lib/
	@rm -rf bin/coverage.*.xml
	@find . -name "*.g.cpp" -exec rm -f {} \; -or -name "*.g.h" -exec rm -f {} \;

targets:
	@echo "Available targets:"
	@for i in $(sort $(TARGETS)); do echo "  $$i" ; done
	@echo "Append _clean to clean specific target."

lib/libstdc++.a: $(shell $(CC) -print-file-name=libstdc++_pic.a)
	@echo "Frobnicating   $@"
	@mkdir -p lib
	@rm -rf .libstdc++
	@mkdir .libstdc++
	@(cd .libstdc++ && ar x $<)
	@for i in .libstdc++/*.o ; do \
		nm $$i | grep -q \@ || continue ; \
		nm $$i | awk '$$3 ~ /@@/ { COPY = $$3; sub(/@@.*/, "", COPY); print $$3, COPY; }' > .libstdc++/replacements ; \
		objcopy --redefine-syms=.libstdc++/replacements $$i $$i.new && mv $$i.new $$i ; \
		rm .libstdc++/replacements ; \
		nm $$i | awk '$$3 ~ /@/ { print $$3; }' > .libstdc++/deletes ; \
		objcopy --strip-symbols=.libstdc++/deletes $$i $$i.new && mv $$i.new $$i ; \
		rm .libstdc++/deletes ; \
	done
	@ar rcs $@ .libstdc++/*.o
	@rm -r .libstdc++

docpreview: javadoc
	@echo "Generating     docpreview"
	@TARGETS= $(MAKE) -C documentation docpreview

docpreview_clean:
	@echo "Cleaning       docpreview"
	@CLEAN_TARGETS= $(MAKE) -C documentation -s --no-print-directory docpreview_clean

packages/foundationdb-docs-$(VERSION).tar.gz: FORCE javadoc
	@echo "Packaging      documentation"
	@TARGETS= $(MAKE) -C documentation docpackage
	@mkdir -p packages
	@rm -f packages/foundationdb-docs-$(VERSION).tar.gz
	@cp documentation/sphinx/.dist/foundationdb-docs-$(VERSION).tar.gz packages/foundationdb-docs-$(VERSION).tar.gz

docpackage: packages/foundationdb-docs-$(VERSION).tar.gz

FORCE:

.SECONDEXPANSION:

bin/coverage.%.xml: bin/coveragetool.exe $$(%_ALL_SOURCES)
	@echo "Creating       $@"
	@$(MONO) bin/coveragetool.exe $@ $(filter-out $<,$^) >/dev/null

$(CPP_MK_GENERATED): $$(@D)/*.vcxproj

$(CS_MK_GENERATED): $$(@D)/*.csproj
