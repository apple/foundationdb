#
# vcxproj.mk
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

TARGETS += GENNAME
CLEAN_TARGETS += GENNAME()_clean

GENNAME()_ALL_SOURCES := $(addprefix GENDIR/,GENSOURCES)

GENNAME()_BUILD_SOURCES := $(patsubst %.actor.cpp,%.actor.g.cpp,$(filter-out %.h %.hpp,$(GENNAME()_ALL_SOURCES)))
GENNAME()_GENERATED_SOURCES := $(patsubst %.actor.h,%.actor.g.h,$(patsubst %.actor.cpp,%.actor.g.cpp,$(filter %.actor.h %.actor.cpp,$(GENNAME()_ALL_SOURCES))))
GENERATED_SOURCES += $(GENNAME()_GENERATED_SOURCES)

-include GENDIR/local.mk

# If we have any static libs, we have to wrap them in the appropriate
# compiler flag magic
ifeq ($(GENNAME()_STATIC_LIBS),)
  GENNAME()_STATIC_LIBS_REAL :=
else
  GENNAME()_STATIC_LIBS_REAL := -Wl,-Bstatic $(GENNAME()_STATIC_LIBS) -Wl,-Bdynamic
endif

# If we have any -L directives in our LDFLAGS, we need to add those
# paths to the VPATH
VPATH += $(addprefix :,$(patsubst -L%,%,$(filter -L%,$(GENNAME()_LDFLAGS))))

IGNORE := $(shell echo $(VPATH))

GENNAME()_OBJECTS := $(addprefix $(OBJDIR)/,$(GENNAME()_BUILD_SOURCES:=.o))
GENNAME()_DEPS := $(addprefix $(DEPSDIR)/,$(GENNAME()_BUILD_SOURCES:=.d))

.PHONY: GENNAME()_clean GENNAME

GENNAME: GENTARGET

-include $(GENNAME()_DEPS)

GENDIR/%.actor.g.cpp: GENDIR/%.actor.cpp $(ACTORCOMPILER)
	@echo "Actorcompiling $<"
	@$(MONO) $(ACTORCOMPILER) $< $@ >/dev/null

GENDIR/%.actor.g.h: GENDIR/%.actor.h $(ACTORCOMPILER)
	@if [ -e $< ]; then echo "Actorcompiling $<" ; $(MONO) $(ACTORCOMPILER) $< $@ >/dev/null ; fi
.PRECIOUS: GENDIR/%.actor.g.cpp GENDIR/%.actor.g.h

# The order-only dependency on the generated .h files is to force make
# to actor compile all headers before attempting compilation of any .c
# or .cpp files. We have no mechanism to detect dependencies on
# generated headers before compilation.

$(OBJDIR)/GENDIR/%.cpp.o: GENDIR/%.cpp $(ALL_MAKEFILES) | $(filter %.h,$(GENERATED_SOURCES))
	@echo "Compiling      $<"
ifeq ($(VERBOSE),1)
	@echo "$(CCACHE_CXX) $(CFLAGS) $(CXXFLAGS) $(GENNAME()_CFLAGS) $(GENNAME()_CXXFLAGS) -MMD -MT $@ -MF $(DEPSDIR)/$<.d.tmp -c $< -o $@"
endif
	@mkdir -p $(DEPSDIR)/$(<D) && \
	mkdir -p $(OBJDIR)/$(<D) && \
	$(CCACHE_CXX) $(CFLAGS) $(CXXFLAGS) $(GENNAME()_CFLAGS) $(GENNAME()_CXXFLAGS) -MMD -MT $@ -MF $(DEPSDIR)/$<.d.tmp -c $< -o $@ && \
	cp $(DEPSDIR)/$<.d.tmp $(DEPSDIR)/$<.d && \
	sed -e 's/#.*//' -e 's/^[^:]*: *//' -e 's/ *\\$$//' -e '/^$$/ d' -e 's/$$/ :/' < $(DEPSDIR)/$<.d.tmp >> $(DEPSDIR)/$<.d && \
	rm $(DEPSDIR)/$<.d.tmp

$(OBJDIR)/GENDIR/%.c.o: GENDIR/%.c $(ALL_MAKEFILES) | $(filter %.h,$(GENERATED_SOURCES))
	@echo "Compiling      $<"
ifeq ($(VERBOSE),1)
	@echo "$(CCACHE_CC) $(CFLAGS) $(GENNAME()_CFLAGS) -MMD -MT $@ -MF $(DEPSDIR)/$<.d.tmp -c $< -o $@"
endif
	@mkdir -p $(DEPSDIR)/$(<D) && \
	mkdir -p $(OBJDIR)/$(<D) && \
	$(CCACHE_CC) $(CFLAGS) $(GENNAME()_CFLAGS) -MMD -MT $@ -MF $(DEPSDIR)/$<.d.tmp -c $< -o $@ && \
	cp $(DEPSDIR)/$<.d.tmp $(DEPSDIR)/$<.d && \
	sed -e 's/#.*//' -e 's/^[^:]*: *//' -e 's/ *\\$$//' -e '/^$$/ d' -e 's/$$/ :/' < $(DEPSDIR)/$<.d.tmp >> $(DEPSDIR)/$<.d && \
	rm $(DEPSDIR)/$<.d.tmp

$(OBJDIR)/GENDIR/%.S.o: GENDIR/%.S $(ALL_MAKEFILES) | $(filter %.h,$(GENERATED_SOURCES))
	@echo "Assembling     $<"
ifeq ($(VERBOSE),1)
	@echo "$(CCACHE_CC) $(CFLAGS) $(GENNAME()_CFLAGS) -MMD -MT $@ -MF $(DEPSDIR)/$<.d.tmp -c $< -o $@"
endif
	@mkdir -p $(DEPSDIR)/$(<D) && \
	mkdir -p $(OBJDIR)/$(<D) && \
	$(CCACHE_CC) $(CFLAGS) $(GENNAME()_CFLAGS) -MMD -MT $@ -MF $(DEPSDIR)/$<.d.tmp -c $< -o $@ && \
	cp $(DEPSDIR)/$<.d.tmp $(DEPSDIR)/$<.d && \
	sed -e 's/#.*//' -e 's/^[^:]*: *//' -e 's/ *\\$$//' -e '/^$$/ d' -e 's/$$/ :/' < $(DEPSDIR)/$<.d.tmp >> $(DEPSDIR)/$<.d && \
	rm $(DEPSDIR)/$<.d.tmp

GENNAME()_clean:
	@echo "Cleaning       GENNAME"
	@rm -f GENTARGET $(GENNAME()_GENERATED_SOURCES) GENTARGET().debug GENTARGET()-debug
	@rm -rf $(DEPSDIR)/GENDIR
	@rm -rf $(OBJDIR)/GENDIR

GENTARGET: $(GENNAME()_OBJECTS) $(GENNAME()_LIBS) $(GENNAME()_STATIC_LIBS) $(ALL_MAKEFILES) build/link-wrapper.sh build/link-validate.sh
	@mkdir -p GENOUTDIR
	@./build/link-wrapper.sh GENCONFIGTYPE GENNAME $@ $(TARGET_LIBC_VERSION)
