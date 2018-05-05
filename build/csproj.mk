#
# csproj.mk
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

GENNAME()_REFERENCES=-r:GENREFERENCES
GENNAME()_SOURCES=$(addprefix GENDIR/,GENSOURCES)

-include GENDIR/local.mk

.PHONY: GENNAME()_clean GENNAME

GENNAME: GENTARGET

GENNAME()_clean:
	@echo "Cleaning       GENNAME"
	@rm -f GENTARGET

GENTARGET: $(GENNAME()_SOURCES) $(ALL_MAKEFILES)
	@echo "Building       $@"
	@mkdir -p $(@D)
	@$(MCS) $(GENNAME()_REFERENCES) $(GENNAME()_LOCAL_REFERENCES) $(GENNAME()_SOURCES) -target:GENOUTPUTTYPE -sdk:4 -out:$@
