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

TARGETS += fdb_ruby fdb_ruby_gem
CLEAN_TARGETS += fdb_ruby_clean fdb_ruby_gem_clean

ifeq ($(PLATFORM),linux)
  packages: fdb_ruby_gem

  packages_clean: fdb_ruby_gem_clean
endif

ifeq ($(PLATFORM),linux)
  GEM := gem
else ifeq ($(PLATFORM),osx)
  GEM := gem
else
  $(error Not prepared to build a gem on platform $(PLATFORM))
endif

ifeq ($(RELEASE),true)
  GEMVER = $(VERSION)
else
  GEMVER = $(VERSION)PRERELEASE
endif

fdb_ruby: bindings/ruby/lib/fdboptions.rb

bindings/ruby/lib/fdboptions.rb: bin/vexillographer.exe fdbclient/vexillographer/fdb.options
	@echo "Building       $@"
	@$(MONO) bin/vexillographer.exe fdbclient/vexillographer/fdb.options ruby $@

fdb_ruby_clean:
	@echo "Cleaning       fdb_ruby"
	@rm -f bindings/ruby/lib/fdboptions.rb

fdb_ruby_gem_clean:
	@echo "Cleaning       RubyGem"
	@rm -f packages/fdb-*.gem bindings/ruby/fdb.gemspec

bindings/ruby/fdb.gemspec: bindings/ruby/fdb.gemspec.in $(ALL_MAKEFILES) versions.target
	@m4 -DVERSION=$(GEMVER) $< > $@

fdb_ruby_gem: bindings/ruby/fdb.gemspec fdb_ruby
	@echo "Packaging      RubyGem"
	@mkdir -p packages
	@rm -f packages/fdb-*.gem
	@cp LICENSE bindings/ruby/LICENSE
	@(cd $(<D) && rm -f *.gem && $(GEM) build fdb.gemspec && mv *.gem ../../packages)
	@rm bindings/ruby/LICENSE
