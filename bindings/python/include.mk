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

TARGETS += fdb_python
CLEAN_TARGETS += fdb_python_clean

ifeq ($(RELEASE),true)
  PYVER = $(VERSION)
else
  PYVER = $(VERSION)a1
endif

fdb_python: bindings/python/fdb/fdboptions.py bindings/python/setup.py fdb_python_check

bindings/python/fdb/fdboptions.py: bin/vexillographer.exe fdbclient/vexillographer/fdb.options
	@echo "Building       $@"
	@$(MONO) bin/vexillographer.exe fdbclient/vexillographer/fdb.options python $@

fdb_python_clean:
	@echo "Cleaning       fdb_python"
	@rm -f bindings/python/fdb/fdboptions.py bindings/python/setup.py

bindings/python/setup.py: bindings/python/setup.py.in $(ALL_MAKEFILES) versions.target
	@echo "Generating     $@"
	@m4 -DVERSION=$(PYVER) $< > $@

fdb_python_check: bindings/python/setup.py bindings/python/fdb/*.py bindings/python/tests/*.py
	@echo "Checking       fdb_python"
	@bash -c "if which pycodestyle &> /dev/null ; then pycodestyle bindings/python --config=bindings/python/setup.cfg ; else echo \"Skipped Python style check! Missing: pycodestyle\"; fi"

fdb_python_sdist: fdb_python
	@mkdir -p packages
	@rm -rf bindings/python/dist
	@cp LICENSE bindings/python/LICENSE
	@cd bindings/python && python setup.py sdist
	@rm bindings/python/LICENSE
	@cp bindings/python/dist/*.tar.gz packages/

fdb_python_sdist_upload: fdb_python
	@mkdir -p packages
	@rm -rf bindings/python/dist
	@cp LICENSE bindings/python/LICENSE
	@cd bindings/python && python setup.py sdist upload -r apple-pypi
	@rm bindings/python/LICENSE
	@cp bindings/python/dist/*.tar.gz packages/

fdb_python_sdist_clean:
	@echo "Cleaning       fdb_python_sdist"
	@rm -rf bindings/python/dist
	@rm -f bindings/python/MANIFEST bindings/python/setup.py
	@rm -f packages/foundationdb-*.tar.gz

packages: fdb_python_sdist

packages_clean: fdb_python_sdist_clean
