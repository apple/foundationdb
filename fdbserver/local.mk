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

fdbserver_CFLAGS := $(fdbclient_CFLAGS)
fdbserver_LDFLAGS := $(fdbrpc_LDFLAGS)
fdbserver_LIBS := lib/libfdbclient.a lib/libfdbrpc.a lib/libflow.a

ifeq ($(PLATFORM),linux)
  fdbserver_LIBS += -ldl -lpthread -lrt
  fdbserver_LDFLAGS += -static-libstdc++ -static-libgcc

  # GPerfTools profiler (uncomment to use)
  # fdbserver_CFLAGS += -I/opt/gperftools/include -DUSE_GPERFTOOLS=1
  # fdbserver_LDFLAGS += -L/opt/gperftools/lib
  # fdbserver_STATIC_LIBS += -ltcmalloc -lunwind -lprofiler
else ifeq ($(PLATFORM),osx)
  fdbserver_LDFLAGS += -lc++
endif

ifeq ($(WORKLOADS),false)
  fdbserver_ALL_SOURCES := $(filter-out fdbserver/workloads/%,$(fdbserver_ALL_SOURCES))
  fdbserver_BUILD_SOURCES := $(filter-out fdbserver/workloads/%,$(fdbserver_BUILD_SOURCES))
endif

bin/fdbserver: bin/coverage.fdbserver.xml

bin/fdbserver.debug: bin/fdbserver

FORCE:

createtemplatedb: bin/fdbserver
	bin/fdbserver -r createtemplatedb
	python -c 'import textwrap; s=open("template.fdb", "rb").read().encode("hex").upper(); t="".join(["\\x"+x+y for (x,y) in zip(s[0::2], s[1::2])]) ; open("fdbserver/template_fdb.h","wb").write("static const char template_fdb[] = \\\n\t\"%s\";"%"\" \\\n\t\"".join(textwrap.wrap(t,80)))'
