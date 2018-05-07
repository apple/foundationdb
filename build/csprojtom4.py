#!/usr/bin/python
#
# csprojtom4.py
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


import sys

if len(sys.argv) != 2:
    print """Usage:
  %s [input]""" % sys.argv[0]
    sys.exit()

csproj = sys.argv[1]

from xml.dom.minidom import parse

try:
    dom = parse(csproj)
except:
    print "ERROR: Unable to open CSProj file %s" % csproj
    sys.exit()

outputType = dom.getElementsByTagName("OutputType")[0].childNodes[0].data
assemblyName = dom.getElementsByTagName("AssemblyName")[0].childNodes[0].data

if outputType == "Exe":
    print "define(`GENTARGET', `bin/%s.exe')dnl" % assemblyName
    print "define(`GENOUTPUTTYPE', `exe')dnl"
elif outputType == "Library":
    print "define(`GENTARGET', `bin/%s.dll')dnl" % assemblyName
    print "define(`GENOUTPUTTYPE', `library')dnl"
else:
    print "ERROR: Unable to determine output type"
    sys.exit()

sources = [node.getAttribute("Include").replace('\\', '/') for node in
           dom.getElementsByTagName("Compile")]
assemblies = [node.getAttribute("Include") for node in
              dom.getElementsByTagName("Reference")]

print "define(`GENSOURCES', `%s')dnl" % ' '.join(sources)
print "define(`GENREFERENCES', `%s')dnl" % ','.join(assemblies)
