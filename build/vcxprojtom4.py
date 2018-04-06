#!/usr/bin/python
#
# vcxprojtom4.py
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

vcxproj = sys.argv[1]

from xml.dom.minidom import parse

try:
    dom = parse(vcxproj)
except:
    print "ERROR: Unable to open VCXProj file %s" % vcxproj
    sys.exit()

# We need to find out what kind of project/configuration we're going
# to build. FIXME: Right now we're hardcoded to look for the
# Release|X64 configuration/platform.

groups = dom.getElementsByTagName("PropertyGroup")
for group in groups:
    if group.getAttribute("Label").lower() == "configuration" and \
            group.getAttribute("Condition").lower() == "'$(configuration)|$(platform)'=='release|x64'":
        ctnodes = group.getElementsByTagName("ConfigurationType")
        configType = ctnodes[0].childNodes[0].data
        break

print "define(`GENCONFIGTYPE', `%s')dnl" % configType

if configType == "StaticLibrary":
    print "define(`GENTARGET', `lib/lib`'GENNAME.a')dnl"
    print "define(`GENOUTDIR', `lib')dnl"
elif configType == "DynamicLibrary":
    print "define(`GENTARGET', `lib/lib`'GENNAME.$(DLEXT)')dnl"
    print "define(`GENOUTDIR', `lib')dnl"
elif configType == "Application":
    print "define(`GENTARGET', `bin/'`GENNAME')dnl"
    print "define(`GENOUTDIR', `bin')dnl"
else:
    print "ERROR: Unable to determine configuration type"
    sys.exit()

sources = [node.getAttribute("Include").replace('\\', '/') for node in
           dom.getElementsByTagName("ActorCompiler") +
           dom.getElementsByTagName("ClCompile") +
           dom.getElementsByTagName("ClInclude")
           if not node.getElementsByTagName("ExcludedFromBuild") and node.hasAttribute("Include")]

print "define(`GENSOURCES', `%s')dnl" % ' '.join(sorted(sources))
