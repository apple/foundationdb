#
# scver.mk
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

#########################################################################
#
# This makefile will define the make variables related to source control
# variables, values, and settings
#
#
# Author:  Alvin Moore
# Created: 15-08-01
#########################################################################


# Retrieves the major version number from a version string
# Param:
#   1. String to parse in form 'major[.minor][.build]'.
MAJORVERFUNC = $(firstword $(subst ., ,$1))

# Retrieves the major version number from a version string
# If there is no minor part in the string, returns the second argument
# (if specified).
# Param:
#   1. String to parse in form 'major[.minor][.build]'.
#   2. (optional) Fallback value.
MINORVERFUNC = $(or $(word 2,$(subst ., ,$1)),$(value 2))

# Ensures that the specified directory is created
# Displays a creation message, if the directory does not exists
# Param:
#   1. Path to the directory to create
#   2. (optional) Display name of the directory.
CREATEDIRFUNC = if [ ! -d "$1" ]; then echo "`date +%F_%H-%M-%S` Creating $2 directory: $1"; mkdir -p "$1"; fi


# Make Environment Settings
#
ARCH := $(shell uname -m)
MAKEDIR := $(shell dirname $(realpath $(lastword $(MAKEFILE_LIST))))
FDBDIR := $(abspath $(MAKEDIR)/..)
FDBPARENTDIR := $(abspath $(FDBDIR)/..)
FDBDIRBASE := $(shell basename $(FDBDIR))
USERID := $(shell id -u)
USER := $(shell whoami)
PROCESSID := $(shell echo "$$$$")

ifeq ($(PLATFORM),osx)
  MD5SUM=md5
else
  MD5SUM=md5sum
endif


#
# Define the Java Variables
#

# Determine the Java compiler, if not defined
ifndef JAVAC
	JAVAC := $(shell which javac)
	ifeq ($(JAVAC),)
$(warning JAVA compiler is not installed on $(PLATFORM) $(ARCH))
	endif
endif

# Define the Java Flags based on Java version
ifdef JAVAC
	JAVAVER := $(shell bash -c 'javac -version 2>&1 | cut -d\  -f2-')
	JAVAVERMAJOR := $(call MAJORVERFUNC,$(JAVAVER))
	JAVAVERMINOR := $(call MINORVERFUNC,$(JAVAVER))
	ifneq ($(JAVAVERMAJOR),1)
$(warning Unable to compile source using Java version: $(JAVAVER) with compiler: $(JAVAC) on $(PLATFORM) $(ARCH))
	else
		JAVAFLAGS := -Xlint -source 1.8 -target 1.8
	endif
endif


# Determine active Version Control
#
GITPRESENT := $(wildcard $(FDBDIR)/.git)
HGPRESENT := $(wildcard $(FDBDIR)/.hg)

# Use Git, if not missing
ifneq ($(GITPRESENT),)
	SCVER := $(shell cd "$(FDBDIR)" && git --version 2>/dev/null)
	ifneq ($(SCVER),)
		VERSION_ID := $(shell cd "$(FDBDIR)" && git rev-parse --verify HEAD)
		SOURCE_CONTROL := GIT
		SCBRANCH := $(shell cd "$(FDBDIR)" && git rev-parse --abbrev-ref HEAD)
	else
$(error Missing git executable on $(PLATFORM) )
	endif
# Otherwise, use Mercurial
else
	# Otherwise, use Mercurial, if not missing
	ifneq ($(HGPRESENT),)
		SCVER := $(shell cd "$(FDBDIR)" && hg --version 2>/dev/null)
		ifdef SCVER
			VERSION_ID := $(shell cd "$(FDBDIR)" && hg id -n)
			SOURCE_CONTROL := MERCURIAL
			SCBRANCH := $(shell cd "$(FDBDIR)" && hg branch)
		else
$(error Missing hg executable on $(PLATFORM))
		endif
	else
	FDBFILES := (shell ls -la $(FDBDIR))
$(error Missing source control information for source on $(PLATFORM) in directory: $(FDBDIR) with files: $(FDBFILES))
	endif
endif

# Set the RELEASE variable based on the KVRELEASE variable.
ifeq ($(KVRELEASE),1)
  RELEASE := true
endif

# Define the Package Release and the File Version
ifeq ($(RELEASE),true)
  PKGRELEASE := 1
else ifeq ($(PRERELEASE),true)
  PKGRELEASE := 0.$(VERSION_ID).PRERELEASE
else
  PKGRELEASE := 0INTERNAL
endif


info:
	@echo "Displaying Make  Information"
	@echo "Version:        $(VERSION)"
	@echo "Package:        $(PACKAGE_NAME)"
	@echo "Version ID:     $(VERSION_ID)"
	@echo "Package ID:     $(PKGRELEASE)"
	@echo "SC Branch:      $(SCBRANCH)"
	@echo "Git Dir:        $(GITPRESENT)"
	@echo "Make Dir:       $(MAKEDIR)"
	@echo "Foundation Dir: $(FDBDIR)"
	@echo "Fdb Dir Base:   $(FDBDIRBASE)"
	@echo "User:           ($(USERID)) $(USER)"
	@echo "Java Version:   ($(JAVAVERMAJOR).$(JAVAVERMINOR)) $(JAVAVER)"
	@echo "Platform:       $(PLATFORM)"
	@echo ""
