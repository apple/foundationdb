/*
 * FDBOptions.h
 *
 * This source file is part of the FoundationDB open source project
 *
 * Copyright 2013-2022 Apple Inc. and the FoundationDB project authors
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

#pragma once
#ifndef FDBCLIENT_FDBOPTIONS_H
#define FDBCLIENT_FDBOPTIONS_H

#include <string>
#include <list>
#include <map>

#include "flow/Arena.h"

struct FDBOptionInfo {
	std::string name;
	std::string comment;
	std::string parameterComment;

	bool hasParameter;
	bool hidden;
	bool persistent;

	// If non-negative, this specifies the code for the transaction option that this option is the default value for.
	// Options that have a defaultFor will only retain the value from time they were most recently set (i.e. there can
	// be no cumulative effects from calling multiple times).
	int defaultFor;

	enum class ParamType { None, String, Int, Bytes };

	ParamType paramType;

	FDBOptionInfo(std::string name,
	              std::string comment,
	              std::string parameterComment,
	              bool hasParameter,
	              bool hidden,
	              bool persistent,
	              int defaultFor,
	              ParamType paramType)
	  : name(name), comment(comment), parameterComment(parameterComment), hasParameter(hasParameter), hidden(hidden),
	    persistent(persistent), defaultFor(defaultFor), paramType(paramType) {}

	FDBOptionInfo() {}
};

template <class T>
class FDBOptionInfoMap {
private:
	std::map<typename T::Option, FDBOptionInfo> optionInfo;

public:
	typename std::map<typename T::Option, FDBOptionInfo>::const_iterator begin() const { return optionInfo.begin(); }
	typename std::map<typename T::Option, FDBOptionInfo>::const_iterator end() const { return optionInfo.end(); }
	typename std::map<typename T::Option, FDBOptionInfo>::const_iterator find(const typename T::Option& key) const {
		return optionInfo.find(key);
	}

	void insert(const typename T::Option& key, FDBOptionInfo info) { optionInfo[key] = info; }

	FDBOptionInfo const& getMustExist(const typename T::Option& key) const {
		auto itr = optionInfo.find(key);
		ASSERT(itr != optionInfo.end());
		return itr->second;
	}

	FDBOptionInfoMap() { T::init(); }
};

// An ordered list of options where each option is represented only once. Subsequent insertions will remove the option
// from its original location and add it to the end with the new value.
template <class T>
class UniqueOrderedOptionList {
public:
	typedef std::list<std::pair<typename T::Option, Optional<Standalone<StringRef>>>> OptionList;

private:
	OptionList options;
	std::map<typename T::Option, typename OptionList::iterator> optionsIndexMap;

public:
	void addOption(typename T::Option option, Optional<Standalone<StringRef>> value) {
		auto itr = optionsIndexMap.find(option);
		if (itr != optionsIndexMap.end()) {
			options.erase(itr->second);
		}
		options.emplace_back(option, value);
		optionsIndexMap[option] = --options.end();
	}

	typename OptionList::const_iterator begin() const { return options.cbegin(); }
	typename OptionList::const_iterator end() const { return options.cend(); }
};

#define ADD_OPTION_INFO(                                                                                               \
    type, var, name, comment, parameterComment, hasParameter, hidden, persistent, defaultFor, paramType)               \
	type::optionInfo.insert(                                                                                           \
	    var, FDBOptionInfo(name, comment, parameterComment, hasParameter, hidden, persistent, defaultFor, paramType));

#endif
