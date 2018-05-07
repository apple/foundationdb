/*
 * FDBOptions.h
 *
 * This source file is part of the FoundationDB open source project
 *
 * Copyright 2013-2018 Apple Inc. and the FoundationDB project authors
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
#include <map>

struct FDBOptionInfo {
	std::string name;
	std::string comment;
	std::string parameterComment;

	bool hasParameter;
	bool hidden;

	FDBOptionInfo(std::string name, std::string comment, std::string parameterComment, bool hasParameter, bool hidden) 
		: name(name), comment(comment), parameterComment(parameterComment), hasParameter(hasParameter), hidden(hidden) { }

	FDBOptionInfo() { }
};

template<class T>
class FDBOptionInfoMap {
private:
	std::map<typename T::Option, FDBOptionInfo> optionInfo;

public:
	typename std::map<typename T::Option, FDBOptionInfo>::iterator begin() { return optionInfo.begin(); }
	typename std::map<typename T::Option, FDBOptionInfo>::iterator end() { return optionInfo.end(); }

	FDBOptionInfo& operator[] (const typename T::Option& key) { return optionInfo[key]; }

	FDBOptionInfoMap() { T::init(); }
};

#define ADD_OPTION_INFO( type, var, name, comment, parameterComment, hasParameter, hidden ) type::optionInfo[var] = FDBOptionInfo(name, comment, parameterComment, hasParameter, hidden);

#endif