/*
 * FdbOptions.h
 *
 * This source file is part of the FoundationDB open source project
 *
 * Copyright 2013-2025 Apple Inc. and the FoundationDB project authors
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

#ifndef FDBCLI_LIB_CLI_FDB_OPTIONS_H
#define FDBCLI_LIB_CLI_FDB_OPTIONS_H

#include "fdbclient/NativeAPI.actor.h"

namespace fdbcli_lib {
namespace utils {
class FdbOptions {
public:
	void setOption(Reference<ITransaction> tr,
	               StringRef optionStr,
	               bool enabled,
	               Optional<StringRef> arg,
	               bool intrans) {
		auto transactionItr = transactionOptions.legalOptions.find(optionStr.toString());
		if (transactionItr != transactionOptions.legalOptions.end())
			setTransactionOption(tr, transactionItr->second, enabled, arg, intrans);
		else {
			throw invalid_option();
		}
	}

	// Applies all enabled transaction options to the given transaction
	void apply(Reference<ITransaction> tr) {
		for (const auto& [name, value] : transactionOptions.options) {
			tr->setOption(name, value.castTo<StringRef>());
		}
	}

	// Returns true if any options have been set
	bool hasAnyOptionsEnabled() const { return !transactionOptions.options.empty(); }

	// Returns a vector of the names of all documented options
	std::vector<std::string> getValidOptions() const { return transactionOptions.getValidOptions(); }

private:
	// Sets a transaction option. If intrans == true, then this option is also applied to the passed in transaction.
	void setTransactionOption(Reference<ITransaction> tr,
	                          FDBTransactionOptions::Option option,
	                          bool enabled,
	                          Optional<StringRef> arg,
	                          bool intrans) {
		// If the parameter type is an int, we will extract into this variable and reference its memory with a StringRef
		int64_t parsedInt = 0;

		if (enabled) {
			auto optionInfo = FDBTransactionOptions::optionInfo.getMustExist(option);
			if (arg.present() != optionInfo.hasParameter) {
				throw invalid_option_value();
			}
			if (arg.present() && optionInfo.paramType == FDBOptionInfo::ParamType::Int) {
				try {
					size_t nextIdx;
					std::string value = arg.get().toString();
					parsedInt = std::stoll(value, &nextIdx);
					if (nextIdx != value.length()) {
						throw invalid_option_value();
					}
					arg = StringRef(reinterpret_cast<uint8_t*>(&parsedInt), 8);
				} catch (std::exception e) {
					throw invalid_option_value();
				}
			}
		}

		if (intrans) {
			tr->setOption(option, arg);
		}

		transactionOptions.setOption(option, enabled, arg.castTo<StringRef>());
	}

	// A group of enabled options (of type T::Option) as well as a legal options map from string to T::Option
	template <class T>
	struct OptionGroup {
		std::map<typename T::Option, Optional<Standalone<StringRef>>> options;
		std::map<std::string, typename T::Option> legalOptions;

		OptionGroup() {}
		OptionGroup(OptionGroup<T>& base)
		  : options(base.options.begin(), base.options.end()), legalOptions(base.legalOptions) {}

		// Enable or disable an option. Returns true if option value changed
		bool setOption(typename T::Option option, bool enabled, Optional<StringRef> arg) {
			auto optionItr = options.find(option);
			if (enabled && (optionItr == options.end() ||
			                Optional<Standalone<StringRef>>(optionItr->second).castTo<StringRef>() != arg)) {
				options[option] = arg.castTo<Standalone<StringRef>>();
				return true;
			} else if (!enabled && optionItr != options.end()) {
				options.erase(optionItr);
				return true;
			}

			return false;
		}

		// Returns true if the specified option is documented
		bool isDocumented(typename T::Option option) const {
			FDBOptionInfo info = T::optionInfo.getMustExist(option);

			std::string deprecatedStr = "Deprecated";
			return !info.comment.empty() && info.comment.substr(0, deprecatedStr.size()) != deprecatedStr;
		}

		// Returns a vector of the names of all documented options
		std::vector<std::string> getValidOptions() const {
			std::vector<std::string> ret;

			for (auto itr = legalOptions.begin(); itr != legalOptions.end(); ++itr)
				if (isDocumented(itr->second))
					ret.push_back(itr->first);

			return ret;
		}
	};

	OptionGroup<FDBTransactionOptions> transactionOptions;

public:
	FdbOptions() {
		for (auto itr = FDBTransactionOptions::optionInfo.begin(); itr != FDBTransactionOptions::optionInfo.end();
		     ++itr)
			transactionOptions.legalOptions[itr->second.name] = itr->first;
	}

	FdbOptions(FdbOptions& base) : transactionOptions(base.transactionOptions) {}
};
} // namespace utils
} // namespace fdbcli_lib

#endif // FDBCLI_LIB_CLI_COMMAND_SERVER_H
