/*
 * LatencyBandConfig.h
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

#ifndef FDBSERVER_LATENCYBANDCONFIG_H
#define FDBSERVER_LATENCYBANDCONFIG_H
#pragma once

#include "fdbclient/FDBTypes.h"
#include "fdbclient/JSONDoc.h"

struct LatencyBandConfig {
	struct RequestConfig {
		std::set<double> bands;

		friend bool operator==(RequestConfig const& lhs, RequestConfig const& rhs);
		friend bool operator!=(RequestConfig const& lhs, RequestConfig const& rhs);

		virtual void fromJson(JSONDoc json);

		template <class Ar>
		void serialize(Ar& ar) {
			serializer(ar, bands);
		}

	protected:
		virtual bool isEqual(RequestConfig const& r) const;
	};

	struct GrvConfig : RequestConfig {};

	struct ReadConfig : RequestConfig {
		Optional<int> maxReadBytes;
		Optional<int> maxKeySelectorOffset;

		void fromJson(JSONDoc json) override;

		template <class Ar>
		void serialize(Ar& ar) {
			serializer(ar, *(RequestConfig*)this, maxReadBytes, maxKeySelectorOffset);
		}

	protected:
		bool isEqual(RequestConfig const& r) const override;
	};

	struct CommitConfig : RequestConfig {
		Optional<int> maxCommitBytes;

		void fromJson(JSONDoc json) override;

		template <class Ar>
		void serialize(Ar& ar) {
			serializer(ar, *(RequestConfig*)this, maxCommitBytes);
		}

	protected:
		bool isEqual(RequestConfig const& r) const override;
	};

	GrvConfig grvConfig;
	ReadConfig readConfig;
	CommitConfig commitConfig;

	template <class Ar>
	void serialize(Ar& ar) {
		serializer(ar, grvConfig, readConfig, commitConfig);
	}

	static Optional<LatencyBandConfig> parse(ValueRef configurationString);

	bool operator==(LatencyBandConfig const& r) const;
	bool operator!=(LatencyBandConfig const& r) const;
};

#endif
