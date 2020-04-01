/*
 * FBTrace.h
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

#include "flow/FastRef.h"
#include "flow/ObjectSerializer.h"
#include <type_traits>

class FBTraceImpl : public ReferenceCounted<FBTraceImpl> {
protected:
	virtual void write(ObjectWriter& writer) = 0;
public:
	virtual ~FBTraceImpl();
};

template <class T>
class FDBTrace : FBTraceImpl {
protected:
	void write(ObjectWriter& writer) override {
		writer.serialize(*static_cast<T*>(this));
	}
};

void fbTrace(Reference<FBTraceImpl> const& traceLine);

template <class T>
void fbTrace(Reference<T> const& traceLine) {
	static_assert(std::is_base_of<FBTraceImpl, T>::value, "fbTrace only accepts FBTraceImpl as argument");
	fbTrace(traceLine.template castTo<FBTraceImpl>());
}
