/*
 * IClosable.h
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
#ifndef FDBSERVER_ICLOSABLE_H
#define FDBSERVER_ICLOSABLE_H
#pragma once
#include "flow/flow.h"
class IClosable {
public:
	// IClosable is a base interface for any disk-backed data structure that needs to support asynchronous errors,
	// shutdown and deletion

	virtual Future<Void> getError()
	    const = 0; // asynchronously throws an error if there is an internal error. Never set
	               // inside (on the stack of) a call to another API function on this object.
	virtual Future<Void> onClosed()
	    const = 0; // the future is set to Void when this is totally shut down after dispose() or
	               // close().  But this function cannot be called after dispose or close!
	virtual void dispose() = 0; // permanently delete the data AND invalidate this interface
	virtual void close() = 0; // invalidate this interface, but do not delete the data.  Outstanding operations may or
	                          // may not take effect in the background.
};

#endif