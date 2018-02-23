/*
 * ActorCollection.actor.cpp
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

#include "actorcompiler.h"
#include "ActorCollection.h"
#include "IndexedSet.h"

ACTOR Future<Void> actorCollection( FutureStream<Future<Void>> addActor, int* pCount, double *lastChangeTime, double *idleTime, double *allTime, bool returnWhenEmptied )
{
	state int64_t nextTag = 0;
	state Map<int64_t, Future<Void>> tag_streamHelper;
	state PromiseStream<int64_t> complete;
	state PromiseStream<Error> errors;
	state int count = 0;
	if (!pCount) pCount = &count;

	loop choose {
		when (Future<Void> f = waitNext(addActor)) {
			int64_t t = nextTag++;
			tag_streamHelper[t] = streamHelper( complete, errors, tag(f, t) );
			++*pCount;
			if( *pCount == 1 && lastChangeTime && idleTime && allTime) {
				double currentTime = now();
				*idleTime += currentTime - *lastChangeTime;
				*allTime += currentTime - *lastChangeTime;
				*lastChangeTime = currentTime;
			}
		}
		when (int64_t t = waitNext(complete.getFuture())) {
			if (!--*pCount) {
				if( lastChangeTime && idleTime && allTime) {
					double currentTime = now();
					*allTime += currentTime - *lastChangeTime;
					*lastChangeTime = currentTime;
				}
				if (returnWhenEmptied)
					return Void();
			}
			tag_streamHelper.erase(t);
		}
		when (Error e = waitNext(errors.getFuture())) { throw e; }
	}
}
