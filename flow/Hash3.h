/*
 * Hash3.h
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

#ifndef FLOW_HASH3_H
#define FLOW_HASH3_H
#pragma once

// Prototypes for Bob Jenkins' "lookup3" hash
// See Hash3.c for detailed documentation

extern "C" {

	uint32_t hashlittle( const void *key, size_t length, uint32_t initval);

	void hashlittle2( 
	  const void *key,       /* the key to hash */
	  size_t      length,    /* length of the key */
	  uint32_t   *pc,        /* IN: primary initval, OUT: primary hash */
	  uint32_t   *pb);       /* IN: secondary initval, OUT: secondary hash */

}

#endif