/*
 * LoadPlugin.h
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

// Will need to be defined at link time
extern "C" void *get_plugin(const char *plugin_type_name_and_version);

template <class T>
Reference<T> loadPlugin( std::string const& plugin_name ) {
#ifdef TLS_DISABLED
	return Reference<T>( NULL );
#else
	return (plugin_name.empty()) ? Reference<T>( NULL ) : Reference<T>( (T*)get_plugin( T::get_plugin_type_name_and_version() ) );
#endif
}
