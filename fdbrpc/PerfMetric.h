/*
 * PerfMetric.h
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

#ifndef FLOW_PERFMETRIC_H
#define FLOW_PERFMETRIC_H
#pragma once

#include <vector>
#include <string>
#include "flow/flow.h"

using std::vector;

struct PerfMetric {
	constexpr static FileIdentifier file_identifier = 5980618;
	PerfMetric() : m_name(""), m_value(0), m_averaged(false), m_format_code( "%.3g" ) {}
	PerfMetric( std::string name, double value, bool averaged ) : m_name(name), m_value(value), m_averaged(averaged), m_format_code( "%.3g" ) {}
	PerfMetric( std::string name, double value, bool averaged, std::string format_code ) : m_name(name), m_value(value), m_averaged(averaged), m_format_code(format_code) {}

	std::string name() const { return m_name; }
	double value() const { return m_value; }
	std::string formatted() const { return format(m_format_code.c_str(), m_value); }
	std::string format_code() const { return m_format_code; }
	bool averaged() const { return m_averaged; }

	PerfMetric withPrefix( const std::string& pre ) { return PerfMetric(pre+name(), value(), averaged(), format_code()); }

	template <class Ar>
	void serialize( Ar& ar ) {
		serializer(ar, m_name, m_format_code, m_value, m_averaged);
	}

private:
	std::string m_name, m_format_code;
	double m_value;
	bool m_averaged;
};

struct PerfIntCounter {
	PerfIntCounter(std::string name) : name(name), value(0) {}
	PerfIntCounter(std::string name, vector<PerfIntCounter*>& v) : name(name), value(0) { v.push_back(this); }
	void operator += (int64_t delta) { value += delta; }
	void operator ++ () { value += 1; }
	PerfMetric getMetric() { return PerfMetric( name, (double)value, false, "%.0lf" ); }
	int64_t getValue() { return value; }
	void clear() { value = 0; }

private:
	std::string name;
	int64_t value;
};

struct PerfDoubleCounter {
	PerfDoubleCounter(std::string name) : name(name), value(0) {}
	PerfDoubleCounter(std::string name, vector<PerfDoubleCounter*>& v) : name(name), value(0) { v.push_back(this); }
	void operator += (double delta) { value += delta; }
	void operator ++ () { value += 1.0; }
	PerfMetric getMetric() { return PerfMetric( name, value, false ); }
	double getValue() { return value; }
	void clear() { value = 0.0; }

private:
	std::string name;
	double value;
};

#endif
