#include "fdbrpc/DDSketch.h"
#include "flow/Error.h"
#include "flow/IRandom.h"
#include "flow/UnitTest.h"
#include <limits>
#include <random>
#include "flow/actorcompiler.h" // has to be last include
void forceLinkDDSketchTests() {}

TEST_CASE("/fdbrpc/ddsketch/accuracy") {

	int TRY = 100, SIZE = 1e6;
	const int totalPercentiles = 7;
	double targetPercentiles[totalPercentiles] = { .0001, .01, .1, .50, .90, .99, .9999 };
	double stat[totalPercentiles] = { 0 };
	for (int t = 0; t < TRY; t++) {
		DDSketch<double> dd;
		std::vector<double> nums;
		for (int i = 0; i < SIZE; i++) {
			static double a = 1, b = 1; // a skewed distribution
			auto y = deterministicRandom()->random01();
			auto num = b / pow(1 - y, 1 / a);
			nums.push_back(num);
			dd.addSample(num);
		}
		std::sort(nums.begin(), nums.end());
		for (int percentID = 0; percentID < totalPercentiles; percentID++) {
			double percentile = targetPercentiles[percentID];
			double ground = nums[percentile * (SIZE - 1)], ddvalue = dd.percentile(percentile);
			double relativeError = fabs(ground - ddvalue) / ground;
			stat[percentID] += relativeError;
		}
	}

	for (int percentID = 0; percentID < totalPercentiles; percentID++) {
		printf("%.4lf per, relative error %.4lf\n", targetPercentiles[percentID], stat[percentID] / TRY);
	}

	return Void();
}

TEST_CASE("/fdbrpc/ddsketch/correctness") {
	DDSketch<double> dd;

	for (int i = 0; i < 4000; i++) {
		// This generates a uniform real disitribution between the range of
		// [0.0004, 0.01]
		double sample = (static_cast<double>(deterministicRandom()->randomSkewedUInt32(40, 1000)) / 100000);
		dd.addSample(sample);
	}
	double p50 = dd.percentile(0.5);
	ASSERT(p50 > 0 && p50 != std::numeric_limits<double>::infinity());
	double p90 = dd.percentile(0.9);
	ASSERT(p90 > 0 && p90 != std::numeric_limits<double>::infinity());
	double p95 = dd.percentile(0.95);
	ASSERT(p95 > 0 && p95 != std::numeric_limits<double>::infinity());
	double p99 = dd.percentile(0.99);
	ASSERT(p99 > 0 && p99 != std::numeric_limits<double>::infinity());
	double p999 = dd.percentile(0.999);
	ASSERT(p999 > 0 && p999 != std::numeric_limits<double>::infinity());
	return Void{};
}
