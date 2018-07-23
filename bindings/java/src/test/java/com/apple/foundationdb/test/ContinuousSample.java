/*
 * ContinuousSample.java
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

package com.apple.foundationdb.test;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Random;

public class ContinuousSample<T extends Number & Comparable<T>> {
	public ContinuousSample(int sampleSize) {
		this.sampleSize = sampleSize;
		this.samples = new ArrayList<>(sampleSize);
		this.populationSize = 0;
		this.sorted = true;
	}

	public ContinuousSample<T> addSample(T sample) {
		if(populationSize == 0)
			min = max = sample;
		populationSize++;
		sorted = false;

		if(populationSize <= sampleSize) {
			samples.add(sample);
		} else if(random.nextDouble() < ((double)sampleSize / populationSize)) {
			samples.add(random.nextInt(sampleSize), sample);
		}

		max = sample.compareTo(max) > 0 ? sample : max;
		min = sample.compareTo(min) < 0 ? sample : min;
		return this;
	}

	public double mean() {
		if (samples.size() == 0) return 0;
		double sum = 0;
		for(int c = 0; c < samples.size(); c++) {
			sum += samples.get(c).doubleValue();
		}
		return sum / samples.size();
	}

	public T median() {
		return percentile(0.5);
	}

	public T percentile(double percentile) {
		if(samples.size() == 0 || percentile < 0.0 || percentile > 1.0)
			return null;
		sort();
		int idx = (int)Math.floor((samples.size() - 1) * percentile);
		return samples.get(idx);
	}

	public T min() {
		return min;
	}

	public T max() {
		return max;
	}

	@Override
	public String toString() {
		return String.format("Mean: %.2f, Median: %.2f, 90%%: %.2f, 98%%: %.2f",
				mean(), median(), percentile(0.90), percentile(0.98));
	}

	private Random random = new Random();
	private int sampleSize;
	private long populationSize;
	private boolean sorted;
	private List<T> samples;
	private T min, max;

	private void sort() {
		if(!sorted && samples.size() > 1)
			Collections.sort(samples);
		sorted = true;
	}
}
