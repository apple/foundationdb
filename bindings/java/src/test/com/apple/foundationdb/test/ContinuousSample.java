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

import java.util.Arrays;
import java.util.Random;

public abstract class ContinuousSample<T extends Comparable<T>> {
	protected final Random random = new Random();
	protected final int sampleSize;
	protected boolean sorted;

	protected long populationSize;

	protected ContinuousSample(int sampleSize) {
		this.sampleSize = sampleSize;
		this.populationSize = 0;
		this.sorted = false;
	}

	public abstract ContinuousSample<T> addSample(T sample);

	public double mean() {
		if(populationSize ==0){
			return 0;
		}
		return sum()/populationSize;
	}

	public T median() {
		return percentile(0.5);
	}

	public T percentile(double percentile) {
		sort();
		int size = populationSize < sampleSize? (int)populationSize : sampleSize;
		int idx = (int)Math.floor((size - 1) * percentile);
		if(idx<0 ){
			return minValue();
		}
		return valueAt(idx);
	}

	public abstract double sum();

	protected abstract T valueAt(int idx);

	public abstract T minValue(); 

	public abstract T maxValue(); 

	protected abstract void sort();

	@Override
	public String toString() {
		return String.format("Mean: %.2f, Median: %.2f, 90%%: %.2f, 98%%: %.2f",
				mean(), median(), percentile(0.90), percentile(0.98));
	}


	public static class LongSample extends ContinuousSample<Long>{
		private final long[] samples;
		private long min,max;
		private long sum = 0L;

		protected LongSample(int sampleSize) {
			super(sampleSize);
			this.samples = new long[sampleSize];
		}

		@Override
		public double sum(){
			return sum;
		}

		public long min(){
			return min;
		}

		public long max(){
			return max;
		} 

		@Override
		public Long minValue(){
			return min();
		}

		@Override
		public Long maxValue(){
			return max();
		}

		@Override
		protected Long valueAt(int idx) {
			return samples[idx];
		}

		@Override
		public ContinuousSample<Long> addSample(Long sample){
			return add(sample);
		}

		protected void sort(){
			if(!sorted && populationSize>1){
				Arrays.sort(samples,0,(int)(populationSize> sampleSize? sampleSize : populationSize));
				sorted = true;
			}
		}

		public LongSample add(long sample){
			if(populationSize ==0){
				min = max = sample;
			}
			populationSize++;

			if(populationSize <= sampleSize){
				samples[(int)(populationSize-1)] = sample;
			}else if(random.nextDouble() < ((double)sampleSize/populationSize)){
				samples[random.nextInt(samples.length)] = sample;
			}

			if(sample > max){
				max = sample;
			}
			if(sample < min){
				min = sample;
			}

			sum+=sample;

			return this;
		}
	}

	public static class DoubleSample extends ContinuousSample<Double>{
		private final double[] samples;
		private double min,max;
		private double sum = 0L;

		protected DoubleSample(int sampleSize) {
			super(sampleSize);
			this.samples = new double[sampleSize];
		}

		@Override
		public double sum(){
			return sum;
		}

		public double min(){
			return min;
		}

		public double max(){
			return max;
		} 

		public double p(double percentile){
			sort();
			int pos = populationSize<sampleSize? (int)populationSize : sampleSize;
			int idx = (int)(percentile*pos);
			return samples[idx];
		}

		@Override
		public Double minValue(){
			return min();
		}

		@Override
		public Double maxValue(){
			return max();
		}

		@Override
		protected Double valueAt(int idx) {
			return samples[idx];
		}

		@Override
		public ContinuousSample<Double> addSample(Double sample){
			assert sample != null : "Cannot accept a null sample";
			return add(sample);
		}

		protected void sort(){
			if(!sorted && populationSize>1){
				Arrays.sort(samples,0,(int)(populationSize> sampleSize? sampleSize : populationSize));
				sorted = true;
			}
		}

		public DoubleSample add(double sample){
			if(populationSize ==0){
				min = max = sample;
			}
			populationSize++;

			if(populationSize <= sampleSize){
				samples[(int)populationSize-1] = sample;
			}else if(random.nextDouble() < ((double)sampleSize/populationSize)){
				samples[random.nextInt(samples.length)] = sample;
			}

			if(sample > max){
				max = sample;
			}
			if(sample < min){
				min = sample;
			}

			sum+=sample;

			return this;
		}
	}
}
