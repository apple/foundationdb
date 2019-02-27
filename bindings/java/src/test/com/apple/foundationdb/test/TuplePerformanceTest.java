package com.apple.foundationdb.test;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Random;
import java.util.UUID;

import com.apple.foundationdb.subspace.Subspace;
import com.apple.foundationdb.tuple.ByteArrayUtil;
import com.apple.foundationdb.tuple.Tuple;
import com.apple.foundationdb.tuple.Versionstamp;

public class TuplePerformanceTest {

	private final Random r;
	private final int ignoreIterations;
	private final int iterations;

	public TuplePerformanceTest(Random r, int ignoreIterations, int iterations) {
		this.r = r;
		this.ignoreIterations = ignoreIterations;
		this.iterations = iterations;
	}

	public Tuple createTuple(int length) {
		List<Object> values = new ArrayList<>(length);
		for(int i = 0; i < length; i++) {
			double choice = r.nextDouble();
			if(choice < 0.1) {
				values.add(null);
			}
			else if(choice < 0.2) {
				byte[] bytes = new byte[r.nextInt(20)];
				r.nextBytes(bytes);
				values.add(bytes);
			}
			else if(choice < 0.3) {
				char[] chars = new char[r.nextInt(20)];
				for (int j = 0; j < chars.length; j++) {
					chars[j] = (char)('a' + r.nextInt(26));
				}
				values.add(new String(chars));
			}
			else if(choice < 0.4) {
				values.add(r.nextInt(10_000_000) * (r.nextBoolean() ? -1 : 1));
			}
			else if(choice < 0.5) {
				values.add(r.nextFloat());
			}
			else if(choice < 0.6) {
				values.add(r.nextDouble());
			}
			else if(choice < 0.7) {
				values.add(r.nextBoolean());
			}
			else if(choice < 0.8) {
				values.add(UUID.randomUUID());
			}
			else if(choice < 0.9) {
				byte[] versionBytes = new byte[Versionstamp.LENGTH];
				r.nextBytes(versionBytes);
				Versionstamp vs = Versionstamp.fromBytes(versionBytes);
				values.add(vs);
			}
			else {
				int nestedLength = r.nextInt(length);
				Tuple nested = createTuple(nestedLength);
				values.add(nested);
			}
		}
		return Tuple.from(values);
	}

	public void run() {
		System.out.println("Warming up test...");
		for(int i = 0; i < ignoreIterations; i++) {
			int length = r.nextInt(20);
			createTuple(length).pack();
		}

		System.gc();

		System.out.println("Beginning to record values...");
		Subspace subspace = new Subspace(Tuple.from("test", "subspace"));
		long packNanos = 0L;
		long unpackNanos = 0L;
		long equalsNanos = 0L;
		long hashNanos = 0L;
		long secondHashNanos = 0L;
		long subspacePackNanos = 0L;
		long subspaceUnpackNanos = 0L;
		long totalLength = 0L;
		long totalBytes = 0L;
		for(int i = 0; i < iterations; i++) {
			int length = r.nextInt(20);
			Tuple t = createTuple(length);

			long startNanos = System.nanoTime();
			byte[] serialized = t.pack();
			long endNanos = System.nanoTime();
			packNanos += endNanos - startNanos;
			totalLength += length;
			totalBytes += serialized.length;

			startNanos = System.nanoTime();
			Tuple t2 = Tuple.fromBytes(serialized);
			endNanos = System.nanoTime();
			unpackNanos += endNanos - startNanos;

			startNanos = System.nanoTime();
			if(!t.equals(t2)) {
				throw new RuntimeException("deserialized did not match serialized: " + t + " -- " + t2);
			}
			endNanos = System.nanoTime();
			equalsNanos += endNanos - startNanos;

			startNanos = System.nanoTime();
			byte[] subspacePacked = subspace.pack(t);
			endNanos = System.nanoTime();
			if(!Arrays.equals(ByteArrayUtil.join(subspace.getKey(), serialized), subspacePacked)) {
				throw new RuntimeException("subspace pack not equal to concatenation of parts");
			}
			subspacePackNanos += endNanos - startNanos;

			startNanos = System.nanoTime();
			Tuple t3 = subspace.unpack(subspacePacked);
			endNanos = System.nanoTime();
			if(!t.equals(t3)) {
				throw new RuntimeException("does not unpack equally from subspace");
			}
			if(!Arrays.equals(t.pack(), t3.pack())) {
				throw new RuntimeException("does not pack equally from subspace");
			}
			subspaceUnpackNanos += endNanos - startNanos;

			startNanos = System.nanoTime();
			int hash = t.hashCode();
			endNanos = System.nanoTime();
			hashNanos += endNanos - startNanos;

			startNanos = System.nanoTime();
			int secondHash = t.hashCode();
			endNanos = System.nanoTime();
			if(hash != secondHash) {
				throw new RuntimeException("hash unstable");
			}
			secondHashNanos += endNanos - startNanos;
		}

		System.out.println("Test ended.");
		System.out.printf("  Total elements:                  %d%n", totalLength);
		System.out.printf("  Total bytes:                     %d kB%n", totalBytes / 1000);
		System.out.printf("  Bytes per tuple:                 %f B%n", totalBytes * 1.0 / iterations);
		System.out.printf("  Pack time:                       %f s%n", packNanos * 1e-9);
		System.out.printf("  Pack time per tuple:             %f \u03BCs%n", packNanos * 1e-3 / iterations);
		System.out.printf("  Pack time per kB:                %f \u03BCs%n", packNanos * 1.0 / totalBytes);
		System.out.printf("  Serialization rate:              %f objects / \u03BCs%n", totalLength * 1000.0 / packNanos);
		System.out.printf("  Unpack time:                     %f s%n", unpackNanos * 1e-9);
		System.out.printf("  Unpack time per tuple:           %f \u03BCs%n", unpackNanos * 1e-3 / iterations);
		System.out.printf("  Equals time:                     %f s%n", equalsNanos * 1e-9);
		System.out.printf("  Equals time per tuple:           %f \u03BCs%n", equalsNanos * 1e-3 / iterations);
		System.out.printf("  Subspace pack time:              %f s%n", subspacePackNanos * 1e-9);
		System.out.printf("  Subspace pack time per tuple:    %f \u03BCs%n", subspacePackNanos * 1e-3 / iterations);
		System.out.printf("  Subspace unpack time:            %f s%n", subspaceUnpackNanos * 1e-9);
		System.out.printf("  Subspace unpack time per tuple:  %f \u03BCs%n", subspaceUnpackNanos * 1e-3 / iterations);
		System.out.printf("  Hash time:                       %f s%n", hashNanos * 1e-9);
		System.out.printf("  Hash time per tuple:             %f \u03BCs%n", hashNanos * 1e-3 / iterations);
		System.out.printf("  Second hash time:                %f s%n", secondHashNanos * 1e-9);
		System.out.printf("  Second hash time per tuple:      %f \u03BCs%n", secondHashNanos * 1e-3 / iterations);
	}

	public static void main(String[] args) {
		TuplePerformanceTest tester = new TuplePerformanceTest(new Random(), 100_000, 10_000);
		tester.run();
	}
}
