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

	private enum GeneratedTypes {
		ALL,
		LONG,
		FLOATING_POINT,
		STRING_LIKE
	}

	private final Random r;
	private final int ignoreIterations;
	private final int iterations;
	private final GeneratedTypes generatedTypes;

	public TuplePerformanceTest(Random r, int ignoreIterations, int iterations, GeneratedTypes generatedTypes) {
		this.r = r;
		this.ignoreIterations = ignoreIterations;
		this.iterations = iterations;
		this.generatedTypes = generatedTypes;
	}

	public Tuple createMultiTypeTuple(int length) {
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
					chars[j] = (char) ('a' + r.nextInt(26));
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
		return Tuple.fromList(values);
	}

	public Tuple createLongsTuple(int length) {
		List<Object> values = new ArrayList<>(length);
		for(int i = 0; i < length; i++) {
			int byteLength = r.nextInt(Long.BYTES + 1);
			long val = 0L;
			for(int x = 0; x < byteLength; x++) {
				int nextBytes = r.nextInt(256);
				val = (val << 8) + nextBytes;
			}
			values.add(val);
		}
		return Tuple.fromList(values);
	}

	public Tuple createFloatingPointTuple(int length) {
		List<Object> values = new ArrayList<>(length);
		for(int i = 0; i < length; i++) {
			double choice = r.nextDouble();
			if(choice < 0.40) {
				values.add(r.nextFloat());
			}
			else if(choice < 0.80) {
				values.add(r.nextDouble());
			}
			// These last two are more likely to produce NaN values
			else if(choice < 0.90) {
				values.add(Float.intBitsToFloat(r.nextInt()));
			}
			else {
				values.add(Double.longBitsToDouble(r.nextLong()));
			}
		}
		return Tuple.fromList(values);
	}

	public Tuple createStringLikeTuple(int length) {
		List<Object> values = new ArrayList<>(length);
		for(int i = 0; i < length; i++) {
			double choice = r.nextDouble();
			if(choice < 0.4) {
				byte[] arr = new byte[r.nextInt(20)];
				r.nextBytes(arr);
				values.add(arr);
			}
			else if(choice < 0.8) {
				// Random ASCII codepoints
				int[] codepoints = new int[r.nextInt(20)];
				for(int x = 0; x < codepoints.length; x++) {
					codepoints[x] = r.nextInt(0x7F);
				}
				values.add(new String(codepoints, 0, codepoints.length));
			}
			else if(choice < 0.9) {
				// All zeroes
				byte[] zeroes = new byte[r.nextInt(20)];
				values.add(zeroes);
			}
			else {
				// Random Unicode codepoints
				int[] codepoints = new int[r.nextInt(20)];
				for(int x = 0; x < codepoints.length; x++) {
					int codepoint = r.nextInt(0x10FFFF);
					while(Character.isSurrogate((char)codepoint)) {
						codepoint = r.nextInt(0x10FFFF);
					}
					codepoints[x] = codepoint;
				}
				values.add(new String(codepoints, 0, codepoints.length));
			}
		}
		return Tuple.fromList(values);
	}

	public Tuple createTuple(int length) {
		switch (generatedTypes) {
			case ALL:
				return createMultiTypeTuple(length);
			case LONG:
				return createLongsTuple(length);
			case FLOATING_POINT:
				return createFloatingPointTuple(length);
			case STRING_LIKE:
				return createStringLikeTuple(length);
			default:
				throw new IllegalStateException("unknown generated types " + generatedTypes);
		}
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
		long equalsArrayNanos = 0L;
		long sizeNanos = 0L;
		long hashNanos = 0L;
		long secondHashNanos = 0L;
		long subspacePackNanos = 0L;
		long subspaceUnpackNanos = 0L;
		long totalLength = 0L;
		long totalBytes = 0L;
		for(int i = 0; i < iterations; i++) {
			if(i % 100_000 == 0) {
				System.out.println("   iteration " + i);
			}
			int length = r.nextInt(20);
			Tuple t = createTuple(length);

			long startNanos = System.nanoTime();
			byte[] serialized = t.pack();
			long endNanos = System.nanoTime();
			packNanos += endNanos - startNanos;
			totalLength += t.size();
			totalBytes += t.getPackedSize();

			startNanos = System.nanoTime();
			Tuple t2 = Tuple.fromBytes(serialized);
			endNanos = System.nanoTime();
			unpackNanos += endNanos - startNanos;

			// Copy items over as if both are packed, their byte arrays are compared
			Tuple tCopy = Tuple.fromList(t.getItems());
			Tuple t2Copy = Tuple.fromList(t2.getItems());
			startNanos = System.nanoTime();
			if (!tCopy.equals(t2Copy)) {
				throw new RuntimeException("deserialized did not match serialized: " + t + " -- " + t2);
			}
			endNanos = System.nanoTime();
			equalsNanos += endNanos - startNanos;

			startNanos = System.nanoTime();
			if(!t.equals(t2)) {
				throw new RuntimeException("deserialized did not match serialized: " + t + " -- " + t2);
			}
			endNanos = System.nanoTime();
			equalsArrayNanos += endNanos - startNanos;

			tCopy = Tuple.fromList(t.getItems());
			startNanos = System.nanoTime();
			int size = tCopy.getPackedSize();
			endNanos = System.nanoTime();
			if (size != t.pack().length) {
				throw new RuntimeException("packed size did not match actual packed length: " + t + " -- " + " " + tCopy.getPackedSize() + " instead of " + t.getPackedSize());
			}
			sizeNanos += endNanos - startNanos;

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
			if (!Tuple.fromList(t.getItems()).equals(Tuple.fromList(t3.getItems())) || !t.equals(t3)) {
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
		System.out.printf("  Total elements:                        %d%n", totalLength);
		System.out.printf("  Total bytes:                           %d kB%n", totalBytes / 1000);
		System.out.printf("  Bytes per tuple:                       %f B%n", totalBytes * 1.0 / iterations);
		System.out.printf("  Pack time:                             %f s%n", packNanos * 1e-9);
		System.out.printf("  Pack time per tuple:                   %f \u03BCs%n", packNanos * 1e-3 / iterations);
		System.out.printf("  Pack time per kB:                      %f \u03BCs%n", packNanos * 1.0 / totalBytes);
		System.out.printf("  Serialization rate:                    %f objects / \u03BCs%n", totalLength * 1000.0 / packNanos);
		System.out.printf("  Unpack time:                           %f s%n", unpackNanos * 1e-9);
		System.out.printf("  Unpack time per tuple:                 %f \u03BCs%n", unpackNanos * 1e-3 / iterations);
		System.out.printf("  Equals time:                           %f s%n", equalsNanos * 1e-9);
		System.out.printf("  Equals time per tuple:                 %f \u03BCs%n", equalsNanos * 1e-3 / iterations);
		System.out.printf("  Equals time (using packed):            %f s%n", equalsArrayNanos * 1e-9);
		System.out.printf("  Equals time (using packed) per tuple:  %f \u03BCs%n", equalsArrayNanos * 1e-3 / iterations);
		System.out.printf("  Size time:                             %f s%n", sizeNanos * 1e-9);
		System.out.printf("  Size time per tuple:                   %f \u03BCs%n", sizeNanos * 1e-3 / iterations);
		System.out.printf("  Subspace pack time:                    %f s%n", subspacePackNanos * 1e-9);
		System.out.printf("  Subspace pack time per tuple:          %f \u03BCs%n", subspacePackNanos * 1e-3 / iterations);
		System.out.printf("  Subspace unpack time:                  %f s%n", subspaceUnpackNanos * 1e-9);
		System.out.printf("  Subspace unpack time per tuple:        %f \u03BCs%n", subspaceUnpackNanos * 1e-3 / iterations);
		System.out.printf("  Hash time:                             %f s%n", hashNanos * 1e-9);
		System.out.printf("  Hash time per tuple:                   %f \u03BCs%n", hashNanos * 1e-3 / iterations);
		System.out.printf("  Second hash time:                      %f s%n", secondHashNanos * 1e-9);
		System.out.printf("  Second hash time per tuple:            %f \u03BCs%n", secondHashNanos * 1e-3 / iterations);
	}

	public static void main(String[] args) {
		TuplePerformanceTest tester = new TuplePerformanceTest(new Random(), 100_000, 10_000_000, GeneratedTypes.ALL);
		tester.run();
	}
}
