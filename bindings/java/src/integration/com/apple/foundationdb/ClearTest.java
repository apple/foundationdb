package com.apple.foundationdb;

import java.nio.charset.Charset;
import java.util.Iterator;
import java.util.Map;
import java.util.TreeMap;

import com.apple.foundationdb.tuple.ByteArrayUtil;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;

/**
 * Tests around the clear() operation
 */
@ExtendWith(RequiresDatabase.class)
public class ClearTest {

	@Test
	void testClearKeyWorks() throws Exception {
		try (Database db = FDB.selectAPIVersion(700).open()) {
			byte[] key = "testBytes".getBytes(Charset.forName("UTF-8"));
			byte[] value = "testValue".getBytes(Charset.forName("UTF-8"));
			db.run(tr -> {
				tr.set(key, value);

				return null;
			});

			// verify that the data in the database
			db.run(tr -> {
				byte[] actualVal = tr.get(key).join();
				Assertions.assertArrayEquals(value, actualVal, "Did not return key as expected");
				return null;
			});

			// now delete it, and make sure it's gone
			db.run(tr -> {
				tr.clear(key);
				return null;
			});

			db.run(tr -> {
				byte[] actualVal = tr.get(key).join();
				Assertions.assertNull(actualVal, "Returned key after delete!");
				return null;
			});
		}
	}

	@Test
	void testClearRangeWorks() throws Exception {
		try (Database db = FDB.selectAPIVersion(700).open()) {
			TreeMap<byte[], byte[]> data = new TreeMap<>(ByteArrayUtil.comparator());
			db.run(tr -> {
				for (int i = 0; i < 10; i++) {
					byte[] key = ("clearRangeGet" + i).getBytes(Charset.forName("UTF-8"));
					byte[] value = ("clearRangeGet" + i).getBytes(Charset.forName("UTF-8"));
					data.put(key, value);
					tr.set(key, value);
				}

				return null;
			});

			// verify that the data in the database
			db.run(tr -> {
				for (Map.Entry<byte[], byte[]> entry : data.entrySet()) {
					byte[] actualVal = tr.get(entry.getKey()).join();
					Assertions.assertArrayEquals(
					    entry.getValue(), actualVal,
					    "Did not return value as expected (key=" + ByteArrayUtil.printable(entry.getKey()) + ")");
				}
				return null;
			});

			// now delete the range, and make sure it's gone
			db.run(tr -> {
				tr.clear(new Range(data.firstKey(), ByteArrayUtil.strinc(data.lastKey())));
				return null;
			});

			db.run(tr -> {
				for (Map.Entry<byte[], byte[]> entry : data.entrySet()) {
					byte[] actualVal = tr.get(entry.getKey()).join();
					Assertions.assertNull(actualVal, " value was not deleted as expected (key=" +
					                                     ByteArrayUtil.printable(entry.getKey()) + ")");
				}
				return null;
			});
		}
	}

	@Test
	void testClearRangeWithRangeScanWorks() throws Exception {
		try (Database db = FDB.selectAPIVersion(700).open()) {
			TreeMap<byte[], byte[]> data = new TreeMap<>(ByteArrayUtil.comparator());
			db.run(tr -> {
				for (int i = 0; i < 10; i++) {
					byte[] key = ("clearRangeQuery" + i).getBytes(Charset.forName("UTF-8"));
					byte[] value = ("clearRangeQuery" + i).getBytes(Charset.forName("UTF-8"));
					data.put(key, value);
					tr.set(key, value);
				}

				return null;
			});

			final Range queryRange = new Range(data.firstKey(), ByteArrayUtil.strinc(data.lastKey()));
			// verify that the data in the database
			db.run(tr -> {
				Iterator<KeyValue> actualKvs = tr.getRange(queryRange).iterator();
				Iterator<Map.Entry<byte[], byte[]>> expectedKvs = data.entrySet().iterator();
				while (expectedKvs.hasNext()) {
					Assertions.assertTrue(actualKvs.hasNext(), "Iterator ended too early");
					KeyValue n = actualKvs.next();
					Map.Entry<byte[], byte[]> expectedN = expectedKvs.next();
					Assertions.assertArrayEquals(
					    expectedN.getKey(), n.getKey(),
					    "Keys returned out of order!(key=" + ByteArrayUtil.printable(expectedN.getKey()) + ")");
				}
				Assertions.assertFalse(actualKvs.hasNext(), "Iterator keeps going!");

				return null;
			});

			// now delete the range, and make sure it's gone
			db.run(tr -> {
				tr.clear(new Range(data.firstKey(), ByteArrayUtil.strinc(data.lastKey())));
				return null;
			});

			db.run(tr -> {
				Iterator<KeyValue> actualKvs = tr.getRange(queryRange).iterator();
				Assertions.assertFalse(actualKvs.hasNext(), "Data from range is still present!");
				return null;
			});
		}
	}

	@Test
	void testClearRangePrefixWithRangeScanWorks() throws Exception {
		try (Database db = FDB.selectAPIVersion(700).open()) {
			TreeMap<byte[], byte[]> data = new TreeMap<>(ByteArrayUtil.comparator());
			db.run(tr -> {
				for (int i = 0; i < 10; i++) {
					byte[] key = ("clearRangePrefix" + i).getBytes(Charset.forName("UTF-8"));
					byte[] value = ("clearRangePrefix" + i).getBytes(Charset.forName("UTF-8"));
					data.put(key, value);
					tr.set(key, value);
				}

				return null;
			});

			final Range queryRange = Range.startsWith("clearRangePrefix".getBytes(Charset.forName("UTF-8")));
			// verify that the data in the database
			db.run(tr -> {
				Iterator<KeyValue> actualKvs = tr.getRange(queryRange).iterator();
				Iterator<Map.Entry<byte[], byte[]>> expectedKvs = data.entrySet().iterator();
				while (expectedKvs.hasNext()) {
					Assertions.assertTrue(actualKvs.hasNext(), "Iterator ended too early");
					KeyValue n = actualKvs.next();
					Map.Entry<byte[], byte[]> expectedN = expectedKvs.next();
					Assertions.assertArrayEquals(
					    expectedN.getKey(), n.getKey(),
					    "Keys returned out of order!(key=" + ByteArrayUtil.printable(expectedN.getKey()) + ")");
				}
				Assertions.assertFalse(actualKvs.hasNext(), "Iterator keeps going!");

				return null;
			});

			// now delete the range, and make sure it's gone
			db.run(tr -> {
				tr.clear(new Range(data.firstKey(), ByteArrayUtil.strinc(data.lastKey())));
				return null;
			});

			db.run(tr -> {
				Iterator<KeyValue> actualKvs = tr.getRange(queryRange).iterator();
				Assertions.assertFalse(actualKvs.hasNext(), "Data from range is still present!");
				return null;
			});
		}
	}
}
