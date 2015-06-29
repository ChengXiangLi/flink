/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 * <p/>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p/>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.flink.runtime.operators.util;

import com.google.common.base.Preconditions;
import org.apache.flink.core.memory.MemorySegment;
import org.apache.flink.util.Murmur3;

import static com.google.common.base.Preconditions.checkArgument;

public class BloomFilter2 {

	public static final double DEFAULT_FPP = 0.05;
	protected BitSet bitSet;
	protected int numBits;
	protected int numHashFunctions;

	public BloomFilter2(MemorySegment memeorySegment, int offset, int numBits, long expectedEntries, double fpp) {
		checkArgument(expectedEntries > 0, "expectedEntries should be > 0");
		checkArgument(fpp > 0.0 && fpp < 1.0, "False positive probability should be > 0.0 & < 1.0");
		this.numBits = numBits;
		this.numHashFunctions = optimalNumOfHashFunctions(expectedEntries, numBits);
		this.bitSet = new BitSet(memeorySegment, offset, numBits);
	}

	static int optimalNumOfHashFunctions(long n, long m) {
		return Math.max(1, (int) Math.round((double) m / n * Math.log(2)));
	}

	public static int optimalNumOfBits(long expectedEntries, double fpp) {
		return (int) (-expectedEntries * Math.log(fpp) / (Math.log(2) * Math.log(2)));
	}

	public void add(byte[] val) {
		if (val == null) {
			addBytes(val, -1);
		} else {
			addBytes(val, val.length);
		}
	}

	public void addBytes(byte[] val, int length) {
		// We use the trick mentioned in "Less Hashing, Same Performance: Building a Better Bloom Filter"
		// by Kirsch et.al. From abstract 'only two hash functions are necessary to effectively
		// implement a Bloom filter without any loss in the asymptotic false positive probability'

		// Lets split up 64-bit hashcode into two 32-bit hash codes and employ the technique mentioned
		// in the above paper
		long hash64 = val == null ? Murmur3.NULL_HASHCODE : Murmur3.hash64(val, length);
		addHash(hash64);
	}

	private void addHash(long hash64) {
		int hash1 = (int) hash64;
		int hash2 = (int) (hash64 >>> 32);

		for (int i = 1; i <= numHashFunctions; i++) {
			int combinedHash = hash1 + (i * hash2);
			// hashcode should be positive, flip all the bits if it's negative
			if (combinedHash < 0) {
				combinedHash = ~combinedHash;
			}
			int pos = combinedHash % numBits;
			bitSet.set(pos);
		}
	}

	public void addString(String val) {
		if (val == null) {
			add(null);
		} else {
			add(val.getBytes());
		}
	}

	public void addLong(long val) {
		addHash(getLongHash(val));
	}

	public void addDouble(double val) {
		addLong(Double.doubleToLongBits(val));
	}

	public boolean test(byte[] val) {
		if (val == null) {
			return testBytes(val, -1);
		}
		return testBytes(val, val.length);
	}

	public boolean testBytes(byte[] val, int length) {
		long hash64 = val == null ? Murmur3.NULL_HASHCODE : Murmur3.hash64(val, length);
		return testHash(hash64);
	}

	private boolean testHash(long hash64) {
		int hash1 = (int) hash64;
		int hash2 = (int) (hash64 >>> 32);

		for (int i = 1; i <= numHashFunctions; i++) {
			int combinedHash = hash1 + (i * hash2);
			// hashcode should be positive, flip all the bits if it's negative
			if (combinedHash < 0) {
				combinedHash = ~combinedHash;
			}
			int pos = combinedHash % numBits;
			if (!bitSet.get(pos)) {
				return false;
			}
		}
		return true;
	}

	public boolean testString(String val) {
		if (val == null) {
			return test(null);
		} else {
			return test(val.getBytes());
		}
	}

	public boolean testLong(long val) {
		return testHash(getLongHash(val));
	}

	// Thomas Wang's integer hash function
	// http://web.archive.org/web/20071223173210/http://www.concentric.net/~Ttwang/tech/inthash.htm
	private long getLongHash(long key) {
		key = (~key) + (key << 21); // key = (key << 21) - key - 1;
		key = key ^ (key >> 24);
		key = (key + (key << 3)) + (key << 8); // key * 265
		key = key ^ (key >> 14);
		key = (key + (key << 2)) + (key << 4); // key * 21
		key = key ^ (key >> 28);
		key = key + (key << 31);
		return key;
	}

	public boolean testDouble(double val) {
		return testLong(Double.doubleToLongBits(val));
	}

	public long sizeInBytes() {
		return getBitSize() / 8;
	}

	public int getBitSize() {
		return bitSet.bitSize();
	}

	public int getNumHashFunctions() {
		return numHashFunctions;
	}

	@Override
	public String toString() {
		return "m: " + numBits + " k: " + numHashFunctions;
	}

	public void reset() {
		this.bitSet.clear();
	}

	/**
	 * Bare metal bit set implementation. For performance reasons, this implementation does not check
	 * for index bounds nor expand the bit set size if the specified index is greater than the size.
	 */
	public class BitSet {
		private final MemorySegment memorySegment;
		private final int offset;
		private final int length;
		private final int LONG_POSITION_MASK = 0xffffffc0;

		public BitSet(MemorySegment memorySegment, int offset, int length) {
			Preconditions.checkArgument(length > 0, "bits size should be greater than 0.");
			Preconditions.checkArgument(length << 26 == 0, "bits size should be integral multiple of long size(64).");
			this.memorySegment = memorySegment;
			this.offset = offset;
			this.length = length;
		}

		/**
		 * Sets the bit at specified index.
		 *
		 * @param index - position
		 */
		public void set(int index) {
			int longIndex = index & LONG_POSITION_MASK;
			long current = memorySegment.getLong(offset + longIndex);
			current |= (1L << index);
			memorySegment.putLong(offset + longIndex, current);
		}

		/**
		 * Returns true if the bit is set in the specified index.
		 *
		 * @param index - position
		 * @return - value at the bit position
		 */
		public boolean get(int index) {
			int longIndex = index & LONG_POSITION_MASK;
			long current = memorySegment.getLong(offset + longIndex);
			return (current & (1L << index)) != 0;
		}

		/**
		 * Number of bits
		 */
		public int bitSize() {
			return length;
		}

		/**
		 * Clear the bit set.
		 */
		public void clear() {
			long zeroValue = 0L;
			for (int i = 0; i < (length / Long.SIZE); i++) {
				memorySegment.putLong(offset + i * Long.SIZE, zeroValue);
			}
		}
	}
}
