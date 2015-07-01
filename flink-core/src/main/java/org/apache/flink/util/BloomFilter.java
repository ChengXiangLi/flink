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

package org.apache.flink.util;

import com.google.common.base.Preconditions;
import org.apache.flink.core.memory.MemorySegment;

import static com.google.common.base.Preconditions.checkArgument;

public class BloomFilter {

	protected BitSet bitSet;
	protected int numBits;
	protected int numHashFunctions;

	public BloomFilter(MemorySegment memeorySegment, int offset, int numBits, long expectedEntries) {
		checkArgument(expectedEntries > 0, "expectedEntries should be > 0");
		checkArgument(numBits << 29 == 0, "numBits should be multiple of Byte size(8).");
		this.numBits = numBits;
		this.numHashFunctions = optimalNumOfHashFunctions(expectedEntries, numBits);
		this.bitSet = new BitSet(memeorySegment, offset, numBits / 8);
	}

	static int optimalNumOfHashFunctions(long n, long m) {
		return Math.max(1, (int) Math.round((double) m / n * Math.log(2)));
	}

	public static int optimalNumOfBits(long expectedEntries, double fpp) {
		int bitsNum = (int) (-expectedEntries * Math.log(fpp) / (Math.log(2) * Math.log(2)));
		// make 'bitsNum' multiple of 64
		bitsNum = bitsNum + (Long.SIZE - (bitsNum % Long.SIZE));
		return bitsNum;
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

	public void addInt(int val) {
		addHash(getIntHash(val));
	}

	public void addLong(long val) {
		addHash(getLongHash(val));
	}

	public void addFloat(float val) {
		addInt(Float.floatToIntBits(val));
	}

	public void addDouble(double val) {
		addLong(Double.doubleToLongBits(val));
	}

	public void addString(String val) {
		if (val == null) {
			add(null);
		} else {
			add(val.getBytes());
		}
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

	public boolean testInt(int val) {
		return testHash(getIntHash(val));
	}

	public boolean testLong(long val) {
		return testHash(getLongHash(val));
	}

	public boolean testFloat(float val) {
		return testInt(Float.floatToIntBits(val));
	}

	public boolean testDouble(double val) {
		return testLong(Double.doubleToLongBits(val));
	}

	public boolean testString(String val) {
		if (val == null) {
			return test(null);
		} else {
			return test(val.getBytes());
		}
	}

	private long getIntHash(int key) {
		return getLongHash((long) key);
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

	public BitSet getBitSet() {
		return this.bitSet;
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

	public void reset() {
		this.bitSet.clear();
	}

	@Override
	public String toString() {
		StringBuilder output = new StringBuilder();
		output.append("BloomFilter:\n");
		output.append("\tbits number:").append(numBits).append("\n");
		output.append("\thash function number:").append(numHashFunctions).append("\n");
		output.append(bitSet);
		return output.toString();
	}

	/**
	 * Bare metal bit set implementation. For performance reasons, this implementation does not check
	 * for index bounds nor expand the bit set size if the specified index is greater than the size.
	 */
	public class BitSet {
		private final MemorySegment memorySegment;
		// MemorySegment byte array offset.
		private final int offset;
		// MemorySegment byte size.
		private final int length;
		private final int LONG_POSITION_MASK = 0xffffffc0;

		public BitSet(MemorySegment memorySegment, int offset, int length) {
			Preconditions.checkArgument(length > 0, "bits size should be greater than 0.");
			Preconditions.checkArgument(length << 29 == 0, "bytes size should be integral multiple of long size(64).");
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
			int longIndex = (index & LONG_POSITION_MASK) >>> 3;
			try {
				long current = memorySegment.getLong(offset + longIndex);
				current |= (1L << index);
				memorySegment.putLong(offset + longIndex, current);
			} catch (IndexOutOfBoundsException e) {
				System.out.println(String.format("MemorySegment size[%d], index[%d], longIndex[%d], offset[%d]", memorySegment.size(),
					index, longIndex, offset));
				throw e;
			}
		}

		/**
		 * Returns true if the bit is set in the specified index.
		 *
		 * @param index - position
		 * @return - value at the bit position
		 */
		public boolean get(int index) {
			int longIndex = (index & LONG_POSITION_MASK) >>> 3;
			try {
				long current = memorySegment.getLong(offset + longIndex);
				return (current & (1L << index)) != 0;
			} catch (IndexOutOfBoundsException e) {
				System.out.println(String.format("MemorySegment size[%d], index[%d], longIndex[%d], offset[%d]", memorySegment.size(),
					index, longIndex, offset));
				throw e;
			}
		}

		/**
		 * Number of bits
		 */
		public int bitSize() {
			return length << 3;
		}

		public MemorySegment getMemorySegment() {
			return this.memorySegment;
		}

		/**
		 * Clear the bit set.
		 */
		public void clear() {
			long zeroValue = 0L;
			for (int i = 0; i < (length / 8); i++) {
				memorySegment.putLong(offset + i * 8, zeroValue);
			}
		}

		@Override
		public String toString() {
			StringBuilder output = new StringBuilder();
			output.append("BitSet:\n");
			output.append("\tMemorySegment:").append(memorySegment.size()).append("\n");
			output.append("\tOffset:").append(offset).append("\n");
			output.append("\tLength:").append(length).append("\n");
			return output.toString();
		}
	}
}
