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
package org.apache.flink.runtime.operators.hash;

import org.apache.flink.core.memory.MemorySegment;

public class BloomFilter2 {

	/**
	 * Bare metal bit set implementation. For performance reasons, this implementation does not check
	 * for index bounds nor expand the bit set size if the specified index is greater than the size.
	 */
	public class BitSet {
		private final MemorySegment memorySegment;
		private final int offset;
		private final int length;

		public BitSet(MemorySegment memorySegment, int offset, int length) {
			assert length << 5 == 0 : "memory size should be able to mod by long size.";
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
			int longIndex = (index >>> 6) >> 6;
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
			int longIndex = (index >>> 6) >> 6;
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
			memorySegment.free();
		}
	}
}
