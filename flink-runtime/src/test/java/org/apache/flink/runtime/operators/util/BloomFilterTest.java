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

import org.apache.flink.core.memory.MemorySegment;
import org.apache.flink.util.BloomFilter;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;

public class BloomFilterTest {

	private static BloomFilter bloomFilter;
	private static final int INPUT_SIZE = 1024;
	private static final double FALSE_POSITIVE_PROBABILITY = 0.05;

	@BeforeClass
	public static void init() {
		int bitsSize = BloomFilter.optimalNumOfBits(INPUT_SIZE, FALSE_POSITIVE_PROBABILITY);
		MemorySegment memorySegment = new MemorySegment(new byte[bitsSize/8]);
		bloomFilter = new BloomFilter(memorySegment, 0, bitsSize, INPUT_SIZE);
		System.out.println(String.format("BloomFilter bitSize[%d], hashFunctionNumber[%d]", bitsSize, bloomFilter.getNumHashFunctions()));
	}

	@Test
	public void verifyIntInput() {
		bloomFilter.reset();
		for (int i = 0; i < INPUT_SIZE; i++) {
			bloomFilter.addInt(i);
		}

		for (int i = 0; i < INPUT_SIZE; i++) {
			Assert.assertTrue(bloomFilter.testInt(i));
		}
	}

	@Test
	public void verifyLongInput() {
		bloomFilter.reset();
		for (long i = 0; i < INPUT_SIZE; i++) {
			bloomFilter.addLong(i);
		}

		for (long i = 0; i < INPUT_SIZE; i++) {
			Assert.assertTrue(bloomFilter.testLong(i));
		}
	}

	@Test
	public void verifyFloatInput() {
		bloomFilter.reset();
		final float additional = 0.1f;
		for (int i = 0; i < INPUT_SIZE; i++) {
			bloomFilter.addFloat(i + additional);
		}

		for (int i = 0; i < INPUT_SIZE; i++) {
			Assert.assertTrue(bloomFilter.testFloat(i + additional));
		}
	}

	@Test
	public void verifyDoubleInput() {
		bloomFilter.reset();
		final double additional = 0.1d;
		for (long i = 0; i < INPUT_SIZE; i++) {
			bloomFilter.addDouble(i + additional);
		}

		for (long i = 0; i < INPUT_SIZE; i++) {
			Assert.assertTrue(bloomFilter.testDouble(i + additional));
		}
	}

	@Test
	public void verifyStringInput() {
		bloomFilter.reset();
		for (int i = 0; i < INPUT_SIZE; i++) {
			bloomFilter.addString(Integer.toHexString(i));
		}

		for (int i = 0; i < INPUT_SIZE; i++) {
			Assert.assertTrue(bloomFilter.testString(Integer.toHexString(i)));
		}
	}

	@Test
	public void verifyIntFalsePositiveProbability() {
		bloomFilter.reset();
		for (int i = 0; i < INPUT_SIZE; i++) {
			bloomFilter.addInt(i);
		}

		int count = 0;
		for (int i = INPUT_SIZE; i < INPUT_SIZE + INPUT_SIZE; i++) {
			if (bloomFilter.testInt(i)) {
				count++;
			}
		}
		double actualFPP = (double)count / INPUT_SIZE;
		// FALSE_POSITIVE_PROBABILITY is not an accurate limit, we can accept the actual false positive probability
		// if it less than 2 * FALSE_POSITIVE_PROBABILITY.
		String message = String.format("Expect FPP less than %f, but get %f.", FALSE_POSITIVE_PROBABILITY, actualFPP);
		Assert.assertTrue(message, 2 * FALSE_POSITIVE_PROBABILITY > actualFPP);
	}

	@Test
	public void verifyLongFalsePositiveProbability() {
		bloomFilter.reset();
		for (long i = 0; i < INPUT_SIZE; i++) {
			bloomFilter.addLong(i);
		}

		int count = 0;
		for (long i = INPUT_SIZE; i < INPUT_SIZE + INPUT_SIZE; i++) {
			if (bloomFilter.testLong(i)) {
				count++;
			}
		}
		double actualFPP = (double)count / INPUT_SIZE;
		String message = String.format("Expect FPP less than %f, but get %f.", FALSE_POSITIVE_PROBABILITY, actualFPP);
		Assert.assertTrue(message, 2 * FALSE_POSITIVE_PROBABILITY > actualFPP);
	}

	@Test
	public void verifyFloatFalsePositiveProbability() {
		bloomFilter.reset();
		final float additional = 0.1f;
		for (int i = 0; i < INPUT_SIZE; i++) {
			bloomFilter.addFloat(i + additional);
		}

		int count = 0;
		for (int i = INPUT_SIZE; i < INPUT_SIZE + INPUT_SIZE; i++) {
			if (bloomFilter.testFloat(i + additional)) {
				count++;
			}
		}
		double actualFPP = (double)count / INPUT_SIZE;
		String message = String.format("Expect FPP less than %f, but get %f.", FALSE_POSITIVE_PROBABILITY, actualFPP);
		Assert.assertTrue(message, 2 * FALSE_POSITIVE_PROBABILITY > actualFPP);
	}

	@Test
	public void verifyDoubleFalsePositiveProbability() {
		bloomFilter.reset();
		final double additional = 0.1d;
		for (int i = 0; i < INPUT_SIZE; i++) {
			bloomFilter.addDouble(i + additional);
		}

		int count = 0;
		for (int i = INPUT_SIZE; i < INPUT_SIZE + INPUT_SIZE; i++) {
			if (bloomFilter.testDouble(i + additional)) {
				count++;
			}
		}
		double actualFPP = (double)count / INPUT_SIZE;
		String message = String.format("Expect FPP less than %f, but get %f.", FALSE_POSITIVE_PROBABILITY, actualFPP);
		Assert.assertTrue(message, 2 * FALSE_POSITIVE_PROBABILITY > actualFPP);
	}

	@Test
	public void verifyStringFalsePositiveProbability() {
		bloomFilter.reset();
		for (int i = 0; i < INPUT_SIZE; i++) {
			bloomFilter.addString(Integer.toHexString(i));
		}

		int count = 0;
		for (int i = INPUT_SIZE; i < INPUT_SIZE; i++) {
			if (bloomFilter.testString(Integer.toHexString(i))) {
				count++;
			}
		}
		double actualFPP = (double)count / INPUT_SIZE;
		String message = String.format("Expect FPP less than %f, but get %f.", FALSE_POSITIVE_PROBABILITY, actualFPP);
		Assert.assertTrue(message, 2 * FALSE_POSITIVE_PROBABILITY > actualFPP);
	}
}
