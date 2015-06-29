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
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;

public class BloomFilterTest {

    private BloomFilter2 bloomFilter;
    private static final int INPUT_SIZE = 1000;
    private static final double FALSE_POSITIVE_PROBABILITY = 0.05;

    @BeforeClass
    public void init() {
        int bitsSize = BloomFilter2.optimalNumOfBits(INPUT_SIZE, FALSE_POSITIVE_PROBABILITY);
        MemorySegment memorySegment = new MemorySegment(new byte[bitsSize]);
        bloomFilter = new BloomFilter2(memorySegment, 0, bitsSize, INPUT_SIZE, FALSE_POSITIVE_PROBABILITY);
    }

    @Test
    public void verifyIntInput() {
        bloomFilter.reset();
        for (int i = 0; i < INPUT_SIZE; i++) {
            bloomFilter.addLong((long) i);
        }

        for (int i = 0; i < INPUT_SIZE; i++) {
            Assert.assertTrue(bloomFilter.testLong((long) i));
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
    public void verifyStringInput() {
        bloomFilter.reset();
        for (int i = 0; i < INPUT_SIZE; i++) {
            bloomFilter.addString(Integer.toString(i));
        }

        for (int i = 0; i < INPUT_SIZE; i++) {
            Assert.assertTrue(bloomFilter.testString(Integer.toString(i)));
        }
    }

    @Test
    public void verifyFalsePositiveProbability() {
        bloomFilter.reset();
        for (int i = 0; i < INPUT_SIZE; i++) {
            bloomFilter.addLong((long) i);
        }

        int count = 0;
        for (int i = INPUT_SIZE; i < INPUT_SIZE + INPUT_SIZE; i++) {
            if (bloomFilter.testLong((long) i)) {
                count++;
            }
        }
        double actualFPP = count / INPUT_SIZE;
        Assert.assertTrue(String.format("Expect FPP %f, but get %f.", FALSE_POSITIVE_PROBABILITY, actualFPP),
                FALSE_POSITIVE_PROBABILITY > actualFPP);
    }
}
