/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.flink.runtime.operators.sort;

import org.apache.flink.api.common.typeutils.TypeComparator;
import org.apache.flink.api.common.typeutils.TypeSerializer;
import org.apache.flink.core.memory.MemorySegment;
import org.apache.flink.runtime.io.disk.iomanager.IOManager;
import org.apache.flink.runtime.io.disk.iomanager.IOManagerAsync;
import org.apache.flink.runtime.memorymanager.DefaultMemoryManager;
import org.apache.flink.runtime.operators.testutils.DummyInvokable;
import org.apache.flink.runtime.operators.testutils.types.IntPair;
import org.apache.flink.runtime.operators.testutils.types.IntPairComparator;
import org.apache.flink.runtime.operators.testutils.types.IntPairSerializer;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.util.List;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;

public class FixedLengthQuickSelectorITCase {
	private static final int MEMORY_SIZE = 1024 * 32 * 7;

	private static final int MEMORY_PAGE_SIZE = 32 * 1024;

	private DefaultMemoryManager memoryManager;
	private IOManager ioManager;

	private TypeSerializer<IntPair> serializer;

	private TypeComparator<IntPair> comparator;

	@Before
	public void init() {
		this.memoryManager = new DefaultMemoryManager(MEMORY_SIZE, MEMORY_PAGE_SIZE);
		this.ioManager = new IOManagerAsync();
		this.serializer = new IntPairSerializer();
		this.comparator = new IntPairComparator();
	}

	@After
	public void afterTest() {
		if (!this.memoryManager.verifyEmpty()) {
			Assert.fail("Memory Leak: Some memory has not been returned to the memory manager.");
		}

		if (this.memoryManager != null) {
			this.memoryManager.shutdown();
			this.memoryManager = null;
		}
	}

	// ------------------------------------------------------------------------------------------------
	// --------------------------------- Boundary verification ----------------------------------------
	// ------------------------------------------------------------------------------------------------

	@Test
	public void testEmptyFixedLengthQuickSelector() throws Exception {
		final int numSegments = MEMORY_SIZE / MEMORY_PAGE_SIZE;
		final List<MemorySegment> memory = this.memoryManager.allocatePages(new DummyInvokable(), numSegments);
		FixedLengthQuickSelector<IntPair> selector = new FixedLengthQuickSelector<>(memory, serializer, comparator, ioManager);
		try {
			IntPair next = selector.next();
			assertNull(next);
		} finally {
			this.memoryManager.release(selector.close());
		}
	}

	// ------------------------------------------------------------------------------------------------
	// --------------------------------- Correctness verification ----------------------------------------
	// ------------------------------------------------------------------------------------------------

	@Test
	public void testFixedLengthQuickSelector1() throws Exception {
		final int numSegments = MEMORY_SIZE / MEMORY_PAGE_SIZE;
		final List<MemorySegment> memory = this.memoryManager.allocatePages(new DummyInvokable(), numSegments);
		FixedLengthQuickSelector<IntPair> selector = new FixedLengthQuickSelector<>(memory, serializer, comparator, ioManager);
		try {
			selector.insert(new IntPair(1, 1));
			selector.insert(new IntPair(6, 6));
			selector.insert(new IntPair(2, 2));
			selector.insert(new IntPair(5, 5));
			selector.insert(new IntPair(0, 0));
			selector.insert(new IntPair(3, 3));
			selector.insert(new IntPair(4, 4));

			for (int i = 0; i < 7; i++) {
				IntPair next = selector.next();
				assertEquals(i, next.getKey());
			}
		} finally {
			this.memoryManager.release(selector.close());
		}
	}

	@Test
	public void testFixedLengthQuickSelectorWithFlush() throws Exception {
		final int numSegments = MEMORY_SIZE / MEMORY_PAGE_SIZE;
		final List<MemorySegment> memory = this.memoryManager.allocatePages(new DummyInvokable(), numSegments);
		FixedLengthQuickSelector<IntPair> selector = new FixedLengthQuickSelector<>(memory, serializer, comparator, ioManager);
		try {
			int recordNumber = (1024 * 32 * 4) / 8;

			for (int i = recordNumber - 1; i >= 0; i--) {
				selector.insert(new IntPair(i, i));
			}

			for (int i = 0; i < recordNumber; i++) {
				IntPair next = selector.next();
				assertEquals(i, next.getKey());
			}
		} finally {
			this.memoryManager.release(selector.close());
		}
	}

	@Test
	public void testFixedLengthQuickSelectorWithFlushDuringReload() throws Exception {
		final int numSegments = MEMORY_SIZE / MEMORY_PAGE_SIZE;
		final List<MemorySegment> memory = this.memoryManager.allocatePages(new DummyInvokable(), numSegments);
		FixedLengthQuickSelector<IntPair> selector = new FixedLengthQuickSelector<>(memory, serializer, comparator, ioManager);
		try {
			int recordNumber = (1024 * 32 * 7) / 8;

			for (int i = recordNumber - 1; i >= 0; i--) {
				selector.insert(new IntPair(i, i));
			}

			for (int i = 0; i < recordNumber; i++) {
				IntPair next = selector.next();
				assertEquals(i, next.getKey());
			}
		} finally {
			this.memoryManager.release(selector.close());
		}
	}
}
