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

import org.apache.flink.api.common.typeutils.base.StringSerializer;
import org.apache.flink.core.memory.MemorySegment;
import org.apache.flink.runtime.io.disk.iomanager.IOManager;
import org.apache.flink.runtime.io.disk.iomanager.IOManagerAsync;
import org.apache.flink.runtime.memorymanager.DefaultMemoryManager;
import org.apache.flink.runtime.operators.testutils.DummyInvokable;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.io.IOException;
import java.util.List;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

public class RadixHeapPriorityQueueITCase {

	private static final int MEMORY_SIZE = 1024 * 1024 * 4;

	private static final int MEMORY_PAGE_SIZE = 32 * 1024;

	private DefaultMemoryManager memoryManager;
	private IOManager ioManager;

	private StringSerializer serializer;

	@Before
	public void init() {
		this.memoryManager = new DefaultMemoryManager(MEMORY_SIZE, MEMORY_PAGE_SIZE);
		this.ioManager = new IOManagerAsync();
		this.serializer = new StringSerializer();
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

	@Test
	public void testEmptyRadixHeapPriorityQueue() throws Exception {
		final int numSegments = MEMORY_SIZE / MEMORY_PAGE_SIZE;
		final List<MemorySegment> memory = this.memoryManager.allocatePages(new DummyInvokable(), numSegments);
		RadixHeapPriorityQueue<String> heap = new RadixHeapPriorityQueue<>(serializer, memory, ioManager);
		try {
			String result = heap.poll();
			assertTrue("Heap should poll null while it's empty.", result == null);

		} finally {
			heap.close();
			this.memoryManager.release(heap.availableMemory);
		}
	}

	@Test
	public void testInMemoryRadixHeapPriorityQueue() throws Exception {
		final int numSegments = MEMORY_SIZE / MEMORY_PAGE_SIZE;
		final List<MemorySegment> memory = this.memoryManager.allocatePages(new DummyInvokable(), numSegments);
		RadixHeapPriorityQueue<String> heap = new RadixHeapPriorityQueue<>(serializer, memory, ioManager);
		try {
			// Insert by index from 0 to 49 with ascend order.
			for (int i = 0; i < 50; i++) {
				heap.insert(i, i + " test data.");
			}

			// Insert by index from 99 to 50 with descend order.
			for (int i = 99; i >= 50; i--) {
				heap.insert(i, i + " test data.");
			}

			for (int i = 0; i < 100; i++) {
				String result = heap.poll();
				assertEquals("RadixHeapPriorityQueue should poll smallest element each time.", i + " test data.", result);
			}

		} finally {
			heap.close();
			this.memoryManager.release(heap.availableMemory);
		}
	}

	@Test
	public void testInMemoryRadixHeapPriorityQueueWithInsertAfterPoll() throws Exception {
		final int numSegments = MEMORY_SIZE / MEMORY_PAGE_SIZE;
		final List<MemorySegment> memory = this.memoryManager.allocatePages(new DummyInvokable(), numSegments);
		RadixHeapPriorityQueue<String> heap = new RadixHeapPriorityQueue<>(serializer, memory, ioManager);
		try {
			// Insert by index from 0 to 49 with ascend order.
			for (int i = 0; i < 50; i++) {
				heap.insert(i, i + " test data.");
			}

			for (int i = 0; i < 50; i++) {
				String result = heap.poll();
				assertEquals("RadixHeapPriorityQueue should poll smallest element each time.", i + " test data.", result);
			}

			// Insert by index from 99 to 50 with descend order.
			for (int i = 99; i >= 50; i--) {
				heap.insert(i, i + " test data.");
			}

			for (int i = 50; i < 100; i++) {
				String result = heap.poll();
				assertEquals("RadixHeapPriorityQueue should poll smallest element each time.", i + " test data.", result);
			}
		} finally {
			heap.close();
			this.memoryManager.release(heap.availableMemory);
		}
	}

	@Test(expected = Exception.class)
	public void testInMemoryRadixHeapPriorityQueueWithInvalidInsert() throws Exception {
		final int numSegments = MEMORY_SIZE / MEMORY_PAGE_SIZE;
		final List<MemorySegment> memory = this.memoryManager.allocatePages(new DummyInvokable(), numSegments);
		RadixHeapPriorityQueue<String> heap = new RadixHeapPriorityQueue<>(serializer, memory, ioManager);
		try {
			// Insert by index from 0 to 49 with ascend order.
			for (int i = 0; i < 50; i++) {
				heap.insert(i, i + " test data.");
			}

			for (int i = 0; i < 50; i++) {
				String result = heap.poll();
				assertEquals("RadixHeapPriorityQueue should poll smallest element each time.", i + " test data.", result);
			}

			heap.insert(1, "1 invalid test data.");

		} finally {
			heap.close();
			this.memoryManager.release(heap.availableMemory);
		}
	}

	@Test
	public void testInMemoryRadixHeapPriorityQueueWithPeek() throws Exception {
		final int numSegments = MEMORY_SIZE / MEMORY_PAGE_SIZE;
		final List<MemorySegment> memory = this.memoryManager.allocatePages(new DummyInvokable(), numSegments);
		RadixHeapPriorityQueue<String> heap = new RadixHeapPriorityQueue<>(serializer, memory, ioManager);
		try {
			// Insert by index from 0 to 49 with ascend order.
			for (int i = 0; i < 100; i++) {
				heap.insert(i, i + " test data.");
			}

			String peeked1 = heap.peek();
			assertEquals("RadixHeapPriorityQueue peek() should not remove smallest element from heap.", peeked1, "0 test data.");
			String peeked2 = heap.peek();
			assertEquals("RadixHeapPriorityQueue peek() should not remove smallest element from heap.", peeked1, peeked2);
			String polled = heap.poll();
			assertEquals("RadixHeapPriorityQueue peek() should not remove smallest element from heap.", peeked1, polled);
		} finally {
			heap.close();
			this.memoryManager.release(heap.availableMemory);
		}
	}

	@Test
	public void testInMemoryRadixHeapPriorityQueueWithMultiClose() throws Exception {
		final int numSegments = MEMORY_SIZE / MEMORY_PAGE_SIZE;
		final List<MemorySegment> memory = this.memoryManager.allocatePages(new DummyInvokable(), numSegments);
		RadixHeapPriorityQueue<String> heap = new RadixHeapPriorityQueue<>(serializer, memory, ioManager);
		try {
			// Insert by index from 0 to 49 with ascend order.
			for (int i = 0; i < 100; i++) {
				heap.insert(i, i + " test data.");
			}
		} finally {
			heap.close();
			heap.close();
			this.memoryManager.release(heap.availableMemory);
		}
	}

	@Test(expected = IOException.class)
	public void testInMemoryRadixHeapPriorityQueueInsertAfterClose() throws Exception {
		final int numSegments = MEMORY_SIZE / MEMORY_PAGE_SIZE;
		final List<MemorySegment> memory = this.memoryManager.allocatePages(new DummyInvokable(), numSegments);
		RadixHeapPriorityQueue<String> heap = new RadixHeapPriorityQueue<>(serializer, memory, ioManager);
		try {
			// Insert by index from 0 to 49 with ascend order.
			for (int i = 0; i < 100; i++) {
				heap.insert(i, i + " test data.");
			}
			heap.close();
			heap.insert(100, "100 test data.");
		} finally {
			heap.close();
			this.memoryManager.release(heap.availableMemory);
		}
	}

	@Test
	public void testSpilledRadixHeapPriorityQueue() throws Exception {
		final int numSegments = MEMORY_SIZE / MEMORY_PAGE_SIZE;
		final List<MemorySegment> memory = this.memoryManager.allocatePages(new DummyInvokable(), numSegments);
		RadixHeapPriorityQueue<String> heap = new RadixHeapPriorityQueue<>(serializer, memory, ioManager);
		String insertStr = new String(new byte[1024]);
		try {
			// Insert by index from 0 to 49 with ascend order.
			for (int i = 0; i < 5000; i++) {
				heap.insert(i, i + insertStr);
			}

			// Insert by index from 99 to 50 with descend order.
			for (int i = 9999; i >= 5000; i--) {
				heap.insert(i, i + insertStr);
			}

			for (int i = 0; i < 10000; i++) {
				String result = heap.poll();
				assertEquals("RadixHeapPriorityQueue should poll smallest element each time.", i + insertStr, result);
			}

		} finally {
			heap.close();
			this.memoryManager.release(heap.availableMemory);
		}
	}

	@Test
	public void testSpilledRadixHeapPriorityQueueWithInsertAfterPoll() throws Exception {
		final int numSegments = MEMORY_SIZE / MEMORY_PAGE_SIZE;
		final List<MemorySegment> memory = this.memoryManager.allocatePages(new DummyInvokable(), numSegments);
		RadixHeapPriorityQueue<String> heap = new RadixHeapPriorityQueue<>(serializer, memory, ioManager);
		String insertStr = new String(new byte[1024]);
		try {
			// Insert by index from 0 to 49 with ascend order.
			for (int i = 0; i < 5000; i++) {
				heap.insert(i, i + insertStr);
			}

			for (int i = 0; i < 5000; i++) {
				String result = heap.poll();
				assertEquals("RadixHeapPriorityQueue should poll smallest element each time.", i + insertStr, result);
			}

			// Insert by index from 99 to 50 with descend order.
			for (int i = 9999; i >= 5000; i--) {
				heap.insert(i, i + insertStr);
			}

			for (int i = 5000; i < 10000; i++) {
				String result = heap.poll();
				assertEquals("RadixHeapPriorityQueue should poll smallest element each time.", i + insertStr, result);
			}

		} finally {
			heap.close();
			this.memoryManager.release(heap.availableMemory);
		}
	}

	@Test
	public void testSpilledRadixHeapPriorityQueueWithPeek() throws Exception {
		final int numSegments = MEMORY_SIZE / MEMORY_PAGE_SIZE;
		final List<MemorySegment> memory = this.memoryManager.allocatePages(new DummyInvokable(), numSegments);
		RadixHeapPriorityQueue<String> heap = new RadixHeapPriorityQueue<>(serializer, memory, ioManager);
		String insertStr = new String(new byte[1024]);
		try {
			// Insert by index from 0 to 49 with ascend order.
			for (int i = 0; i < 10000; i++) {
				heap.insert(i, i + insertStr);
			}

			for (int i = 0; i < 5000; i++) {
				heap.poll();
			}

			String peeked1 = heap.peek();
			assertEquals("RadixHeapPriorityQueue peek() should not remove smallest element from heap.", 5000 + insertStr, peeked1);
			String polled = heap.poll();
			assertEquals("RadixHeapPriorityQueue peek() should not remove smallest element from heap.", peeked1, polled);
			String peeked2 = heap.peek();
			assertEquals("RadixHeapPriorityQueue peek() should not remove smallest element from heap.", 5001 + insertStr, peeked2);
		} finally {
			heap.close();
			this.memoryManager.release(heap.availableMemory);
		}
	}

	@Test
	public void testSpilledRadixHeapPriorityQueueWithMultiClose() throws Exception {
		final int numSegments = MEMORY_SIZE / MEMORY_PAGE_SIZE;
		final List<MemorySegment> memory = this.memoryManager.allocatePages(new DummyInvokable(), numSegments);
		RadixHeapPriorityQueue<String> heap = new RadixHeapPriorityQueue<>(serializer, memory, ioManager);
		String insertStr = new String(new byte[1024]);
		try {
			// Insert by index from 0 to 49 with ascend order.
			for (int i = 0; i < 10000; i++) {
				heap.insert(i, i + insertStr);
			}
		} finally {
			heap.close();
			heap.close();
			this.memoryManager.release(heap.availableMemory);
		}
	}

	@Test(expected = Exception.class)
	public void testSpilledRadixHeapPriorityQueueInsertAfterClose() throws Exception {
		final int numSegments = MEMORY_SIZE / MEMORY_PAGE_SIZE;
		final List<MemorySegment> memory = this.memoryManager.allocatePages(new DummyInvokable(), numSegments);
		RadixHeapPriorityQueue<String> heap = new RadixHeapPriorityQueue<>(serializer, memory, ioManager);
		String insertStr = new String(new byte[1024]);
		try {
			// Insert by index from 0 to 49 with ascend order.
			for (int i = 0; i < 10000; i++) {
				heap.insert(i, i + insertStr);
			}
			heap.close();
			heap.insert(10000, 10000 + insertStr);
		} finally {
			heap.close();
			this.memoryManager.release(heap.availableMemory);
		}
	}
}
