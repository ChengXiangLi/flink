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

import org.apache.flink.api.common.typeutils.base.StringComparator;
import org.apache.flink.api.common.typeutils.base.StringSerializer;
import org.apache.flink.core.memory.MemorySegment;
import org.apache.flink.runtime.io.disk.iomanager.IOManager;
import org.apache.flink.runtime.io.disk.iomanager.IOManagerAsync;
import org.apache.flink.runtime.memorymanager.DefaultMemoryManager;
import org.apache.flink.runtime.memorymanager.MemoryAllocationException;
import org.apache.flink.runtime.operators.testutils.DummyInvokable;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.io.IOException;
import java.util.List;
import java.util.PriorityQueue;

public class RadixHeapPriorityQueueITCase {

	private static final int MEMORY_SIZE = 1024 * 1024 * 64;

	private static final int MEMORY_PAGE_SIZE = 32 * 1024;

	private DefaultMemoryManager memoryManager;
	private IOManager ioManager;

	private StringSerializer serializer;
	private StringComparator comparator;

	@Before
	public void init() {
		this.memoryManager = new DefaultMemoryManager(MEMORY_SIZE, MEMORY_PAGE_SIZE);
		this.ioManager = new IOManagerAsync();
		this.serializer = new StringSerializer();
		this.comparator = new StringComparator(true);
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
	public void simpleTest() throws MemoryAllocationException, IOException {
		final int numSegments = MEMORY_SIZE / MEMORY_PAGE_SIZE;
		final List<MemorySegment> memory = this.memoryManager.allocatePages(new DummyInvokable(), numSegments);
		RadixHeapPriorityQueue<String> heap = new RadixHeapPriorityQueue<>(serializer, comparator, memory, ioManager);

		try {
			long start = System.currentTimeMillis();
			for (int i=0; i<1024; i++) {
				heap.insert(i, i + "hahsdkfjeifalkhahsdkfjeifalksjdfioeoalsdjkfeiflasjedfiehahsdkfjeifalksjdfioeoalsdjkfeiflasjedfiehahsdkfjeifalksjdfioeoalsdjkfeiflasjedfiehahsdkfjeifalksjdfioeoalsdjkfeiflasjedfiesjdfioeoalsdjkfeiflasjedfie");
			}
			System.out.println(memory.size());
			long inserted = System.currentTimeMillis();
			System.out.println("insert cost ["+(inserted - start)+"]ms");
			String result = heap.poll();
			int count = 0;
			while (result != null && count <100) {
				count++;
				result = heap.poll();
			}
			System.out.println(count);
			long end = System.currentTimeMillis() - inserted;
			System.out.println("poll cost ["+end+"]ms");

			for (int i=0; i<1024; i++) {
				heap.insert(i, i + "hahsdkfjeifalkhahsdkfjeifalksjdfioeoalsdjkfeiflasjedfiehahsdkfjeifalksjdfioeoalsdjkfeiflasjedfiehahsdkfjeifalksjdfioeoalsdjkfeiflasjedfiehahsdkfjeifalksjdfioeoalsdjkfeiflasjedfiesjdfioeoalsdjkfeiflasjedfie");
			}
			result = heap.poll();
			count = 0;
			while (result != null && count <100) {
				count++;
				result = heap.poll();
			}
			System.out.println(count);
		} catch (Exception e) {
			throw e;
		} finally {
			heap.close();
			this.memoryManager.release(heap.availableMemory);
		}
	}

	@Test
	public void simpleTest2() throws MemoryAllocationException, IOException {
		PriorityQueue<Pair> heap = new PriorityQueue<>();

		try {
			long start = System.currentTimeMillis();
			for (int i=0; i<204800; i++) {
				Pair pair = new Pair(i, i + "hahsdkfjeifalkhahsdkfjeifalksjdfioeoalsdjkfeiflasjedfiehahsdkfjeifalksjdfioeoalsdjkfeiflasjedfiehahsdkfjeifalksjdfioeoalsdjkfeiflasjedfiehahsdkfjeifalksjdfioeoalsdjkfeiflasjedfiesjdfioeoalsdjkfeiflasjedfie");
				heap.add(pair);
			}
			long inserted = System.currentTimeMillis();
			System.out.println("insert cost ["+(inserted - start)+"]ms");
			Pair result = heap.poll();
			int count = 0;
			while (result != null && count<100) {
				count++;
				result = heap.poll();
			}
			long end = System.currentTimeMillis() - inserted;
			System.out.println("poll cost ["+end+"]ms");
			System.out.println(count);
		} catch (Exception e) {
			throw e;
		} finally {
		}
	}

}
class Pair implements Comparable<Pair>{
	int key;

	String value;

	public Pair(int key, String value) {
		this.key = key;
		this.value = value;
	}

	public String getValue() {
		return value;
	}

	public void setValue(String value) {
		this.value = value;
	}

	public int getKey() {
		return key;
	}

	public void setKey(int key) {
		this.key = key;
	}

	@Override
	public int compareTo(Pair o) {
		return this.key - o.getKey();
	}
}
