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

import com.google.common.base.Preconditions;
import org.apache.commons.lang3.tuple.Pair;
import org.apache.flink.api.common.typeutils.TypeComparator;
import org.apache.flink.api.common.typeutils.TypeSerializer;
import org.apache.flink.core.memory.MemorySegment;
import org.apache.flink.runtime.io.disk.iomanager.FileIOChannel;
import org.apache.flink.runtime.io.disk.iomanager.IOManager;
import org.apache.flink.util.MutableObjectIterator;

import java.io.IOException;
import java.util.List;
import java.util.concurrent.LinkedBlockingQueue;

/**
 * @param <T>
 */
public class RadixHeapPriorityQueue<T> {

	// ------------------------------------------------------------------------
	//                              Constants
	// ------------------------------------------------------------------------

	private final static int BUCKET_NUMBER = 33;

	// ------------------------------------------------------------------------
	//                              Members
	// ------------------------------------------------------------------------

	/**
	 * The utilities to serialize the build side data types.
	 */
	protected final TypeSerializer<T> typeSerializer;

	/**
	 * The utilities to hash and compare the probe side data types.
	 */
	private final TypeComparator<T> typeComparator;

	/**
	 * The free memory segments currently available to the hash join.
	 */
	protected final List<MemorySegment> availableMemory;

	/**
	 * The I/O manager used to instantiate writers for the spilled partitions.
	 */
	protected final IOManager ioManager;

	protected final LinkedBlockingQueue<MemorySegment> writeBehindBuffers;

	/**
	 * The channel enumerator that is used while processing the current partition to create
	 * channels for the spill partitions it requires.
	 */
	protected FileIOChannel.Enumerator currentEnumerator;

	private final RadixPartition<T>[] radixPartitions;

	private int deleteMin = 0;

	private T currentValue;

	private int currentBucketIndex = 0;

	private MutableObjectIterator<Pair<Integer, T>> currentPartitionIterator;

	public RadixHeapPriorityQueue(TypeSerializer<T> typeSerializer, TypeComparator<T> typeComparator, List<MemorySegment> availableMemory, IOManager ioManager) {
		Preconditions.checkArgument(availableMemory != null, "availableMemory can not be null.");
		Preconditions.checkArgument(availableMemory.size() >= 33, "RadixHeapPriorityQueue requires at least 33 segments.");
		this.typeSerializer = typeSerializer;
		this.typeComparator = typeComparator;
		this.availableMemory = availableMemory;
		this.ioManager = ioManager;
		this.radixPartitions = new RadixPartition[BUCKET_NUMBER];
		this.currentEnumerator = this.ioManager.createChannelEnumerator();
		this.writeBehindBuffers = new LinkedBlockingQueue<>();
		initiateRadixPartitions();
	}

	private void initiateRadixPartitions() {
		for (int i = 0; i < BUCKET_NUMBER; i++) {
			radixPartitions[i] = new RadixPartition<>(this.typeSerializer, this.availableMemory, this.ioManager, this);
		}
	}

	public final void insert(int key, T value) throws IOException {
		int index = getIndex(deleteMin, key);
		radixPartitions[index].insert(key, value);
	}

	// Remove.
	private final T next() throws IOException {
		if (currentPartitionIterator == null) {
			currentPartitionIterator = radixPartitions[0].getPartitionIterator();
			currentBucketIndex = 0;
		}

		Pair<Integer, T> next = currentPartitionIterator.next();
		if (next != null) {
			return next.getValue();
		} else {
			currentBucketIndex++;
			for (int i = currentBucketIndex; i < BUCKET_NUMBER; i++) {
				if (radixPartitions[i].getElementCount() == 1) {
					currentPartitionIterator =radixPartitions[i].getPartitionIterator();
					currentBucketIndex = i;
					break;
				}
				if (radixPartitions[i].getElementCount() > 1) {
					rebuildRadix(i);
					break;
				}
			}
			next = currentPartitionIterator.next();
			if (next == null) {
				return null;
			} else {
				return next.getValue();
			}
		}
	}

	public final T poll() throws IOException {
		currentValue = next();
		return currentValue;
	}

	// Do not remove.
	public final T peek() throws IOException {
		if (currentValue == null) {
			currentValue = next();
		}
		return currentValue;
	}

	public int spillPartition() throws IOException {
		int maxCount = 0;
		int index = 0;
		for(int i=0; i<BUCKET_NUMBER; i++) {
			if (radixPartitions[i].getElementCount() > maxCount) {
				maxCount = radixPartitions[i].getElementCount();
				index = i;
			}
		}

		radixPartitions[index].flush(ioManager.createBlockChannelWriter(currentEnumerator.next(), writeBehindBuffers));
		return index;
	}

	public MemorySegment getNextBuffer() {
		if (this.availableMemory.size() > 0) {
			return this.availableMemory.remove(this.availableMemory.size() - 1);
		} else {
			return null;
		}
	}

	// Read key value from RadixPartition, get the min key as the deleteMin, and insert key value again.
	private void rebuildRadix(int bucketIndex) throws IOException {
		for (int i = 0; i < bucketIndex; i++) {
			radixPartitions[i].reset();
		}

		MutableObjectIterator<Pair<Integer, T>> partitionIterator = radixPartitions[bucketIndex].getPartitionIterator();
		this.deleteMin = radixPartitions[bucketIndex].getMinKey();
		Pair<Integer, T> pair = partitionIterator.next();
		while (pair != null) {
			insert(pair.getKey(), pair.getValue());
			pair = partitionIterator.next();
		}

		radixPartitions[bucketIndex].reset();

		currentPartitionIterator = radixPartitions[0].getPartitionIterator();
		currentBucketIndex = 0;
	}

	/**
	 * Get the bucket index of key based on min value. The items in bucket k differ from min in bit k-1, but not in bit k or higher.
	 *
	 * @param min Current min value.
	 * @param key Element key.
	 * @return Bucket index.
	 */
	private int getIndex(int min, int key) {
		int xor = min ^ key;
		if (xor < 1) {
			return xor;
		} else {
			int index = 1;
			while (xor > 1) {
				xor = xor >> 1;
				index++;
			}
			return index;
		}
	}

	public void close() {
		for (int i = 0; i < BUCKET_NUMBER; i++) {
			radixPartitions[i].close();
		}
	}
}
