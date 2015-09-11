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

import java.io.IOException;
import java.util.List;

public class QuickHeapPriorityQueue<T> {

	/**
	 * Fix length records with a length below this threshold will be in-place sorted, if possible.
	 */
	private static final int THRESHOLD_FOR_IN_PLACE_SORTING = 32;

	private final List<MemorySegment> availableMemories;
	private final TypeSerializer<T> typeSerializer;
	private final TypeComparator<T> typeComparator;
	private final IOManager ioManager;
	private final QuickSelector<T> quickSelector;
	private T currentRecord;

	public QuickHeapPriorityQueue(List<MemorySegment> availableMemories, TypeSerializer<T> typeSerializer, TypeComparator<T> typeComparator, IOManager ioManager) {
		this.availableMemories = availableMemories;
		this.typeSerializer = typeSerializer;
		this.typeComparator = typeComparator;
		this.ioManager = ioManager;
		this.quickSelector = new FixedLengthQuickSelector<>(availableMemories, typeSerializer, typeComparator, ioManager);
		//		if (this.typeComparator.supportsSerializationWithKeyNormalization() &&
		//			this.typeSerializer.getLength() > 0 && this.typeSerializer.getLength() <= THRESHOLD_FOR_IN_PLACE_SORTING) {
		//		} else {
		//			//			this.selector = new NormalizedKeySorter<>(typeSerializer, typeComparator, availableMemories);
		//		}
	}

	public void insert(T record) throws IOException {
		this.quickSelector.insert(record);
	}

	public T poll() throws IOException {
		T result = null;
		if (currentRecord != null) {
			result = currentRecord;
			currentRecord = null;
		} else {
			result = this.quickSelector.next();
		}
		return result;
	}

	public T peek() throws IOException {
		if (currentRecord == null) {
			currentRecord = this.quickSelector.next();
		}
		return currentRecord;
	}

	public int size() {
		return this.quickSelector.size();
	}

	public List<MemorySegment> close() throws IOException {
		return this.quickSelector.close();
	}
}
