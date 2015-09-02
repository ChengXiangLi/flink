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

import java.util.List;

public class QuickHeapPriorityQueue<T> {

	private List<MemorySegment> availableMemories;
	private IOManager ioManager;
	private TypeSerializer<T> typeSerializer;
	private TypeComparator<T> typeComparator;
	private MemorySegment[] keyBuffers;
	private int currentKeyBufferIndex;
	private int currentPositionInKeyBuffer;
	private MemorySegment[] valueBuffers;
	private int currentValueBufferIndex;
	private int currentPositionInValueBuffer;
	private MemorySegment valueBufferBridge;

	public QuickHeapPriorityQueue(TypeSerializer<T> typeSerializer, TypeComparator<T> typeComparator, List<MemorySegment> availableMemories, IOManager ioManager) {
		this.typeSerializer = typeSerializer;
		this.typeComparator = typeComparator;
		this.availableMemories = availableMemories;
		this.ioManager = ioManager;
		this.currentKeyBufferIndex = 0;
		this.currentPositionInKeyBuffer = 0;
		this.currentValueBufferIndex = 0;
		this.currentPositionInValueBuffer = 0;
	}

	public void insert(T value) {

	}

	public T poll() {
		return null;
	}

	public T peek() {
		return null;
	}

	private void advance() {}

	private void flush() {
	}

	// add value to value buffer in sequence, and return the start position.
	private long addToValueBuffer(T value) {
		return 0;
	}

	// iterate the value buffers, if value is after latest pivot, spill to disk, else copy to new buffer, and update pointer in key buffers.
	private void refactorValueBuffer() {}

	// implement quick select algorithm, reorganize key buffers until pivot at index 0 is found.
	private void quickSelect() {

	}
}
