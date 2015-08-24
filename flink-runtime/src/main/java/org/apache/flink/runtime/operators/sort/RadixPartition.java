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

import org.apache.commons.lang3.tuple.Pair;
import org.apache.flink.api.common.typeutils.TypeSerializer;
import org.apache.flink.core.memory.MemorySegment;
import org.apache.flink.core.memory.MemorySegmentSource;
import org.apache.flink.core.memory.SeekableDataInputView;
import org.apache.flink.runtime.memorymanager.AbstractPagedInputView;
import org.apache.flink.runtime.memorymanager.AbstractPagedOutputView;
import org.apache.flink.runtime.util.MathUtils;
import org.apache.flink.util.MutableObjectIterator;

import java.io.EOFException;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

public class RadixPartition<T> {
	private final TypeSerializer<T> typeSerializer;
	private final List<MemorySegment> availableMemory;
	private RadixWriteBuffer writeBuffer;

	private int minKey = Integer.MAX_VALUE;
	private int elementCount = 0;

	public RadixPartition(TypeSerializer<T> typeSerializer, List<MemorySegment> availableMemory) {
		this.typeSerializer = typeSerializer;
		this.availableMemory = availableMemory;
		this.writeBuffer = new RadixWriteBuffer(forceGetNextBuffer(), getMemSource());
	}

	public void insert(int key, T value) throws IOException {
		this.writeBuffer.writeInt(key);
		this.typeSerializer.serialize(value, this.writeBuffer);
		elementCount++;
		if (key < this.minKey) {
			this.minKey = key;
		}
	}

	public int getMinKey() {
		return this.minKey;
	}

	public void flush() {
	}

	public int getElementCount() {
		return this.elementCount;
	}

	public MutableObjectIterator<Pair<Integer, T>> getPartitionIterator() throws IOException {
		final RadixReaderBuffer radixReaderBuffer = new RadixReaderBuffer(this.writeBuffer);
		return new PartitionIterator(radixReaderBuffer, typeSerializer);
	}

	/**
	 * Clean all the data.
	 */
	public void reset() {
		for (MemorySegment segment : this.writeBuffer.getSegments()) {
			this.availableMemory.add(segment);
		}
		this.elementCount = 0;
		this.minKey = Integer.MAX_VALUE;
		this.writeBuffer = new RadixWriteBuffer(forceGetNextBuffer(), getMemSource());
	}

	/**
	 * Clean all the data and resources.
	 */
	public void close() {
		for (MemorySegment segment : this.writeBuffer.getSegments()) {
			this.availableMemory.add(segment);
		}
		this.elementCount = 0;
		this.minKey = Integer.MAX_VALUE;
		this.writeBuffer = null;
	}

	private MemorySegmentSource getMemSource() {
		return new MemorySegmentSource() {
			@Override
			public MemorySegment nextSegment() {
				return forceGetNextBuffer();
			}
		};
	}

	private MemorySegment forceGetNextBuffer() {
		// TODO if none memory available, try spill partition.
		return getNextBuffer();
	}

	private MemorySegment getNextBuffer() {
		if (this.availableMemory.size() > 0) {
			return this.availableMemory.remove(this.availableMemory.size() - 1);
		} else {
			return null;
		}
	}

	// ============================================================================================

	protected static final class RadixWriteBuffer extends AbstractPagedOutputView {
		private final List<MemorySegment> targetList;

		private final MemorySegmentSource memSource;


		private RadixWriteBuffer(MemorySegment initialSegment, MemorySegmentSource memSource) {
			super(initialSegment, initialSegment.size(), 0);

			this.targetList = new ArrayList<>();
			this.targetList.add(initialSegment);
			this.memSource = memSource;
		}


		@Override
		protected MemorySegment nextSegment(MemorySegment current, int bytesUsed) throws IOException {
			final MemorySegment next;
			next = this.memSource.nextSegment();
			this.targetList.add(next);

			return next;
		}

		List<MemorySegment> getSegments() {
			return targetList;
		}
	}

	protected static final class RadixReaderBuffer extends AbstractPagedInputView implements SeekableDataInputView {
		private final MemorySegment[] memorySegments;
		private int currentBufferIndex = 1;
		private int finalBufferLimit;
		private int segmentSizeBits;
		private int memorySegmentSize;

		private RadixReaderBuffer(RadixWriteBuffer writeBuffer) {
			super(0);
			MemorySegment[] buffers = writeBuffer.getSegments().toArray(new MemorySegment[writeBuffer.getSegments().size()]);
			this.memorySegments = buffers;
			this.finalBufferLimit = writeBuffer.getCurrentPositionInSegment();
			this.memorySegmentSize = this.memorySegments[0].size();
			this.segmentSizeBits = MathUtils.log2strict(this.memorySegmentSize);
		}

		@Override
		protected MemorySegment nextSegment(MemorySegment current) throws IOException {
			if (currentBufferIndex < memorySegments.length) {
				MemorySegment segment = memorySegments[currentBufferIndex];
				currentBufferIndex++;
				return segment;
			} else {
				throw new EOFException();
			}
		}

		@Override
		protected int getLimitForSegment(MemorySegment segment) {
			return segment == memorySegments[memorySegments.length - 1] ? this.finalBufferLimit : segment.size();
		}

		@Override
		public void setReadPosition(long pointer) {

			final int bufferNum = (int) (pointer >>> this.segmentSizeBits);
			final int offset = (int) (pointer & (this.memorySegmentSize - 1));

			seekInput(this.memorySegments[bufferNum], offset,
				bufferNum < this.memorySegments.length - 1 ? this.memorySegmentSize : this.finalBufferLimit);
		}
	}

	// ============================================================================================

	final class PartitionIterator<T> implements MutableObjectIterator<Pair<Integer, T>> {

		private final RadixReaderBuffer readerBuffer;
		private final TypeSerializer<T> typeSerializer;

		private PartitionIterator(RadixReaderBuffer readerBuffer, TypeSerializer<T> typeSerializer) throws IOException {
			this.readerBuffer = readerBuffer;
			this.readerBuffer.setReadPosition(0);
			this.typeSerializer = typeSerializer;
		}


		public final Pair<Integer, T> next(Pair<Integer, T> reuse) throws IOException {
			return next();
		}

		public final Pair<Integer, T> next() throws IOException {
			try {
				int key = this.readerBuffer.readInt();
				T result = this.typeSerializer.deserialize(this.readerBuffer);
				return Pair.of(key, result);
			} catch (EOFException eofex) {
				return null;
			}
		}
	}
}
