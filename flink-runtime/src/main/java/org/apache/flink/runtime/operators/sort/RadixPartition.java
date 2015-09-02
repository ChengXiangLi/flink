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
import org.apache.flink.runtime.io.disk.iomanager.*;
import org.apache.flink.runtime.memorymanager.AbstractPagedInputView;
import org.apache.flink.runtime.memorymanager.AbstractPagedOutputView;
import org.apache.flink.runtime.util.MathUtils;
import org.apache.flink.util.MutableObjectIterator;

import java.io.EOFException;
import java.io.IOException;
import java.util.ArrayList;
import java.util.LinkedList;
import java.util.List;
import java.util.Queue;

public class RadixPartition<T> {
	private final TypeSerializer<T> typeSerializer;
	private final List<MemorySegment> availableMemory;
	private final IOManager ioManager;
	private BlockChannelWriter<MemorySegment> flushedChannel;
	private RadixWriteBuffer writeBuffer;
	private RadixHeapPriorityQueue<T> queue;

	private int minKey = Integer.MAX_VALUE;
	private int elementCount = 0;

	public RadixPartition(TypeSerializer<T> typeSerializer, List<MemorySegment> availableMemory, IOManager ioManager, RadixHeapPriorityQueue<T> queue) {
		this.typeSerializer = typeSerializer;
		this.availableMemory = availableMemory;
		this.ioManager = ioManager;
		this.queue = queue;
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

	public int flush(BlockChannelWriter<MemorySegment> flushedChannel) throws IOException {
		this.flushedChannel = flushedChannel;
		return this.writeBuffer.spill(flushedChannel);
	}

	public boolean isInMemory() {
		return this.flushedChannel == null;
	}

	public int getElementCount() {
		return this.elementCount;
	}

	public MutableObjectIterator<Pair<Integer, T>> getPartitionIterator() throws IOException {
		if (isInMemory()) {
			int finalBufferLimit = this.writeBuffer.getCurrentPositionInSegment();
			List<MemorySegment> availableSegments = this.writeBuffer.close();
			MemorySegment[] segments = availableSegments.toArray(new MemorySegment[availableSegments.size()]);
			final RadixReaderBuffer radixReaderBuffer = new RadixReaderBuffer(segments, finalBufferLimit);
			return new PartitionIterator(radixReaderBuffer, typeSerializer);
		} else {
			int currentBlockNumber = this.writeBuffer.getCurrentBlockNumber();
			int currentPossitionInSegment = this.writeBuffer.getCurrentPositionInSegment();
			this.writeBuffer.close();
			BlockChannelReader<MemorySegment> channelReader = this.ioManager.createBlockChannelReader(this
				.flushedChannel.getChannelID());

			final List<MemorySegment> segments = new ArrayList<>(2);
			segments.add(forceGetNextBuffer());
			segments.add(forceGetNextBuffer());
			final ChannelReaderInputView inView = new HeaderlessChannelReaderInputView(channelReader, segments,
				currentBlockNumber, currentPossitionInSegment, false);

			return new SpilledPartitionIterator(inView, typeSerializer);
		}
	}
	
	/**
	 * Clean all the data.
	 */
	public void reset() {
		close();
		this.writeBuffer = new RadixWriteBuffer(forceGetNextBuffer(), getMemSource());
	}

	/**
	 * Clean all the data and resources.
	 */
	public void close() {
		List<MemorySegment> segments = this.writeBuffer.close();
		if (segments != null) {
			for (MemorySegment segment : segments) {
				this.availableMemory.add(segment);
			}
		}
		this.elementCount = 0;
		this.minKey = Integer.MAX_VALUE;
		this.flushedChannel = null;
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
		MemorySegment segment = queue.getNextBuffer();
		if (segment == null) {
			try {
				queue.spillPartition();
				segment = queue.getNextBuffer();
			} catch (IOException e) {
				throw new RuntimeException("Failed to spill partition.");
				//TODO
			}

			if (segment == null) {
				throw new RuntimeException("Failed to get segment after spill partition.");
				//TODO
			}
		}

		return segment;
	}

	// ============================================================================================

	protected final class RadixWriteBuffer extends AbstractPagedOutputView {
		private final List<MemorySegment> targetList;
		private final MemorySegmentSource memSource;
		private BlockChannelWriter<MemorySegment> writer;
		private int currentBlockNumber;
		private boolean closed;

		private RadixWriteBuffer(MemorySegment initialSegment, MemorySegmentSource memSource) {
			super(initialSegment, initialSegment.size(), 0);

			this.targetList = new LinkedList<>();
			this.memSource = memSource;
			this.currentBlockNumber = 1;
		}

		@Override
		protected MemorySegment nextSegment(MemorySegment current, int bytesUsed) throws IOException {
			MemorySegment next = null;
			if (this.writer == null) {
				this.targetList.add(current);
				next = this.memSource.nextSegment();
			} else {
				this.writer.writeBlock(current);
				try {
					next = this.writer.getReturnQueue().take();
				} catch (InterruptedException e) {
					// TODO
					throw new IOException("failed to get return queue.");
				}
			}

			this.currentBlockNumber++;
			return next;
		}

		int spill(BlockChannelWriter<MemorySegment> writer) throws IOException {
			this.writer = writer;
			final int numSegments = this.targetList.size();
			for (int i = 0; i < numSegments; i++) {
				writer.writeBlock(this.targetList.get(i));
			}
			this.targetList.clear();
			return numSegments;
		}

		List<MemorySegment> close() {
			if (this.closed) {
				return null;
			}
			final MemorySegment current = getCurrentSegment();
			if (current == null) {
				throw new IllegalStateException("Illegal State in RadixPartition: No current buffer when finalizing " +
					"build side.");
			}
			if (this.writer == null) {
				this.targetList.add(current);
			} else {
				try {
					this.writer.writeBlock(current);
					this.targetList.clear();
					queue.addWriteBehindBuffersAvailable(1);
					this.writer.close();
				} catch (IOException e) {
					// TODO
					throw new RuntimeException("Failed to write block.");
				}
			}
			clear();
			this.closed = true;
			return this.targetList;
		}
		
		int getCurrentBlockNumber() {
			return this.currentBlockNumber;
		}
	}

	protected final class RadixReaderBuffer extends AbstractPagedInputView implements SeekableDataInputView {
		private final MemorySegment[] memorySegments;
		private int currentBufferIndex = 1;
		private int finalBufferLimit;
		private int segmentSizeBits;
		private int memorySegmentSize;
		private boolean closed = false;

		private RadixReaderBuffer(MemorySegment[] segments, int finalBufferLimit) {
			super(0);
			MemorySegment[] buffers = segments;
			this.memorySegments = buffers;
			this.finalBufferLimit = finalBufferLimit;
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

		public boolean isClosed() {
			return this.closed;
		}

		public MemorySegment[] close() {
			this.closed = true;
			return this.memorySegments;
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
				if (!this.readerBuffer.isClosed()) {
					MemorySegment[] segments = this.readerBuffer.close();
					if (segments != null) {
						for (MemorySegment segment : segments) {
							availableMemory.add(segment);
						}
					}
				}
				return null;
			}
		}

		public final void release() {
			if (!this.readerBuffer.isClosed()) {
				MemorySegment[] segments = this.readerBuffer.close();
				if (segments != null) {
					for (MemorySegment segment : segments) {
						availableMemory.add(segment);
					}
				}
			}
		}
	}

	final class SpilledPartitionIterator<T> implements MutableObjectIterator<Pair<Integer, T>> {

		private final ChannelReaderInputView inView;
		private final TypeSerializer<T> typeSerializer;

		private SpilledPartitionIterator(ChannelReaderInputView inView, TypeSerializer<T> typeSerializer) {
			this.inView = inView;
			this.typeSerializer = typeSerializer;
		}

		@Override
		public Pair<Integer, T> next(Pair<Integer, T> reuse) throws IOException {
			return next();
		}

		@Override
		public Pair<Integer, T> next() throws IOException {
			try {
				int key = this.inView.readInt();
				T result = this.typeSerializer.deserialize(this.inView);
				return Pair.of(key, result);
			} catch (EOFException eofex) {
				if (!this.inView.isClosed()) {
					final List<MemorySegment> freeMem = this.inView.close();
					if (freeMem != null) {
						availableMemory.addAll(freeMem);
					}
				}
				return null;
			}
		}
		public final void release() {
			if (!this.inView.isClosed()) {
				final List<MemorySegment> freeMem;
				try {
					freeMem = this.inView.close();
					if (freeMem != null) {
						availableMemory.addAll(freeMem);
					}
				} catch (IOException e) {
					e.printStackTrace();
				}
			}
		}

	}
}
