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
import com.google.common.collect.Lists;
import org.apache.flink.api.common.typeutils.TypeComparator;
import org.apache.flink.api.common.typeutils.TypeSerializer;
import org.apache.flink.core.memory.MemorySegment;
import org.apache.flink.runtime.io.disk.iomanager.BlockChannelReader;
import org.apache.flink.runtime.io.disk.iomanager.BlockChannelWriter;
import org.apache.flink.runtime.io.disk.iomanager.ChannelReaderInputView;
import org.apache.flink.runtime.io.disk.iomanager.ChannelWriterOutputView;
import org.apache.flink.runtime.io.disk.iomanager.FileIOChannel;
import org.apache.flink.runtime.io.disk.iomanager.IOManager;

import java.io.EOFException;
import java.io.File;
import java.io.IOException;
import java.util.Deque;
import java.util.List;
import java.util.Random;

public class FixedLengthQuickSelector<T> implements QuickSelector<T>{

	private final List<MemorySegment> availableMemories;
	private final TypeSerializer<T> typeSerializer;
	private final TypeComparator<T> typeComparator;
	private final IOManager ioManager;
	private Deque<Pivot<T>> pivots;
	private FixedLengthRecordSorter<T> inMemorySorter;
	private List<MemorySegment> spillBuffer;
	private T spillPivot;
	private int totalCount;
	private Random random;
	private int skipped;
	private int segmentSize;

	private BlockChannelWriter<MemorySegment> blockChannelWriter;

	private ChannelWriterOutputView outputView;

	protected FileIOChannel.Enumerator currentEnumerator;

	public FixedLengthQuickSelector(List<MemorySegment> availableMemories, TypeSerializer<T> typeSerializer, TypeComparator<T> typeComparator, IOManager ioManager) {
		this.availableMemories = availableMemories;
		this.typeSerializer = typeSerializer;
		this.typeComparator = typeComparator;
		this.ioManager = ioManager;
		this.currentEnumerator = this.ioManager.createChannelEnumerator();
		this.segmentSize = this.availableMemories.get(0).size();
		this.spillBuffer = Lists.newArrayList();
		this.spillBuffer.add(this.availableMemories.remove(0));
		this.spillBuffer.add(this.availableMemories.remove(0));
		this.spillBuffer.add(this.availableMemories.remove(0));
		this.spillBuffer.add(this.availableMemories.remove(0));
		pivots = Lists.newLinkedList();
		random = new Random();
		this.inMemorySorter = new FixedLengthRecordSorter<>(typeSerializer, typeComparator, availableMemories);

		this.skipped = 0;
		this.totalCount = 0;
	}

	@Override
	public void insert(T value) throws IOException {
		put(value);
		totalCount++;
	}

	private boolean isInMemory() {
		return this.outputView == null;
	}

	private void put(T value) throws IOException {
		if (isInMemory()) {
			putOnMemory(value);
		} else {
			if (this.typeComparator.compare(value, spillPivot) < 0) {
				putOnMemory(value);
			} else {
				this.typeSerializer.serialize(value, outputView);
			}
		}
	}

	private void putOnMemory(T value) throws IOException {
		boolean success = this.inMemorySorter.write(value);
		if (!success) {
			flush();
			success = this.inMemorySorter.write(value);
			if (!success) {
				throw new IOException("Flush did not recycle any memory.");
			}
		}
		this.pivots.clear();
	}

	@Override
	public T next() throws IOException {
		if (totalCount == 0) {
			return null;
		}

		if (pivots.isEmpty()) {
			if (this.inMemorySorter.size() != skipped) {
				quickSelect();
			}
		} else {
			Pivot<T> last = pivots.peekLast();
			if (last.getIndex() != 0) {
				quickSelect(pivots.peekLast().getIndex());
			}
		}

		if (pivots.isEmpty()) {
			if (!isInMemory()) {
				reload();
				quickSelect();
			}
		}

		Pivot<T> last = pivots.pollLast();
		for (Pivot<T> pivot : pivots) {
			pivot.decrementIndex();
		}
		this.totalCount--;
		this.skipped++;
		return last.getRecord();
	}

	@Override
	public int size() {
		return this.totalCount;
	}

	@Override
	public List<MemorySegment> close() {
		List<MemorySegment> dispose = this.inMemorySorter.dispose();
		dispose.addAll(this.spillBuffer);
		if (!isInMemory()) {
			try {
				dispose.addAll(this.outputView.close());
			} catch (IOException e) {
				throw new RuntimeException("Failed to close output.");
			}
		}
		return dispose;
	}

	private void quickSelect() throws IOException {
		quickSelect(this.inMemorySorter.size() - skipped);
	}

	private void quickSelect(int index) throws IOException {
		int pivotIndex = splitAtPivot(index);
		T record = this.typeSerializer.createInstance();
		record = inMemorySorter.getRecord(record, getRealIndex(pivotIndex));
		pivots.addLast(new Pivot<>(pivotIndex, record));
		while (pivotIndex > 1) {
			pivotIndex = splitAtPivot(pivotIndex);
			record = this.typeSerializer.createInstance();
			record = inMemorySorter.getRecord(record, getRealIndex(pivotIndex));
			pivots.addLast(new Pivot<>(pivotIndex, record));
		}
		if (pivotIndex == 1) {
			record = this.typeSerializer.createInstance();
			record = inMemorySorter.getRecord(record, getRealIndex(0));
			pivots.addLast(new Pivot<>(0, record));
		}
	}

	private int splitAtPivot(int count) throws IOException {
		Preconditions.checkArgument(count > 0, "Element number should be positive.");
		if (count == 1) {
			return 0;
		}
		int randomIndex = random.nextInt(count);
		T pivot = this.typeSerializer.createInstance();
		pivot = inMemorySorter.getRecord(pivot, getRealIndex(randomIndex));
		if (randomIndex != count - 1) {
			inMemorySorter.swap(getRealIndex(randomIndex), getRealIndex(count - 1));
		}
		int i = 0, j = count - 2;
		T record1 = this.typeSerializer.createInstance();
		T record2 = this.typeSerializer.createInstance();
		while (true) {
			record1 = inMemorySorter.getRecord(record1, getRealIndex(i));
			while (this.typeComparator.compare(record1, pivot) < 0 && i < count - 1) {
				i++;
				record1 = inMemorySorter.getRecord(record1, getRealIndex(i));
			}

			record2 = inMemorySorter.getRecord(record2, getRealIndex(j));
			while (this.typeComparator.compare(pivot, record2) < 0 && j > i) {
				j--;
				record2 = inMemorySorter.getRecord(record2, getRealIndex(j));
			}

			if (i < j) {
				inMemorySorter.swap(getRealIndex(i), getRealIndex(j));
			} else {
				break;
			}
			i++;
			j--;
		}
		if (i < count - 1) {
			inMemorySorter.swap(getRealIndex(i), getRealIndex(count - 1));
		}
		return i;
	}

	private void flush() throws IOException {

		if (pivots.isEmpty()) {
			quickSelect();
		}

		Pivot<T> firstPivot = pivots.pollFirst();
		int index = firstPivot.getIndex();
		this.spillPivot = firstPivot.getRecord();

		if (outputView == null) {
			this.blockChannelWriter = this.ioManager.createBlockChannelWriter(this.currentEnumerator.next());
			this.outputView = new ChannelWriterOutputView(blockChannelWriter, getBufferForIO(2), this.segmentSize);
		}

		this.inMemorySorter.flushToOutput(outputView, index);
	}

	private void reload() throws IOException {
		releaseBufferForIO(this.outputView.close());
		FileIOChannel.ID channelID = this.blockChannelWriter.getChannelID();

		this.blockChannelWriter = null;
		this.outputView = null;

		BlockChannelReader<MemorySegment> channelReader = this.ioManager.createBlockChannelReader(channelID);
		final ChannelReaderInputView inView = new ChannelReaderInputView(channelReader, getBufferForIO(2), false);
		T reuse = this.typeSerializer.createInstance();
		this.typeSerializer.deserialize(reuse, inView);
		this.inMemorySorter.reset();
		this.skipped = 0;
		while (reuse != null) {
			insert(reuse);
			try {
				this.typeSerializer.deserialize(reuse, inView);
			} catch (EOFException e) {
				// Reach the end.
				reuse = null;
			}
		}

		releaseBufferForIO(inView.close());

		try {
			final File f = new File(channelID.getPath());
			if (f.exists()) {
				f.delete();
			}
		} catch (Throwable t) {
		}
	}

	private List<MemorySegment> getBufferForIO(int index) {
		List<MemorySegment> buffers = Lists.newArrayList();
		for (int i = 0; i < index; i++) {
			buffers.add(this.spillBuffer.remove(0));
		}
		return buffers;
	}

	private void releaseBufferForIO(List<MemorySegment> buffer) {
		this.spillBuffer.addAll(buffer);
	}

	private int getRealIndex(int index) {
		return this.skipped + index;
	}

	class Pivot<T> {
		private int index;
		private T record;

		public Pivot(int index, T record) {
			this.index = index;
			this.record = record;
		}

		public int getIndex() {
			return index;
		}

		public T getRecord() {
			return record;
		}

		public void decrementIndex() {
			index--;
		}

		@Override
		public String toString() {
			return "Index[" + index + "], Record[" + record + "].";
		}
	}
}
