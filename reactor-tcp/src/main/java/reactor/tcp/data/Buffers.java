/*
 * Copyright 2002-2013 the original author or authors.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package reactor.tcp.data;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import reactor.support.Assert;

import java.io.IOException;
import java.io.InputStream;
import java.nio.ByteBuffer;
import java.nio.channels.ReadableByteChannel;
import java.util.Collections;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.concurrent.locks.Condition;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

/**
 * A collection of buffers.
 *
 * @author Gary Russell
 */
public final class Buffers implements Iterable<ByteBuffer> {

	private final Logger logger = LoggerFactory.getLogger(getClass());

	final List<ByteBuffer> buffers = new LinkedList<ByteBuffer>();

	private volatile int epoch;

	private final Lock lock = new ReentrantLock();

	private final Condition newBuffersAvailable;

	private volatile int position;

	private final BuffersByteIterator byteIterator = new BuffersByteIterator();


	public Buffers() {
		this.newBuffersAvailable = this.lock.newCondition();
	}

	public int getPosition() {
		return position;
	}


	public void add(ByteBuffer e) {
		this.lock.lock();
		try {
			buffers.add(e);
			if (logger.isDebugEnabled()) {
				logger.debug("Buffer added, now:" + this.toString());
			}
			this.newBuffersAvailable.signalAll();
		} finally {
			this.lock.unlock();
		}
	}

	public ByteBuffer get(int index) {
		return buffers.get(index).duplicate();
	}

	public void discardBytes(int bytesConsumed) {
		this.epoch++;
		int bytesToConsume = bytesConsumed;
		while (bytesToConsume > 0) {
			ByteBuffer buffer = this.buffers.get(0);
			int remaining = buffer.remaining();
			if (remaining > bytesToConsume) {
				buffer.position(buffer.position() + bytesToConsume);
				break;
			}
			this.buffers.remove(0);
			bytesToConsume -= remaining;
		}
		this.position = 0;
		this.byteIterator.reset();
		if (logger.isDebugEnabled()) {
			logger.debug("Discarded " + bytesConsumed + " bytes; now:" + this.toString());
		}
	}

	public void discardBuffers(int count) {
		this.epoch++;
		for (int i = 0; i < count; i++) {
			this.buffers.remove(0);
		}
		this.position = 0;
		this.byteIterator.reset();
		if (logger.isDebugEnabled()) {
			logger.debug("Discarded " + count + " buffers; now:" + this.toString());
		}
	}

	public int getBufferCount() {
		return buffers.size();
	}

	public int getSize() {
		int total = 0;
		for (ByteBuffer buffer : this.buffers) {
			total += buffer.remaining();
		}
		return total;
	}

	@Override
	public Iterator<ByteBuffer> iterator() {
		return Collections.unmodifiableCollection(buffers).iterator();
	}

	public boolean hasNext() {
		return this.byteIterator.hasNext();
	}

	public byte next() {
		if (this.byteIterator.hasNext()) {
			position++;
			return this.byteIterator.next();
		} else {
			throw new IllegalStateException("No more data");
		}
	}

	public InputStream getInputStream() {
		return new BuffersInputStream(epoch);
	}

	public ReadableByteChannel getReadableByteChannel() {
		return new BuffersReadableByteChannel(epoch);
	}

	@Override
	public String toString() {
		return "[buffers = " + this.buffers.size() + "; position=" +
				this.getPosition() + "; totalLength= " + this.getSize() + "]";
	}

	private class ByteFeeder {

		private volatile int currentBuffer;

		private volatile ByteBuffer bufferView;

		protected boolean isExhausted() {
			if (this.bufferView == null && buffers.size() > 0) {
				this.bufferView = buffers.get(0).duplicate();
			}
			return this.bufferView == null ||
					(this.currentBuffer == buffers.size() - 1 && this.bufferView.remaining() == 0);
		}

		protected int nextByte() {
			if (this.bufferView.remaining() == 0) {
				Assert.isTrue(buffers.size() > this.currentBuffer + 1, "No more buffers");
				ByteBuffer nextBuffer = buffers.get(++this.currentBuffer);
				this.bufferView = nextBuffer.duplicate();
			}
			byte bite = this.bufferView.get();
			return bite;
		}

		void reset() {
			this.currentBuffer = 0;
			this.bufferView = null;
		}

	}

	private class BuffersByteIterator extends ByteFeeder implements ByteIterator {

		@Override
		public boolean hasNext() {
			return !this.isExhausted();
		}

		@Override
		public byte next() {
			Assert.isTrue(!this.isExhausted(), "No more bytes");
			int bite = nextByte();
			return (byte) bite;
		}

	}

	private class BuffersInputStream extends InputStream {

		private final int originalEpoch;

		private final ByteFeeder feeder = new ByteFeeder();

		private volatile int bytesRead;

		public BuffersInputStream(int epoch) {
			this.originalEpoch = epoch;
		}

		@Override
		public int read() throws IOException {
			lock.lock();
			try {
				while (feeder.isExhausted()) {
					// TODO: Add socket timeout
					try {
						newBuffersAvailable.await();
					} catch (InterruptedException e) {
						Thread.currentThread().interrupt();
						throw new IOException(e);
					}
				}
			} finally {
				lock.unlock();
			}

			if (this.originalEpoch != epoch) {
				throw new IOException("InputStream has been reset");
			}
			bytesRead++;
			return this.feeder.nextByte();
		}

		@Override
		public void close() throws IOException {
			discardBytes(this.bytesRead);
		}

	}

	private class BuffersReadableByteChannel implements ReadableByteChannel {

		private final ByteFeeder feeder = new ByteFeeder();
		private final    int originalEpoch;
		private volatile int bytesRead;

		private BuffersReadableByteChannel(int originalEpoch) {
			this.originalEpoch = originalEpoch;
		}

		@Override
		public int read(ByteBuffer dst) throws IOException {
			if (this.originalEpoch != epoch) {
				throw new IOException("InputStream has been reset");
			}

			int start = bytesRead;
			int avail = dst.remaining();
			for (int i = 0; i < avail; i++) {
				if (feeder.isExhausted()) {
					break;
				}
				int b = feeder.nextByte();
				bytesRead++;
				dst.put((byte) b);
			}

			return bytesRead - start;
		}

		@Override
		public boolean isOpen() {
			return hasNext();
		}

		@Override
		public void close() throws IOException {
			discardBytes(this.bytesRead);
		}

	}

}

