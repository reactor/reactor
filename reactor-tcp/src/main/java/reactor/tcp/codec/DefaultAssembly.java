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
package reactor.tcp.codec;

import java.io.InputStream;
import java.io.UnsupportedEncodingException;
import java.nio.ByteBuffer;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.NoSuchElementException;

import reactor.tcp.data.Buffers;
import reactor.tcp.data.ByteIterator;

/**
 * Implementation of {@link Assembly} that represents a series of
 * received buffers as an 'assembled' object. When created, it
 * consumes the data it represents from the {@link Buffers} object
 * without copying the data. {@link DefaultAssembly#asBytes} is
 * cached once accessed.
 * @author Gary Russell
 *
 */
public class DefaultAssembly implements Assembly {

	private final List<ByteBuffer> buffers = new LinkedList<ByteBuffer>();

	private final int length;

	private byte[] asBytes;

	/**
	 * Extracts buffers from pending buffers. Removes fully consumed buffers
	 * and sets the new offset in the pending buffers.
	 *
	 * @param pendingBuffers The pending buffers containing the assembled data
	 * @param assemblyLength The number of bytes in the assembly
	 */
	public DefaultAssembly(Buffers pendingBuffers, int assemblyLength) {
		this(pendingBuffers, 0, assemblyLength, 0);
	}

	/**
	 * Extracts buffers from pending buffers. Removes fully consumed buffers
	 * and sets the new offset in the pending buffers.
	 *
	 * @param pendingBuffers The pending buffers containing the assembled data
	 * @param assemblyLength The number of bytes in the assembly
	 * @param trailingDiscard The number of bytes at the end to skip
	 */
	public DefaultAssembly(Buffers pendingBuffers, int assemblyLength, int trailingDiscard) {
		this(pendingBuffers, 0, assemblyLength, trailingDiscard);
	}

	/**
	 * Extracts buffers from pending buffers. Removes fully consumed buffers
	 * and sets the new offset in the pending buffers.
	 *
	 * @param pendingBuffers The pending buffers containing the assembled data
	 * @param leadingDiscard The number of bytes at the start to skip
	 * @param assemblyLength The number of bytes to includes in the assembly
	 * @param trailingDiscard The number of bytes at the end to skip
	 */
	public DefaultAssembly(Buffers pendingBuffers, int leadingDiscard, int assemblyLength, int trailingDiscard) {
		pendingBuffers.discardBytes(leadingDiscard);
		ByteBuffer buffer = pendingBuffers.get(0);
		this.buffers.add(buffer);
		int remainingFirstBuffer = buffer.remaining();
		int bytesNeeded = assemblyLength - remainingFirstBuffer;
		if (bytesNeeded > 0) {
			int n = 1;
			while (bytesNeeded > 0) {
				buffer = pendingBuffers.get(n++);
				this.buffers.add(buffer);
				bytesNeeded -= buffer.remaining();
			}
		}
		pendingBuffers.discardBytes(assemblyLength + trailingDiscard);
		this.length = assemblyLength;
	}

	@Override
	public InputStream asInputStream() {
		throw new UnsupportedOperationException("Not yet implements"); // TODO
	}

	@Override
	public final byte[] asBytes() {
		if (this.asBytes != null) {
			return this.asBytes;
		}
		byte[] bytes = new byte[this.length];
		Iterator<ByteBuffer> iterator = this.buffers.iterator();
		int remaining = this.length;
		int bytesToCopy;
		int curPos = 0;
		while (iterator.hasNext() && remaining > 0) {
			ByteBuffer buff = iterator.next().duplicate();
			bytesToCopy = Math.min(remaining, buff.remaining());
			remaining -= bytesToCopy;
			buff.get(bytes, curPos, bytesToCopy);
			curPos += bytesToCopy;
		}
		this.asBytes = bytes;
		return bytes;
	}

	@Override
	public final ByteIterator iterator() {
		return new DefaultByteIterator();
	}

	@Override
	public String toString() {
		try {
			return new String(this.asBytes(), "UTF-8");
		}
		catch (UnsupportedEncodingException e) {
			return new String(this.asBytes());
		}
	}



	private class DefaultByteIterator implements ByteIterator {

		private volatile int currentBuffer;

		private volatile ByteBuffer bufferView;

		private volatile int remaining;

		private DefaultByteIterator() {
			if (buffers.size() > 0) {
				this.bufferView = buffers.get(0).duplicate();
			}
			this.remaining = length;
		}

		@Override
		public boolean hasNext() {
			return this.remaining > 0;
		}

		@Override
		public byte next() {
			if (!hasNext()) {
				throw new NoSuchElementException();
			}
			if (this.bufferView.remaining() == 0) {
				this.bufferView = buffers.get(++this.currentBuffer).duplicate();
			}
			this.remaining--;
			return this.bufferView.get();
		}

	}
}
