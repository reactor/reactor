/*
 * Copyright (c) 2011-2013 the original author or authors.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *       http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package reactor.io;

import reactor.util.Assert;

import java.io.IOException;
import java.io.InputStream;
import java.nio.BufferOverflowException;
import java.nio.BufferUnderflowException;
import java.nio.ByteBuffer;
import java.nio.channels.ReadableByteChannel;
import java.nio.channels.WritableByteChannel;
import java.nio.charset.CharacterCodingException;
import java.nio.charset.Charset;
import java.nio.charset.CharsetDecoder;
import java.util.*;

/**
 * @author Jon Brisbin <jon@jbrisbin.com>
 */
public class Buffer implements Comparable<Buffer>,
															 Iterable<Byte>,
															 ReadableByteChannel,
															 WritableByteChannel {

	public static int SMALL_BUFFER_SIZE = Integer.parseInt(
			System.getProperty("reactor.io.defaultBufferSize", "" + 1024 * 16)
	);
	public static int MAX_BUFFER_SIZE   = Integer.parseInt(
			System.getProperty("reactor.io.maxBufferSize", "" + 1024 * 1000 * 16)
	);

	private static final Charset UTF8 = Charset.forName("UTF-8");
	private       CharsetDecoder charDecoder;
	private final boolean        dynamic;
	private       ByteBuffer     buffer;

	public Buffer() {
		this.dynamic = true;
	}

	public Buffer(int atLeast, boolean fixed) {
		if (fixed) {
			if (atLeast <= MAX_BUFFER_SIZE) {
				this.buffer = ByteBuffer.allocate(atLeast);
			} else {
				throw new IllegalArgumentException("Requested buffer size exceeds maximum allowed (" + MAX_BUFFER_SIZE + ")");
			}
		} else {
			ensureCapacity(atLeast);
		}
		this.dynamic = !fixed;
	}

	public Buffer(Buffer bufferToCopy) {
		this.dynamic = bufferToCopy.dynamic;
		this.buffer = bufferToCopy.buffer.duplicate();
	}

	public Buffer(ByteBuffer bufferToStartWith) {
		this.dynamic = true;
		this.buffer = bufferToStartWith;
	}

	public static Buffer wrap(byte[] bytes) {
		return new Buffer(bytes.length, true)
				.append(bytes)
				.flip();
	}

	public static Buffer wrap(String str, boolean fixed) {
		return new Buffer(str.length(), fixed)
				.append(str)
				.flip();
	}

	public static Buffer wrap(String str) {
		return wrap(str, true);
	}

	public static Integer parseInt(Buffer b, int start, int end) {
		int origPos = b.buffer.position();
		int origLimit = b.buffer.limit();

		b.buffer.position(start);
		b.buffer.limit(end);

		Integer i = parseInt(b);

		b.buffer.position(origPos);
		b.buffer.limit(origLimit);

		return i;
	}

	public static Integer parseInt(Buffer b) {
		if (b.remaining() == 0) {
			return null;
		}
		ByteBuffer bb = b.buffer;
		int origPos = bb.position();
		int len = bb.remaining();

		int num = 0;
		int dec = 1;
		for (int i = len; i > 0; ) {
			char c = (char) bb.get(--i);
			num += Character.getNumericValue(c) * dec;
			dec *= 10;
		}

		bb.position(origPos);

		return num;
	}

	public static Long parseLong(Buffer b, int start, int end) {
		int origPos = b.buffer.position();
		int origLimit = b.buffer.limit();

		b.buffer.position(start);
		b.buffer.limit(end);

		Long l = parseLong(b);

		b.buffer.position(origPos);
		b.buffer.limit(origLimit);

		return l;
	}

	public static Long parseLong(Buffer b) {
		if (b.remaining() == 0) {
			return null;
		}
		ByteBuffer bb = b.buffer;
		int origPos = bb.position();
		int len = bb.remaining();

		long num = 0;
		int dec = 1;
		for (int i = len; i > 0; ) {
			char c = (char) bb.get(--i);
			num += Character.getNumericValue(c) * dec;
			dec *= 10;
		}

		bb.position(origPos);

		return num;
	}

	public boolean isDynamic() {
		return dynamic;
	}

	public int position() {
		return (null == buffer ? 0 : buffer.position());
	}

	public int limit() {
		return (null == buffer ? 0 : buffer.limit());
	}

	public int capacity() {
		return (null == buffer ? SMALL_BUFFER_SIZE : buffer.capacity());
	}

	public int remaining() {
		return (null == buffer ? SMALL_BUFFER_SIZE : buffer.remaining());
	}

	public Buffer clear() {
		if (null != buffer) {
			buffer.position(0);
			buffer.limit(buffer.capacity());
		}
		return this;
	}

	public Buffer flip() {
		if (null != buffer) {
			buffer.flip();
		}
		return this;
	}

	public Buffer rewind() {
		if (null != buffer) {
			buffer.rewind();
		}
		return this;
	}

	public Buffer prepend(Buffer b) {
		if (null == b) {
			return this;
		}
		return prepend(b.buffer);
	}

	public Buffer prepend(byte b) {
		ByteBuffer currentBuffer = buffer.duplicate();
		ensureCapacity(1 + currentBuffer.remaining());
		this.buffer.put(b);
		this.buffer.put(currentBuffer);
		return this;
	}

	public Buffer prepend(byte[] bytes) {
		ByteBuffer currentBuffer = buffer.duplicate();
		ensureCapacity(bytes.length + currentBuffer.remaining());
		this.buffer.put(bytes);
		this.buffer.put(currentBuffer);
		return this;
	}

	public Buffer prepend(ByteBuffer b) {
		if (null == b) {
			return this;
		}
		ByteBuffer currentBuffer = buffer.duplicate();
		ensureCapacity(b.remaining() + currentBuffer.remaining());
		this.buffer.put(b);
		this.buffer.put(currentBuffer);
		return this;

	}

	public Buffer prepend(char c) {
		ByteBuffer currentBuffer = buffer.duplicate();
		ensureCapacity(1 + currentBuffer.remaining());
		this.buffer.putChar(c);
		this.buffer.put(currentBuffer);
		return this;
	}

	public Buffer prepend(int i) {
		ByteBuffer currentBuffer = buffer.duplicate();
		ensureCapacity(4 + currentBuffer.remaining());
		this.buffer.putInt(i);
		this.buffer.put(currentBuffer);
		return this;
	}

	public Buffer prepend(long l) {
		ByteBuffer currentBuffer = buffer.duplicate();
		ensureCapacity(8 + currentBuffer.remaining());
		this.buffer.putLong(l);
		this.buffer.put(currentBuffer);
		return this;
	}

	public Buffer prepend(String s) {
		if (null == s) {
			return this;
		}
		ByteBuffer currentBuffer = buffer.duplicate();
		ensureCapacity(s.length() + currentBuffer.remaining());
		this.buffer.put(s.getBytes());
		this.buffer.put(currentBuffer);
		return this;
	}

	public Buffer append(String s) {
		ensureCapacity(s.length());
		buffer.put(s.getBytes());
		return this;
	}

	public Buffer append(int i) {
		ensureCapacity(4);
		buffer.putInt(i);
		return this;
	}

	public Buffer append(long l) {
		ensureCapacity(8);
		buffer.putLong(l);
		return this;
	}

	public Buffer append(char c) {
		ensureCapacity(1);
		buffer.putChar(c);
		return this;
	}

	public Buffer append(ByteBuffer b) {
		ensureCapacity(b.remaining());
		buffer.put(b);
		return this;
	}

	public Buffer append(Buffer b) {
		int pos = (null == buffer ? 0 : buffer.position());
		int len = b.remaining();
		ensureCapacity(len);
		buffer.put(b.byteBuffer());
		buffer.position(pos + len);
		return this;
	}

	public Buffer append(byte b) {
		ensureCapacity(1);
		buffer.put(b);
		return this;
	}

	public Buffer append(byte[] b) {
		ensureCapacity(b.length);
		buffer.put(b);
		return this;
	}

	public byte first() {
		int pos = buffer.position();
		if (pos > 0) {
			buffer.position(0); // got to the 1st position
		}
		byte b = buffer.get(); // get the 1st byte
		buffer.position(pos); // go back to original pos
		return b;
	}

	public byte last() {
		int pos = buffer.position();
		int limit = buffer.limit();
		buffer.position(limit - 1); // go to right before last position
		byte b = buffer.get(); // get the last byte
		buffer.position(pos); // go back to original pos
		return b;
	}

	public byte read() {
		if (null != buffer) {
			return buffer.get();
		}
		throw new BufferUnderflowException();
	}

	public Buffer read(byte[] b) {
		if (null != buffer) {
			buffer.get(b);
		}
		return this;
	}

	public int readInt() {
		if (null != buffer) {
			return buffer.getInt();
		}
		throw new BufferUnderflowException();
	}

	public float readFloat() {
		if (null != buffer) {
			return buffer.getFloat();
		}
		throw new BufferUnderflowException();
	}

	public double readDouble() {
		if (null != buffer) {
			return buffer.getDouble();
		}
		throw new BufferUnderflowException();
	}

	public long readLong() {
		if (null != buffer) {
			return buffer.getLong();
		}
		throw new BufferUnderflowException();
	}

	public char readChar() {
		if (null != buffer) {
			return buffer.getChar();
		}
		throw new BufferUnderflowException();
	}

	@Override
	public Iterator<Byte> iterator() {
		return new Iterator<Byte>() {
			@Override
			public boolean hasNext() {
				return buffer.remaining() > 0;
			}

			@Override
			public Byte next() {
				return buffer.get();
			}

			@Override
			public void remove() {
				// NO-OP
			}
		};
	}

	@Override
	public int read(ByteBuffer dst) throws IOException {
		int pos = dst.position();
		dst.put(buffer);
		return dst.position() - pos;
	}

	@Override
	public int write(ByteBuffer src) throws IOException {
		int pos = src.position();
		append(src);
		return src.position() - pos;
	}

	@Override
	public boolean isOpen() {
		return isDynamic();
	}

	@Override
	public void close() throws IOException {
		clear();
	}

	public String asString() {
		if (null != buffer) {
			return decode();
		} else {
			return null;
		}
	}

	public byte[] asBytes() {
		if (null != buffer) {
			int origPos = buffer.position();
			byte[] b = new byte[buffer.remaining()];
			buffer.get(b);
			buffer.position(origPos);
			return b;
		} else {
			return null;
		}
	}

	public InputStream inputStream() {
		return new BufferInputStream();
	}

	public Buffer slice(int start, int len) {
		int pos = buffer.position();
		ByteBuffer bb = ByteBuffer.allocate(len);
		byte[] bytes = new byte[len];
		buffer.position(start);
		bb.put(buffer);
		buffer.position(pos);
		bb.flip();
		return new Buffer(bb);
	}

	public Iterable<Buffer> split(int delimiter) {
		return split(delimiter, false);
	}

	public Iterable<Buffer> split(int delimiter, boolean stripDelimiter) {
		int origPos = buffer.position();
		int origLimit = buffer.limit();

		List<Integer> positions = new ArrayList<Integer>();
		for (byte b : this) {
			if (b == delimiter) {
				positions.add((stripDelimiter ? buffer.position() - 1 : buffer.position()));
			}
		}
		int end = buffer.position();

		List<Buffer> buffers = new ArrayList<Buffer>(positions.size());
		int start = 0;
		if (!positions.isEmpty()) {
			for (Integer pos : positions) {
				buffer.limit(pos);
				buffer.position(start);
				ByteBuffer bb = buffer.duplicate();
				buffers.add(new Buffer(bb));
				start = (stripDelimiter ? pos + 1 : pos);
			}
		}

		if (buffer.position() + 1 < end) {
			buffer.limit(end);
			buffer.position(start);
			buffers.add(new Buffer(buffer.duplicate()));
		}

		buffer.limit(origLimit);
		buffer.position(origPos);

		return buffers;
	}

	public Iterable<Buffer> slice(Collection<Integer> positions) {
		Assert.notNull(positions, "Positions cannot be null.");
		if (positions.isEmpty()) {
			return Collections.emptyList();
		}

		int origPos = buffer.position();
		int origLimit = buffer.limit();

		List<Buffer> buffers = new ArrayList<Buffer>();
		Iterator<Integer> ipos = positions.iterator();
		while (ipos.hasNext()) {
			Integer start = ipos.next();
			Integer end = (ipos.hasNext() ? ipos.next() : null);
			buffer.position(start);
			if (null == end) {
				buffer.limit(origLimit);
			} else {
				buffer.limit(end);
			}
			buffers.add(new Buffer(buffer.duplicate()));
			buffer.limit(origLimit);
		}

		buffer.position(origPos);
		buffer.limit(origLimit);

		return buffers;
	}

	public ByteBuffer byteBuffer() {
		return buffer;
	}

	@Override
	public String toString() {
		return (null != buffer ? buffer.toString() : "<EMPTY>");
	}

	@Override
	public int compareTo(Buffer buffer) {
		return (null != buffer ? this.buffer.compareTo(buffer.buffer) : -1);
	}

	private void ensureCapacity(int atLeast) {
		if (null == buffer) {
			buffer = ByteBuffer.allocate(SMALL_BUFFER_SIZE);
			return;
		}
		int pos = buffer.position();
		int cap = buffer.capacity();
		if (dynamic && buffer.remaining() < atLeast) {
			if (buffer.limit() < cap) {
				if (pos + atLeast > cap) {
					expand();
				} else {
					buffer.limit(Math.min(pos + atLeast, cap));
				}
			}
		} else if (pos + SMALL_BUFFER_SIZE > MAX_BUFFER_SIZE) {
			throw new BufferOverflowException();
		}
	}

	private void expand() {
		int pos = buffer.position();
		ByteBuffer newBuff = ByteBuffer.allocate(buffer.limit() + SMALL_BUFFER_SIZE);
		buffer.flip();
		newBuff.put(buffer);
		buffer = newBuff;
		buffer.position(pos);
	}

	private String decode() {
		if (null == charDecoder) {
			charDecoder = UTF8.newDecoder();
		}
		int origPos = buffer.position();
		try {
			return charDecoder.decode(buffer).toString();
		} catch (CharacterCodingException e) {
			throw new IllegalStateException(e);
		} finally {
			buffer.position(origPos);
		}
	}

	private class BufferInputStream extends InputStream {
		ByteBuffer buffer = Buffer.this.buffer.duplicate();

		@Override
		public int read(byte[] b) throws IOException {
			int pos = buffer.position();
			buffer.get(b);
			return buffer.position() - pos;
		}

		@Override
		public int read(byte[] b, int off, int len) throws IOException {
			if (null == buffer) {
				return -1;
			}
			if ((off + len) > buffer.limit()) {
				throw new BufferUnderflowException();
			}

			buffer.position(off);
			for (int i = 0; i < len; i++) {
				if (buffer.remaining() == 0) {
					break;
				}
				b[i] = buffer.get();
			}

			return buffer.position() - off;
		}

		@Override
		public long skip(long n) throws IOException {
			if (n < buffer.remaining()) {
				throw new BufferUnderflowException();
			}
			int pos = buffer.position();
			buffer.position((int) (pos + n));
			return buffer.position() - pos;
		}

		@Override
		public int available() throws IOException {
			return buffer.remaining();
		}

		@Override
		public void close() throws IOException {
			buffer.position(buffer.limit());
		}

		@Override
		public synchronized void mark(int readlimit) {
			buffer.mark();
			int pos = buffer.position();
			int max = buffer.capacity() - pos;
			int newLimit = Math.min(max, pos + readlimit);
			buffer.limit(newLimit);
		}

		@Override
		public synchronized void reset() throws IOException {
			buffer.reset();
		}

		@Override
		public boolean markSupported() {
			return true;
		}

		@Override
		public int read() throws IOException {
			return buffer.get();
		}
	}

}
