/*
 * Copyright (c) 2011-2013 GoPivotal, Inc. All Rights Reserved.
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

import reactor.function.Supplier;
import reactor.util.Assert;

import javax.annotation.concurrent.NotThreadSafe;
import java.io.IOException;
import java.io.InputStream;
import java.nio.BufferOverflowException;
import java.nio.BufferUnderflowException;
import java.nio.ByteBuffer;
import java.nio.CharBuffer;
import java.nio.channels.ReadableByteChannel;
import java.nio.channels.WritableByteChannel;
import java.nio.charset.Charset;
import java.nio.charset.CharsetDecoder;
import java.nio.charset.CoderResult;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;

/**
 * A {@literal Buffer} is a general-purpose IO utility class that wraps a {@link ByteBuffer}.
 * It provides optional dynamic expansion of the buffer to accommodate additional content.
 * It also provides convenience methods for operating on buffers.
 *
 * @author Jon Brisbin
 */
@NotThreadSafe
public class Buffer implements Comparable<Buffer>,
															 Iterable<Byte>,
															 ReadableByteChannel,
															 WritableByteChannel {

	/**
	 * The size, in bytes, of a small buffer. Can be configured using the {@code
	 * reactor.io.defaultBufferSize} system property. Default to 16384 bytes.
	 */
	public static int SMALL_BUFFER_SIZE = Integer.parseInt(
			System.getProperty("reactor.io.defaultBufferSize", "" + 1024 * 16)
	);

	/**
	 * The maximum allowed buffer size in bytes. Can be configured using the {@code
	 * reactor.io.maxBufferSize} system property. Defaults to 16384000 bytes.
	 */
	public static int MAX_BUFFER_SIZE   = Integer.parseInt(
			System.getProperty("reactor.io.maxBufferSize", "" + 1024 * 1000 * 16)
	);

	private static final Charset UTF8 = Charset.forName("UTF-8");
	private final boolean        dynamic;
	private       ByteBuffer     buffer;
	private       CharsetDecoder decoder;
	private       CharBuffer     chars;
	private       int            position;
	private       int            limit;

	/**
	 * Create an empty {@literal Buffer} that is dynamic.
	 */
	public Buffer() {
		this.dynamic = true;
	}

	/**
	 * Create an {@literal Buffer} that has an internal {@link ByteBuffer} allocated to the given size
	 * and optional make this buffer fixed-length.
	 *
	 * @param atLeast Allocate this many bytes immediately.
	 * @param fixed   {@literal true} to make this buffer fixed-length, {@literal false} otherwise.
	 */
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

	/**
	 * Copy constructor that creates a shallow copy of the given {@literal Buffer} by calling {@link
	 * java.nio.ByteBuffer#duplicate()} on the underlying {@link ByteBuffer}.
	 *
	 * @param bufferToCopy The {@literal Buffer} to copy.
	 */
	public Buffer(Buffer bufferToCopy) {
		this.dynamic = bufferToCopy.dynamic;
		this.buffer = bufferToCopy.buffer.duplicate();
	}

	/**
	 * Create a {@literal Buffer} using the given {@link ByteBuffer} as the inital source.
	 *
	 * @param bufferToStartWith The {@link ByteBuffer} to start with.
	 */
	public Buffer(ByteBuffer bufferToStartWith) {
		this.dynamic = true;
		this.buffer = bufferToStartWith;
	}

	/**
	 * Convenience method to create a new, fixed-length {@literal Buffer} and putting the given byte
	 * array into the buffer.
	 *
	 * @param bytes The bytes to create a buffer from.
	 * @return The new {@literal Buffer}.
	 */
	public static Buffer wrap(byte[] bytes) {
		return new Buffer(bytes.length, true)
				.append(bytes)
				.flip();
	}

	/**
	 * Convenience method to create a new {@literal Buffer} from the given String and optionally specify
	 * whether the new {@literal Buffer} should be a fixed length or not.
	 *
	 * @param str   The String to create a buffer from.
	 * @param fixed {@literal true} to create a fixed-length {@literal Buffer}, {@literal false} otherwise.
	 * @return The new {@literal Buffer}.
	 */
	public static Buffer wrap(String str, boolean fixed) {
		return new Buffer(str.length(), fixed)
				.append(str)
				.flip();
	}

	/**
	 * Convenience method to create a new, fixed-length {@literal Buffer} from the given String.
	 *
	 * @param str The String to create a buffer from.
	 * @return The new fixed-length {@literal Buffer}.
	 */
	public static Buffer wrap(String str) {
		return wrap(str, true);
	}

	/**
	 * Very efficient method for parsing an {@link Integer} from the given {@literal Buffer} range. Much
	 * faster than {@link Integer#parseInt(String)}.
	 *
	 * @param b     The {@literal Buffer} to slice.
	 * @param start start of the range.
	 * @param end   end of the range.
	 * @return The int value or {@literal null} if the {@literal Buffer} could not be read.
	 */
	public static Integer parseInt(Buffer b, int start, int end) {
		b.snapshot();

		b.buffer.limit(end);
		b.buffer.position(start);

		Integer i = parseInt(b);

		b.reset();

		return i;
	}

	/**
	 * Very efficient method for parsing an {@link Integer} from the given {@literal Buffer}. Much faster
	 * than {@link Integer#parseInt(String)}.
	 *
	 * @param b The {@literal Buffer} to slice.
	 * @return The int value or {@literal null} if the {@literal Buffer} could not be read.
	 */
	public static Integer parseInt(Buffer b) {
		if (b.remaining() == 0) {
			return null;
		}

		b.snapshot();
		int len = b.remaining();

		int num = 0;
		int dec = 1;
		for (int i = (b.position + len); i > b.position; ) {
			char c = (char) b.buffer.get(--i);
			num += Character.getNumericValue(c) * dec;
			dec *= 10;
		}

		b.reset();

		return num;
	}

	/**
	 * Very efficient method for parsing a {@link Long} from the given {@literal Buffer} range. Much faster
	 * than {@link Long#parseLong(String)}.
	 *
	 * @param b     The {@literal Buffer} to slice.
	 * @param start start of the range.
	 * @param end   end of the range.
	 * @return The long value or {@literal null} if the {@literal Buffer} could not be read.
	 */
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

	/**
	 * Very efficient method for parsing a {@link Long} from the given {@literal Buffer}. Much faster than {@link
	 * Long#parseLong(String)}.
	 *
	 * @param b The {@literal Buffer} to slice.
	 * @return The long value or {@literal null} if the {@literal Buffer} could not be read.
	 */
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

	/**
	 * Whether this {@literal Buffer} is fixed-length or not.
	 *
	 * @return {@literal true} if this {@literal Buffer} is not fixed-length, {@literal false} otherwise.
	 */
	public boolean isDynamic() {
		return dynamic;
	}

	/**
	 * Provides the current position in the internal {@link ByteBuffer}.
	 *
	 * @return The current position.
	 */
	public int position() {
		return (null == buffer ? 0 : buffer.position());
	}

	/**
	 * Sets this buffer's position.
	 *
	 * @param pos the new position
	 *
	 * @return this buffer
	 */
	public Buffer position(int pos) {
		if (null != buffer) {
			buffer.position(pos);
		}
		return this;
	}

	/**
	 * Sets this buffer's limit.
	 *
	 * @param limit the new limit
	 *
	 * @return this buffer
	 */
	public Buffer limit(int limit) {
		if (null != buffer) {
			buffer.limit(limit);
		}
		return this;
	}

	/**
	 * Skips {@code len} bytes.
	 *
	 * @param len the number of bytes to skip
	 *
	 * @return this buffer
	 *
	 * @throws BufferUnderflowException if the skip exceeds the available bytes
	 * @throws IllegalArgumentException if len is negative
	 */
	public Buffer skip(int len) {
		if (len < 0) {
			throw new IllegalArgumentException("len must >= 0");
		}
		if (null != buffer) {
			int pos = buffer.position();
			buffer.position(pos + len);
		}
		return this;
	}

	/**
	 * Provides the current limit of the internal {@link ByteBuffer}.
	 *
	 * @return The current limit.
	 */
	public int limit() {
		return (null == buffer ? 0 : buffer.limit());
	}

	/**
	 * Provides the current capacity of the internal {@link ByteBuffer}.
	 *
	 * @return The current capacity.
	 */
	public int capacity() {
		return (null == buffer ? SMALL_BUFFER_SIZE : buffer.capacity());
	}

	/**
	 * How many bytes available in this {@literal Buffer}. If reading, it is the number of bytes
	 * available to read. If writing, it is the number of bytes available for writing.
	 *
	 * @return The number of bytes available in this {@literal Buffer}.
	 */
	public int remaining() {
		return (null == buffer ? SMALL_BUFFER_SIZE : buffer.remaining());
	}

	/**
	 * Clear the internal {@link ByteBuffer} by setting the {@link ByteBuffer#position(int)} to 0
	 * and the limit to the current capacity.
	 *
	 * @return {@literal this}
	 */
	public Buffer clear() {
		if (null != buffer) {
			buffer.position(0);
			buffer.limit(buffer.capacity());
		}
		return this;
	}

	/**
	 * Compact the underlying {@link ByteBuffer}.
	 *
	 * @return {@literal this}
	 */
	public Buffer compact() {
		if (null != buffer) {
			buffer.compact();
		}
		return this;
	}

	/**
	 * Flip this {@literal Buffer}. Used after a write to prepare this {@literal Buffer} for reading.
	 *
	 * @return {@literal this}
	 */
	public Buffer flip() {
		if (null != buffer) {
			buffer.flip();
		}
		return this;
	}

	/**
	 * Rewind this {@literal Buffer} to the beginning. Prepares the {@literal Buffer} for reuse.
	 *
	 * @return {@literal this}
	 */
	public Buffer rewind() {
		if (null != buffer) {
			buffer.rewind();
		}
		return this;
	}

	/**
	 * Rewinds this buffer by {@code len} bytes.
	 *
	 * @param len The number of bytes the rewind by
	 *
	 * @return this buffer
	 *
	 * @throws BufferUnderflowException if the rewind would move past the start of the buffer
	 * @throws IllegalArgumentException if len is negative
	 */
	public Buffer rewind(int len) {
		if (len < 0) {
			throw new IllegalArgumentException("len must >= 0");
		}
		if (null != buffer) {
			int pos = buffer.position();
			buffer.position(pos - len);
		}
		return this;
	}

	/**
	 * Prepend the given {@link Buffer} to this {@literal Buffer}.
	 *
	 * @param b The {@link Buffer} to prepend.
	 * @return {@literal this}
	 */
	public Buffer prepend(Buffer b) {
		if (null == b) {
			return this;
		}
		return prepend(b.buffer);
	}

	/**
	 * Prepend the given {@link String } to this {@literal Buffer}.
	 *
	 * @param s The {@link String} to prepend.
	 * @return {@literal this}
	 */
	public Buffer prepend(String s) {
		if (null == s) {
			return this;
		}
		return prepend(s.getBytes());
	}

	/**
	 * Prepend the given {@code byte[]} array to this {@literal Buffer}.
	 *
	 * @param bytes The {@code byte[]} to prepend.
	 * @return {@literal this}
	 */
	public Buffer prepend(byte[] bytes) {
		shift(bytes.length);
		buffer.put(bytes);
		reset();
		return this;
	}

	/**
	 * Prepend the given {@link ByteBuffer} to this {@literal Buffer}.
	 *
	 * @param b The {@link ByteBuffer} to prepend.
	 * @return {@literal this}
	 */
	public Buffer prepend(ByteBuffer b) {
		if (null == b) {
			return this;
		}
		shift(b.remaining());
		this.buffer.put(b);
		reset();
		return this;

	}

	/**
	 * Prepend the given {@code byte} to this {@literal Buffer}.
	 *
	 * @param b The {@code byte} to prepend.
	 * @return {@literal this}
	 */
	public Buffer prepend(byte b) {
		shift(1);
		this.buffer.put(b);
		reset();
		return this;
	}

	/**
	 * Prepend the given {@code char} to this existing {@literal Buffer}.
	 *
	 * @param c The {@code char} to prepend.
	 * @return {@literal this}
	 */
	public Buffer prepend(char c) {
		shift(2);
		this.buffer.putChar(c);
		reset();
		return this;
	}

	/**
	 * Prepend the given {@code int} to this {@literal Buffer}.
	 *
	 * @param i The {@code int} to prepend.
	 * @return {@literal this}
	 */
	public Buffer prepend(int i) {
		shift(4);
		this.buffer.putInt(i);
		reset();
		return this;
	}

	/**
	 * Prepend the given {@code long} to this {@literal Buffer}.
	 *
	 * @param l The {@code long} to prepend.
	 * @return {@literal this}
	 */
	public Buffer prepend(long l) {
		shift(8);
		this.buffer.putLong(l);
		reset();
		return this;
	}

	/**
	 * Append the given String to this {@literal Buffer}.
	 *
	 * @param s The String to append.
	 * @return {@literal this}
	 */
	public Buffer append(String s) {
		ensureCapacity(s.length());
		buffer.put(s.getBytes());
		return this;
	}

	/**
	 * Append the given {@code int} to this {@literal Buffer}.
	 *
	 * @param i The {@code int} to append.
	 * @return {@literal this}
	 */
	public Buffer append(int i) {
		ensureCapacity(4);
		buffer.putInt(i);
		return this;
	}

	/**
	 * Append the given {@code long} to this {@literal Buffer}.
	 *
	 * @param l The {@code long} to append.
	 * @return {@literal this}
	 */
	public Buffer append(long l) {
		ensureCapacity(8);
		buffer.putLong(l);
		return this;
	}

	/**
	 * Append the given {@code char} to this {@literal Buffer}.
	 *
	 * @param c The {@code char} to append.
	 * @return {@literal this}
	 */
	public Buffer append(char c) {
		ensureCapacity(2);
		buffer.putChar(c);
		return this;
	}

	/**
	 * Append the given {@link ByteBuffer} to this {@literal Buffer}.
	 *
	 * @param buffers The {@link ByteBuffer ByteBuffers} to append.
	 * @return {@literal this}
	 */
	public Buffer append(ByteBuffer... buffers) {
		for (ByteBuffer bb : buffers) {
			ensureCapacity(bb.remaining());
			buffer.put(bb);
		}
		return this;
	}

	/**
	 * Append the given {@link Buffer} to this {@literal Buffer}.
	 *
	 * @param buffers The {@link Buffer Buffers} to append.
	 * @return {@literal this}
	 */
	public Buffer append(Buffer... buffers) {
		for (Buffer b : buffers) {
			int pos = (null == buffer ? 0 : buffer.position());
			int len = b.remaining();
			ensureCapacity(len);
			buffer.put(b.byteBuffer());
			buffer.position(pos + len);
		}
		return this;
	}

	/**
	 * Append the given {@code byte} to this {@literal Buffer}.
	 *
	 * @param b The {@code byte} to append.
	 * @return {@literal this}
	 */
	public Buffer append(byte b) {
		ensureCapacity(1);
		buffer.put(b);
		return this;
	}

	/**
	 * Append the given {@code byte[]} to this {@literal Buffer}.
	 *
	 * @param b The {@code byte[]} to append.
	 * @return {@literal this}
	 */
	public Buffer append(byte[] b) {
		ensureCapacity(b.length);
		buffer.put(b);
		return this;
	}

	/**
	 * Get the first {@code byte} from this {@literal Buffer}.
	 *
	 * @return The first {@code byte}.
	 */
	public byte first() {
		snapshot();
		if (this.position > 0) {
			buffer.position(0); // got to the 1st position
		}
		byte b = buffer.get(); // get the 1st byte
		reset(); // go back to original pos
		return b;
	}

	/**
	 * Get the last {@code byte} from this {@literal Buffer}.
	 *
	 * @return The last {@code byte}.
	 */
	public byte last() {
		int pos = buffer.position();
		int limit = buffer.limit();
		buffer.position(limit - 1); // go to right before last position
		byte b = buffer.get(); // get the last byte
		buffer.position(pos); // go back to original pos
		return b;
	}

	/**
	 * Read a single {@code byte} from the underlying {@link ByteBuffer}.
	 *
	 * @return The next {@code byte}.
	 */
	public byte read() {
		if (null != buffer) {
			return buffer.get();
		}
		throw new BufferUnderflowException();
	}

	/**
	 * Read at least {@code b.length} bytes from the underlying {@link ByteBuffer}.
	 *
	 * @param b The buffer to fill.
	 * @return {@literal this}
	 */
	public Buffer read(byte[] b) {
		if (null != buffer) {
			buffer.get(b);
		}
		return this;
	}

	/**
	 * Read the next {@code int} from the underlying {@link ByteBuffer}.
	 *
	 * @return The next {@code int}.
	 */
	public int readInt() {
		if (null != buffer) {
			return buffer.getInt();
		}
		throw new BufferUnderflowException();
	}

	/**
	 * Read the next {@code float} from the underlying {@link ByteBuffer}.
	 *
	 * @return The next {@code float}.
	 */
	public float readFloat() {
		if (null != buffer) {
			return buffer.getFloat();
		}
		throw new BufferUnderflowException();
	}

	/**
	 * Read the next {@code double} from the underlying {@link ByteBuffer}.
	 *
	 * @return The next {@code double}.
	 */
	public double readDouble() {
		if (null != buffer) {
			return buffer.getDouble();
		}
		throw new BufferUnderflowException();
	}

	/**
	 * Read the next {@code long} from the underlying {@link ByteBuffer}.
	 *
	 * @return The next {@code long}.
	 */
	public long readLong() {
		if (null != buffer) {
			return buffer.getLong();
		}
		throw new BufferUnderflowException();
	}

	/**
	 * Read the next {@code char} from the underlying {@link ByteBuffer}.
	 *
	 * @return The next {@code char}.
	 */
	public char readChar() {
		if (null != buffer) {
			return buffer.getChar();
		}
		throw new BufferUnderflowException();
	}

	/**
	 * Save the current buffer position and limit.
	 */
	public void snapshot() {
		this.position = buffer.position();
		this.limit = buffer.limit();
	}

	/**
	 * Reset the buffer to the previously-saved position and limit.
	 *
	 * @return {@literal this}
	 */
	public Buffer reset() {
		buffer.limit(limit);
		buffer.position(position);
		return this;
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
		snapshot();
		if (dst.remaining() < this.limit) {
			buffer.limit(dst.remaining());
		}
		int pos = dst.position();
		dst.put(buffer);
		buffer.limit(this.limit);
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

	/**
	 * Convert the contents of this buffer into a String using a UTF-8 {@link CharsetDecoder}.
	 *
	 * @return The contents of this {@literal Buffer} as a String.
	 */
	public String asString() {
		if (null != buffer) {
			return decode();
		} else {
			return null;
		}
	}

	/**
	 * Slice a portion of this buffer and convert it to a String.
	 *
	 * @param start start of the range.
	 * @param end   end of the range.
	 * @return The contents of the given range as a String.
	 */
	public String substring(int start, int end) {
		snapshot();

		buffer.limit((end > start ? end : this.limit));
		buffer.position(start);
		String s = asString();

		reset();
		return s;
	}

	/**
	 * Return the contents of this buffer copied into a {@code byte[]}.
	 *
	 * @return The contents of this buffer as a {@code byte[]}.
	 */
	public byte[] asBytes() {
		if (null != buffer) {
			snapshot();
			byte[] b = new byte[buffer.remaining()];
			buffer.get(b);
			reset();
			return b;
		} else {
			return null;
		}
	}

	/**
	 * Create an {@link InputStream} capable of reading the bytes from the internal {@link ByteBuffer}.
	 *
	 * @return A new {@link InputStream}.
	 */
	public InputStream inputStream() {
		return new BufferInputStream();
	}

	/**
	 * Create a copy of the given range.
	 *
	 * @param start start of the range.
	 * @param len   end of the range.
	 * @return A new {@link Buffer}, constructed from the contents of the given range.
	 */
	public Buffer slice(int start, int len) {
		snapshot();
		ByteBuffer bb = ByteBuffer.allocate(len);
		buffer.position(start);
		bb.put(buffer);
		reset();
		bb.flip();
		return new Buffer(bb);
	}

	/**
	 * Split this buffer on the given delimiter.
	 *
	 * @param delimiter The delimiter on which to split this buffer.
	 * @return An {@link Iterable} of {@link View Views} that point to the segments of this buffer.
	 */
	public Iterable<View> split(int delimiter) {
		return split(new ArrayList<View>(), delimiter, false);
	}

	/**
	 * Split this buffer on the given delimiter but save memory by reusing the given {@link List}.
	 *
	 * @param views     The list to store {@link View Views} in.
	 * @param delimiter The delimiter on which to split this buffer.
	 * @return An {@link Iterable} of {@link View Views} that point to the segments of this buffer.
	 */
	public Iterable<View> split(List<View> views, int delimiter) {
		return split(views, delimiter, false);
	}

	/**
	 * Split this buffer on the given delimiter and optionally leave the delimiter intact rather than stripping it.
	 *
	 * @param delimiter      The delimiter on which to split this buffer.
	 * @param stripDelimiter {@literal true} to ignore the delimiter, {@literal false} to leave it in the returned data.
	 * @return An {@link Iterable} of {@link View Views} that point to the segments of this buffer.
	 */
	public Iterable<View> split(int delimiter, boolean stripDelimiter) {
		return split(new ArrayList<View>(), delimiter, stripDelimiter);
	}

	/**
	 * Split this buffer on the given delimiter, save memory by reusing the given {@link List}, and optionally leave the
	 * delimiter intact rather than stripping it.
	 *
	 * @param views          The list to store {@link View Views} in.
	 * @param delimiter      The delimiter on which to split this buffer.
	 * @param stripDelimiter @{literal true} to ignore the delimiter, {@literal false} to leave it in the returned data.
	 * @return An {@link Iterable} of {@link View Views} that point to the segments of this buffer.
	 */
	public Iterable<View> split(List<View> views, int delimiter, boolean stripDelimiter) {
		snapshot();

		int start = this.position;
		for (byte b : this) {
			if (b == delimiter) {
				int end = stripDelimiter ? buffer.position() - 1 : buffer.position();
				views.add(createView(start, end));
				start = end + (stripDelimiter ? 1 : 0);
			}
		}
		if (start != buffer.position()) {
			buffer.position(start);
		}

		reset();

		return views;
	}

	/**
	 * Create a {@link View} of the current range of this {@link Buffer}.
	 *
	 * @return The view of the buffer
	 *
	 * @see #position()
	 * @see #limit()
	 */
	public View createView() {
		snapshot();
		return new View(position, limit);
	}

	/**
	 * Create a {@link View} of the given range of this {@literal Buffer}.
	 *
	 * @param start start of the range.
	 * @param end   end of the range.
	 * @return A new {@link View} object that represents the given range.
	 */
	public View createView(int start, int end) {
		snapshot();
		return new View(start, end);
	}

	/**
	 * Slice this buffer at the given positions. Useful for extracting multiple segments of data from a buffer when the
	 * exact indices of that data is already known.
	 *
	 * @param positions The start and end positions of the slices.
	 * @return A list of {@link View Views} pointing to the slices.
	 */
	public List<View> slice(int... positions) {
		Assert.notNull(positions, "Positions cannot be null.");
		if (positions.length == 0) {
			return Collections.emptyList();
		}

		snapshot();

		List<View> views = new ArrayList<View>();
		int len = positions.length;
		for (int i = 0; i < len; i++) {
			int start = positions[i];
			int end = (i + 1 < len ? positions[++i] : this.limit);
			views.add(createView(start, end));
			reset();
		}

		return views;
	}

	/**
	 * Return the underlying {@link ByteBuffer}.
	 *
	 * @return The {@link ByteBuffer} in use.
	 */
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
				// there's remaining capacity that hasn't been used yet
				if (pos + atLeast > cap) {
					expand();
					cap = buffer.capacity();
				}
				buffer.limit(Math.min(pos + atLeast, cap));
			} else {
				expand();
			}
		} else if (pos + SMALL_BUFFER_SIZE > MAX_BUFFER_SIZE) {
			throw new BufferOverflowException();
		}
	}

	private void expand() {
		snapshot();
		ByteBuffer newBuff = ByteBuffer.allocate(buffer.limit() + SMALL_BUFFER_SIZE);
		buffer.flip();
		newBuff.put(buffer);
		buffer = newBuff;
		reset();
	}

	private String decode() {
		if (null == decoder) {
			decoder = UTF8.newDecoder();
		}
		snapshot();
		try {
			if (null == chars || chars.remaining() < buffer.remaining()) {
				chars = CharBuffer.allocate(buffer.remaining());
			} else {
				chars.rewind();
			}
			decoder.reset();
			CoderResult cr = decoder.decode(buffer, chars, true);
			if (cr.isUnderflow()) {
				decoder.flush(chars);
			}
			chars.flip();

			return chars.toString();
		} finally {
			reset();
		}
	}

	private void shift(int right) {
		ByteBuffer currentBuffer;
		if (null == buffer) {
			ensureCapacity(right);
			currentBuffer = buffer;
		} else {
			currentBuffer = buffer.slice();
		}

		int len = buffer.remaining();
		int pos = buffer.position();
		ensureCapacity(right + len);

		buffer.position(pos + right);
		buffer.put(currentBuffer);
		buffer.position(pos);

		snapshot();
	}

	private class BufferInputStream extends InputStream {
		ByteBuffer buffer = Buffer.this.buffer.slice();

		@Override
		public int read(byte[] b) throws IOException {
			int pos = buffer.position();
			buffer.get(b);
			return buffer.position() - pos;
		}

		@Override
		public int read(byte[] b, int off, int len) throws IOException {
			if (null == buffer || buffer.remaining() == 0) {
				return -1;
			}
			byte[] bytes = asBytes();
			int bytesLen = bytes.length;
			System.arraycopy(bytes, 0, b, off, bytesLen);
			if (len < bytesLen) {
				buffer.position(position + len);
			}
			return bytesLen;
		}

		@Override
		public long skip(long n) throws IOException {
			if (n < buffer.remaining()) {
				throw new IOException(new BufferUnderflowException());
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

	/**
	 * A {@literal View} represents a segment of a buffer. When {@link #get()} is called, the {@literal Buffer} is set to
	 * the correct start and end points as given at creation time. After the view has been used, it is the responsibility
	 * of the caller to {@link #reset()} the buffer if more manipulation is required. Otherwise, multiple views can be
	 * created from a single buffer and used consecutively to extract portions of a buffer without expensive substrings.
	 */
	public class View implements Supplier<Buffer> {
		private final int start;
		private final int end;

		private View(int start, int end) {
			this.start = start;
			this.end = end;
		}

		/**
		 * Get the start of this range.
		 *
		 * @return start of the range.
		 */
		public int getStart() {
			return start;
		}

		/**
		 * Get the end of this range.
		 *
		 * @return end of the range.
		 */
		public int getEnd() {
			return end;
		}

		@Override
		public Buffer get() {
			buffer.limit(end);
			buffer.position(start);
			return Buffer.this;
		}
	}

}
