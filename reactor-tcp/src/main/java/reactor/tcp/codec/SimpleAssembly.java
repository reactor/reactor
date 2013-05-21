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

import java.io.ByteArrayInputStream;
import java.io.InputStream;

import reactor.tcp.ByteIterator;

/**
 * @author Gary Russell
 *
 */
public class SimpleAssembly implements Assembly {

	private final byte[] bytes;

	private final int offset;

	private final int length;

	private volatile byte[] normalized;

	public SimpleAssembly(byte[] bytes) {
		this(bytes, 0, bytes.length);
	}

	public SimpleAssembly(byte[] bytes, int offset, int length) {
		this.bytes = bytes;
		this.offset = offset;
		this.length = length;
	}

	@Override
	public InputStream asInputStream() {
		return new ByteArrayInputStream(this.bytes, this.offset, this.length);
	}

	@Override
	public byte[] asBytes() {
		if (this.length == this.bytes.length) {
			return bytes;
		}
		if (this.normalized == null) {
			byte[] normalized = new byte[this.length];
			System.arraycopy(this.bytes, this.offset, normalized, 0, length);
			this.normalized = normalized;
		}
		return this.normalized;
	}

	@Override
	public ByteIterator iterator() {
		return new SimpleByteIterator();
	}

	private class SimpleByteIterator implements ByteIterator {

		private volatile int offset;

		private SimpleByteIterator() {
			this.offset = SimpleAssembly.this.offset;
		}

		@Override
		public boolean hasNext() {
			return this.offset < SimpleAssembly.this.offset + length;
		}

		@Override
		public byte next() {
			return bytes[offset++];
		}

	}
}
