/*
 * Copyright (c) 2011-2014 Pivotal Software, Inc.
 *
 *  Licensed under the Apache License, Version 2.0 (the "License");
 *  you may not use this file except in compliance with the License.
 *  You may obtain a copy of the License at
 *
 *         http://www.apache.org/licenses/LICENSE-2.0
 *
 *  Unless required by applicable law or agreed to in writing, software
 *  distributed under the License is distributed on an "AS IS" BASIS,
 *  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  See the License for the specific language governing permissions and
 *  limitations under the License.
 */

package reactor.io.codec.compress;

import reactor.fn.Consumer;
import reactor.fn.Function;
import reactor.io.buffer.Buffer;
import reactor.io.codec.BufferCodec;
import reactor.io.codec.Codec;

import java.io.*;

/**
 * @author Jon Brisbin
 * @author Stephane Maldini
 */
public abstract class CompressionCodec<IN, OUT> extends BufferCodec<IN, OUT> {

	private final Codec<Buffer, IN, OUT> delegate;

	protected CompressionCodec(Codec<Buffer, IN, OUT> delegate) {
		this.delegate = delegate;
	}

	@Override
	protected IN decodeNext(Buffer buffer, Object context) {
		try {
			ByteArrayInputStream bin = new ByteArrayInputStream(buffer.asBytes());
			InputStream zin = createInputStream(bin);
			Buffer newBuff = new Buffer();
			while (zin.available() > 0) {
				newBuff.append((byte) zin.read());
			}
			zin.close();
			return delegate.decodeNext(newBuff.flip());
		} catch (IOException e) {
			throw new IllegalStateException(e.getMessage(), e);
		}
	}

	@Override
	public Buffer apply(OUT out) {
		Buffer buff = delegate.apply(out);
		try {
			ByteArrayOutputStream bout = new ByteArrayOutputStream();
			OutputStream zout = createOutputStream(bout);
			zout.write(buff.asBytes());
			zout.flush();
			bout.flush();
			zout.close();
			return Buffer.wrap(bout.toByteArray());
		} catch (IOException e) {
			throw new IllegalStateException(e.getMessage(), e);
		}
	}

	protected abstract InputStream createInputStream(InputStream parent) throws IOException;

	protected abstract OutputStream createOutputStream(OutputStream parent) throws IOException;

}

