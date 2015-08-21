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

import org.xerial.snappy.SnappyInputStream;
import org.xerial.snappy.SnappyOutputStream;
import reactor.io.buffer.Buffer;
import reactor.io.codec.Codec;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;

/**
 * @author Jon Brisbin
 */
public class SnappyCodec<IN, OUT> extends CompressionCodec<IN, OUT> {

	public SnappyCodec(Codec<Buffer, IN, OUT> delegate) {
		super(delegate);
	}

	@Override
	protected InputStream createInputStream(InputStream parent) throws IOException {
		return new SnappyInputStream(parent);
	}

	@Override
	protected OutputStream createOutputStream(OutputStream parent) throws IOException {
		return new SnappyOutputStream(parent);
	}

}
