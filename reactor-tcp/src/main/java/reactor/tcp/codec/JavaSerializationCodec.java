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

import java.io.IOException;
import java.io.OutputStream;
import java.nio.ByteBuffer;

import reactor.Fn;
import reactor.tcp.data.Buffers;

/**
 * @author Gary Russell
 *
 */
public class JavaSerializationCodec extends PullCodecSupport implements StreamingCodec {

	@Override
	public void decode(Buffers buffers, DecoderCallback callback) {
		this.getReactor().notify(Fn.event(new AssemblyInstructions(buffers, callback)));
	}

	@Override
	public Buffers encode(ByteBuffer buffer) {
		throw new UnsupportedOperationException("Use streaming encode");
	}

	@Override
	public void encode(Object data, OutputStream outputStream) {
		try {
			SerializationUtils.serialize(data, outputStream);
		}
		catch (IOException e) {
			throw new RuntimeException(e);
		}
	}

	@Override
	void doAssembly(Buffers buffers, DecoderCallback callback) {
		try {
			Object object = SerializationUtils.deserialize(buffers.getInputStream());
			callback.complete(new DecodedObject(object));
		}
		catch (IOException e) {
			throw new RuntimeException(e);
		}
	}

}
