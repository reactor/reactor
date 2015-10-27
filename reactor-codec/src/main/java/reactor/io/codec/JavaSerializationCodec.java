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

package reactor.io.codec;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;

import reactor.io.buffer.Buffer;

/**
 * {@code Codec} to transform Java objects into {@link reactor.io.buffer.Buffer Buffers} and visa-versa.
 *
 * @author Jon Brisbin
 * @author Stephane Maldini
 */
public class JavaSerializationCodec<T> extends BufferCodec<T, T> {

	@Override
	@SuppressWarnings("unchecked")
	protected T decodeNext(Buffer buff, Object context) {
		if (buff.remaining() <= 0) {
			return null;
		}
		try {
			return  (T) new ObjectInputStream(new ByteArrayInputStream(buff.asBytes())).readObject();
		} catch (Exception e) {
			throw new IllegalStateException(e.getMessage(), e);
		}
	}

	@Override
	public Buffer apply(T t) {
		ByteArrayOutputStream baos = new ByteArrayOutputStream();
		try {
			ObjectOutputStream oos = new ObjectOutputStream(baos);
			oos.writeObject(t);
			oos.flush();
			oos.close();
		} catch (IOException e) {
			throw new IllegalStateException(e.getMessage(), e);
		}

		return Buffer.wrap(baos.toByteArray());
	}

}
