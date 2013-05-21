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

import java.nio.ByteBuffer;

import reactor.tcp.data.Buffers;


/**
 * @author Gary Russell
 *
 */
public class LineFeedCodec extends AbstractCodec {

	private static final int LF = 0x0a;

	private static final ByteBuffer LF_BB = ByteBuffer.wrap(new byte[] {LF});

	@Override
	public void decode(Buffers buffers, DecoderCallback callback) {
		while (buffers.hasNext()) {
			if (buffers.next() == LF) {
				callback.complete(new DefaultAssembly(buffers, buffers.getPosition(), 1));
			}
		}
		if (logger.isDebugEnabled()) {
			logger.debug("Incomplete decoding, current position " + buffers.getPosition() + " of " + buffers.getSize());
		}
	}

	@Override
	public Buffers encode(ByteBuffer data) {
		Buffers buffers = new Buffers();
		buffers.add(data);
		buffers.add(LF_BB.duplicate());
		return buffers;
	}
}
