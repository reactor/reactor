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

import reactor.core.Reactor;
import reactor.fn.Consumer;
import reactor.fn.Event;
import reactor.tcp.Buffers;

/**
 * Base class for codecs (specifically decoders) that can only
 * work by pulling data (such as Java Deserialization) rather than
 * the more efficient push algorithm.
 *
 * @author Gary Russell
 * @since 1.0
 *
 */
public abstract class PullCodecSupport extends AbstractCodec {

	private final Reactor reactor = new Reactor();

	public PullCodecSupport() {
		this.reactor.on(new Consumer<Event<AssemblyInstructions>>() {

			@Override
			public void accept(Event<AssemblyInstructions> event) {
				doAssembly(event.getData().buffers, event.getData().callback);
			}
		});
	}

	protected Reactor getReactor() {
		return reactor;
	}

	abstract void doAssembly(Buffers buffers, DecoderCallback callback);

	protected class AssemblyInstructions {

		private final Buffers buffers;

		private final DecoderCallback callback;

		public AssemblyInstructions(Buffers buffers, DecoderCallback callback) {
			this.buffers = buffers;
			this.callback = callback;
		}
	}
}
