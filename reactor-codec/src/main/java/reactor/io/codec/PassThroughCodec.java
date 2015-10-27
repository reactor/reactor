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

import reactor.fn.Consumer;
import reactor.fn.Function;
import reactor.io.buffer.Buffer;

/**
 * A simple {@code Codec} that uses the source object as both input and output. Override the {@link
 * #beforeAccept(Object)} and {@link #beforeApply(Object)} methods to intercept data coming in and going out
 * (respectively).
 *
 * @author Jon Brisbin
 * @author Stephane Maldini
 */
public class PassThroughCodec<SRC> extends Codec<SRC, SRC, SRC> {
	@Override
	public Function<SRC, SRC> decoder(final Consumer<SRC> next) {
		return new DefaultInvokeOrReturnFunction<>(next);
	}

	@Override
	@SuppressWarnings("unchecked")
	protected SRC decodeNext(SRC buffer, Object context) {
		return beforeAccept(buffer);
	}

	@Override
	public SRC apply(SRC src) {
		return beforeApply(src);
	}

	/**
	 * Override to intercept the source object before it is passed into the next {@link reactor.fn.Consumer} or
	 * returned to the caller if a {@link reactor.fn.Consumer} is not set.
	 *
	 * @param src The source object.
	 * @return
	 */
	protected SRC beforeAccept(SRC src) {
		// NO-OP
		return src;
	}

	/**
	 * Override to intercept the source object before it is returned for output.
	 *
	 * @param src
	 * @return
	 */
	protected SRC beforeApply(SRC src) {
		// NO-OP
		return src;
	}

}
