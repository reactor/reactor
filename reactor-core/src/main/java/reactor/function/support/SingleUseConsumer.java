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

package reactor.function.support;

import reactor.event.registry.Registration;
import reactor.function.Consumer;

/**
 * A {@link Consumer} implementation that allows the delegate {@link Consumer} to only be
 * called once. Should be used in combination with {@link Registration#cancelAfterUse()} to
 * ensure that this {@link reactor.function.Consumer Consumer's} {@link
 * reactor.event.registry.Registration} is cancelled as soon after its use as possible.
 *
 * @param <T> the type of the values that the consumer can accept
 *
 * @author Jon Brisbin
 */
public class SingleUseConsumer<T> implements Consumer<T> {

	private final Consumer<T> delegate;
	private final    Object  monitor = new Object();
	private volatile boolean called  = false;

	/**
	 * Create a single-use {@link Consumer} using the given delgate.
	 *
	 * @param delegate The {@link Consumer} to delegate accept calls to.
	 */
	public SingleUseConsumer(Consumer<T> delegate) {
		this.delegate = delegate;
	}

	@Override
	public void accept(T t) {
		synchronized (monitor) {
			if (called) {
				return;
			}
			called = true;
		}
		delegate.accept(t);
	}

}
