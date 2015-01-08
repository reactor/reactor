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

package reactor.fn.support;

import reactor.bus.registry.Registration;
import reactor.fn.Consumer;

import java.util.concurrent.atomic.AtomicBoolean;

/**
 * A {@link reactor.fn.Consumer} implementation that allows the delegate {@link reactor.fn.Consumer} to only be called once. Should be used in
 * combination with {@link Registration#cancelAfterUse()} to ensure that this {@link reactor.fn.Consumer
 * Consumer's} {@link reactor.bus.registry.Registration} is cancelled as soon after its use as possible.
 *
 * @param <T>
 * 		the type of the values that the consumer can accept
 *
 * @author Jon Brisbin
 * @author Stephane Maldini
 */
public class SingleUseConsumer<T> implements Consumer<T> {

	private final AtomicBoolean called = new AtomicBoolean();

	private final Consumer<? super T> delegate;

	/**
	 * Used to create anonymous subclasses.
	 */
	public SingleUseConsumer() {
		this.delegate = null;
	}

	/**
	 * Create a single-use {@link Consumer} using the given delgate.
	 *
	 * @param delegate
	 * 		The {@link Consumer} to delegate accept calls to.
	 */
	public SingleUseConsumer(Consumer<? super T> delegate) {
		this.delegate = delegate;
	}

	/**
	 * Static helper method for creating {@code SingleUseConsumer SingleUseConsumers} in code with a little less noise.
	 *
	 * @param delegate
	 * 		The delegate {@code Consumer} to invoke.
	 * @param <T>
	 * 		Type of the Consumer's argument.
	 *
	 * @return A new {@code Consumer} that will only be invoked once, then cancelled.
	 */
	public static <T> Consumer<T> once(Consumer<? super T> delegate) {
		return new SingleUseConsumer<T>(delegate);
	}

	@Override
	public final void accept(T t) {
		if (called.get()) {
			return;
		}

		if (called.compareAndSet(false, true)) {
			if (null != delegate) {
				delegate.accept(t);
			} else {
				doAccept(t);
			}
			throw new CancelConsumerException();
		}
	}

	protected void doAccept(T t) {
	}

}
