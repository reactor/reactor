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
package reactor.function.support;

import reactor.function.Consumer;
import reactor.function.Supplier;

import java.util.concurrent.atomic.AtomicReference;

/**
 * A {@code Tap} provides a limited window into an event stream. Using a {@code Tap} one can
 * inspect the current event passing through a stream. A {@code Tap}'s value will be
 * continually updated as data passes through the stream, so a call to {@link #get()} will
 * return the last value seen by the event stream.
 *
 * @param <T> the type of values that this Tap can consume and supply
 *
 * @author Stephane Maldini
 * @author Jon Brisbin
 */
public class Tap<T> implements Consumer<T>, Supplier<T> {

	private final AtomicReference<T> value = new AtomicReference<T>();

	/**
	 * Create a {@code Tap}.
	 */
	public Tap() {
	}

	/**
	 * Get the value of this {@code Tap}, which is the current value of the event stream this
	 * tap is consuming.
	 *
	 * @return the value
	 */
	@Override
	public T get() {
		return value.get();
	}

	@Override
	public void accept(T value) {
		this.value.set(value);
	}

}
