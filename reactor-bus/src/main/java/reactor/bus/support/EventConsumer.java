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

package reactor.bus.support;

import reactor.bus.Event;
import reactor.core.support.Assert;
import reactor.fn.Consumer;

import javax.annotation.Nonnull;

/**
 * Simple {@link Consumer} implementation that pulls the data from an {@link reactor.bus.Event} and
 * passes it to a delegate {@link Consumer}.
 *
 * @param <T> the type of the event that can be handled by the consumer and the type that
 *            can be handled by the delegate
 * @author Jon Brisbin
 * @author Stephane Maldini
 */
public class EventConsumer<T> implements Consumer<Event<T>> {

	private final Consumer<T> delegate;

	/**
	 * Creates a new {@code EventConsumer} that will pass event data to the given {@code
	 * delegate}.
	 *
	 * @param delegate The delegate consumer
	 */
	public EventConsumer(@Nonnull Consumer<T> delegate) {
		Assert.notNull(delegate, "Delegate must not be null");
		this.delegate = delegate;
	}

	@Override
	public void accept(Event<T> ev) {
		delegate.accept(ev.getData());
	}

}
