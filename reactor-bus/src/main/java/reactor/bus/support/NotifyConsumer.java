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

import reactor.bus.Bus;
import reactor.bus.Event;
import reactor.bus.EventBus;
import reactor.core.support.Assert;
import reactor.fn.Consumer;

/**
 * A {@code Consumer} that notifies an observable of each value that it has accepted.
 *
 * @param <T> the type of the values that the consumer can accept
 * @author Jon Brisbin
 * @author Stephane Maldini
 */
public class NotifyConsumer<T> implements Consumer<T> {

	private final Object  notifyKey;
	private final Bus     observable;
	private final boolean wrapEvent;

	/**
	 * Creates a new {@code NotifyConsumer} that will notify the given {@code observable} using
	 * the given {@code notifyKey}. If {@code notifyKey} is {@code null}, {@code observable}
	 * will be notified without a key.
	 *
	 * @param notifyKey  The notification key, may be {@code null}
	 * @param observable The observable to notify. May not be {@code null}
	 */
	public NotifyConsumer(Object notifyKey, Bus<?, ?> observable) {
		Assert.notNull(observable, "Observable cannot be null.");
		this.notifyKey = notifyKey;
		this.observable = observable;
		this.wrapEvent = EventBus.class.isAssignableFrom(observable.getClass());
	}

	@Override
	@SuppressWarnings("unchecked")
	public void accept(T t) {
		observable.notify(notifyKey, (wrapEvent ? Event.wrap(t) : t));
	}

}
