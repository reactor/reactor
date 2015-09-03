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
import reactor.fn.Consumer;

/**
 * Simple {@link reactor.bus.Event} implementation that attaches a callback to an {@link reactor.bus.Event} and
 * passes it to a delegate {@link Consumer}.
 *
 * @param <T> the type of the event payload
 * @author Stephane Maldini
 */
public class CallbackEvent<T> extends Event<T> {
	private static final long serialVersionUID = -7173643160887108377L;
	final Consumer callback;

	public CallbackEvent(T data, Consumer callback) {
		this(null, data, callback);
	}

	public CallbackEvent(Headers headers, T data, Consumer callback) {
		this(headers, data, callback, null);
	}

	public CallbackEvent(Headers headers, T data, Consumer callback, Consumer<Throwable> throwableConsumer) {
		super(headers, data, throwableConsumer);
		this.callback = callback;
	}

	@Override
	public <X> Event<X> copy(X data) {
		if (null != getReplyTo())
			return new CallbackEvent<X>(getHeaders(), data, callback, getErrorConsumer()).setReplyTo(getReplyTo());
		else
			return new CallbackEvent<X>(getHeaders(), data, callback, getErrorConsumer());
	}


	/**
	 * Trigger callback with current payload
	 */
	@SuppressWarnings("unchecked")
	public void callback() {
		if (null != callback) {
			callback.accept(getData());
		}
	}
}
