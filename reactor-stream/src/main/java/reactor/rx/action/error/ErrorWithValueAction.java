/*
 * Copyright (c) 2011-2016 Pivotal Software Inc, All Rights Reserved.
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
package reactor.rx.action.error;

import org.reactivestreams.Publisher;
import reactor.core.error.Exceptions;
import reactor.fn.BiConsumer;

/**
 * @author Stephane Maldini
 */
final public class ErrorWithValueAction<T, E extends Throwable> extends FallbackAction<T> {

	private final BiConsumer<Object, ? super E> consumer;
	private final Class<E>       selector;

	public ErrorWithValueAction(Class<E> selector, BiConsumer<Object, ? super E> consumer, Publisher<?
			extends
			T> fallback) {
		super(fallback);
		this.consumer = consumer;
		this.selector = selector;
	}

	@Override
	protected void doNormalNext(T ev) {
		broadcastNext(ev);
	}

	@Override
	@SuppressWarnings("unchecked")
	protected void doError(Throwable cause) {
		if (selector.isAssignableFrom(cause.getClass())) {
			if (consumer != null) {
				consumer.accept(Exceptions.getFinalValueCause(cause), (E) cause);
			} else if (fallback != null) {
				doSwitch();
				return;
			}
		}
		super.doError(cause);
	}

	@Override
	public String toString() {
		return super.toString() + "{" +
				"catch-type=" + selector +
				'}';
	}
}
