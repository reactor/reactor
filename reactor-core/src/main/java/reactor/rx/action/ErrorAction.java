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
package reactor.rx.action;

import reactor.event.dispatch.Dispatcher;
import reactor.event.selector.ClassSelector;
import reactor.function.Consumer;
import reactor.rx.Stream;

/**
 * @author Stephane Maldini
 * @since 1.1
 */
public class ErrorAction<T, E extends Throwable> extends Action<T, Void> {

	private final Consumer<E>   consumer;
	private final ClassSelector selector;

	public ErrorAction(Dispatcher dispatcher, ClassSelector selector, Consumer<E> consumer) {
		super(dispatcher);
		this.consumer = consumer;
		this.selector = selector;
	}

	@Override
	protected void doNext(Object ev) {
		//IGNORE
	}

	@Override
	@SuppressWarnings("unchecked")
	protected void doError(Throwable cause) {
		if (selector.matches(cause.getClass())) {
			consumer.accept((E) cause);
		}
	}

	@Override
	public String toString() {
		return "{" +
				"catch-type=" + selector.getObject()+", "+
				"prefetch=" + getBatchSize() +
				'}';
	}
}
