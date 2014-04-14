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
package reactor.core.composable.action;

import reactor.event.dispatch.Dispatcher;
import reactor.event.selector.ClassSelector;
import reactor.function.Consumer;

/**
 * @author Stephane Maldini
 * @since 1.1
 */
public class CompleteAction<T, E> extends Action<T, E> {

	private final Consumer<E> consumer;
	private final E           input;

	public CompleteAction(Dispatcher dispatcher, E input, Consumer<E> consumer) {
		super(dispatcher, null);
		this.consumer = consumer;
		this.input = input;
	}

	@Override
	public void doNext(Object ev) {
		//IGNORE
	}

	@Override
	protected void doError(Throwable ev) {
		consumer.accept(input);
	}

	@Override
	@SuppressWarnings("unchecked")
	public void doComplete() {
		consumer.accept(input);
	}

	@Override
	public String toString() {
		return "Complete (" +
				"Input=" + input +
				')';
	}
}
