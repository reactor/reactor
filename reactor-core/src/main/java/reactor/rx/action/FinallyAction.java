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
package reactor.rx.action;

import reactor.event.dispatch.Dispatcher;
import reactor.function.Consumer;

/**
 * @author Stephane Maldini
 * @since 2.0
 */
public class FinallyAction<T, C> extends Action<T, T> {

	private final Consumer<? super C> consumer;
	private final C           input;

	public FinallyAction(Dispatcher dispatcher, C input, Consumer<? super C> consumer) {
		super(dispatcher);
		this.consumer = consumer;
		this.input = input;
	}

	@Override
	protected void doNext(T ev) {
		broadcastNext(ev);
	}

	@Override
	protected void doError(Throwable ev) {
		consumer.accept(input);
		broadcastError(ev);
	}

	@Override
	protected void doComplete() {
		consumer.accept(input);
		broadcastComplete();
	}

}
