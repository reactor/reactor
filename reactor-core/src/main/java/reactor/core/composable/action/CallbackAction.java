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
import reactor.function.Consumer;

/**
 * @author Stephane Maldini
 */
public class CallbackAction<T> extends Action<T, Void> {

	private final Consumer<T> consumer;

	public CallbackAction(Dispatcher dispatcher, Consumer<T> consumer) {
		super(dispatcher, null);
		this.consumer = consumer;
	}

	@Override
	public void doNext(T ev) {
		consumer.accept(ev);
		available();
	}

	@Override
	public void onComplete() {
		//IGNORE;
	}

}
