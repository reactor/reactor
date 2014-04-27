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
import reactor.function.Predicate;
import reactor.rx.Stream;

/**
 * @author Stephane Maldini
 * @since 1.1
 */
public class FlushWhenAction<T> extends Action<T, Void> {

	private final Predicate<T> consumer;
	private final Stream<T> flushOutput;

	public FlushWhenAction(Predicate<T> consumer, Dispatcher d, Stream<T> flushOutput) {
		super(d);
		this.flushOutput = flushOutput;
		this.consumer = consumer;
	}

	@Override
	protected void doNext(T value) {
		if (consumer.test(value)) {
			flushOutput.broadcastFlush();
		}
	}

	@Override
	public void onComplete() {
		//IGNORE
	}
}
