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
import reactor.function.Consumer;
import reactor.function.Predicate;
import reactor.rx.Stream;
import reactor.rx.subscription.PushSubscription;

/**
 * @author Stephane Maldini
 * @since 2.0
 */
public class RetryAction<T> extends Action<T, T> {

	private final long                 numRetries;
	private final Predicate<Throwable> retryMatcher;
	private long currentNumRetries = 0;

	public RetryAction(Dispatcher dispatcher, int numRetries) {
		this(dispatcher, numRetries, null);
	}

	public RetryAction(Dispatcher dispatcher, int numRetries,
	                   Predicate<Throwable> predicate) {
		super(dispatcher);
		this.numRetries = numRetries;
		this.retryMatcher = predicate;
	}

	@Override
	protected void doNext(T ev) {
		currentNumRetries = 0;
		broadcastNext(ev);
	}

	@Override
	@SuppressWarnings("unchecked")
	public void onError(Throwable cause) {
		trySyncDispatch(cause, new Consumer<Throwable>() {
			@Override
			public void accept(Throwable throwable) {
				if (++currentNumRetries > numRetries && (retryMatcher == null || !retryMatcher.test(throwable))) {
					doError(throwable);
					currentNumRetries = 0;
				} else {
					if (subscription != null) {
						Action<?, ?> rootAction = findOldestUpstream(Action.class, true);

						if (rootAction.subscription != null
								&& PushSubscription.class.isAssignableFrom(rootAction.subscription.getClass())) {

							Stream originalStream = ((PushSubscription<?>) rootAction.subscription).getPublisher();
							rootAction.cancel();
							originalStream.subscribe(rootAction);

						}
						requestConsumer.accept(capacity);
					}
				}

			}
		});
	}
}
