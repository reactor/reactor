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

import org.reactivestreams.Publisher;
import org.reactivestreams.Subscription;
import reactor.event.dispatch.Dispatcher;
import reactor.function.Consumer;
import reactor.function.Predicate;
import reactor.rx.Stream;
import reactor.rx.subscription.PushSubscription;

import java.util.concurrent.atomic.AtomicLong;

/**
 * @author Stephane Maldini
 * @since 2.0
 */
public class RetryAction<T> extends Action<T, T> {

	private final long                 numRetries;
	private final Predicate<Throwable> retryMatcher;
	private long currentNumRetries = 0;
	private final Publisher<? extends T> rootPublisher;
	private       long                   pendingRequests;

	public RetryAction(Dispatcher dispatcher, int numRetries,
	                   Predicate<Throwable> predicate, Publisher<? extends T> parentStream) {
		super(dispatcher);
		this.numRetries = numRetries;
		this.retryMatcher = predicate;
		this.rootPublisher = parentStream;
	}

	@Override
	protected void requestUpstream(AtomicLong capacity, boolean terminated, long elements) {
		if ((pendingRequests += elements) < 0) pendingRequests = Long.MAX_VALUE;
		super.requestUpstream(capacity, terminated, elements);
	}

	@Override
	protected void doNext(T ev) {
		if (pendingRequests > 0l && pendingRequests != Long.MAX_VALUE) {
			pendingRequests--;
		}
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
					if (upstreamSubscription != null) {
						if (rootPublisher != null) {
							cancel();
							rootPublisher.subscribe(RetryAction.this);
						}
						if (pendingRequests > 0) {
							upstreamSubscription.request(pendingRequests);
						}
					}
				}

			}
		});
	}
}
