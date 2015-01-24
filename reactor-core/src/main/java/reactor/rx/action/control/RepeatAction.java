/*
 * Copyright (c) 2011-2015 Pivotal Software Inc., Inc. All Rights Reserved.
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
package reactor.rx.action.control;

import org.reactivestreams.Publisher;
import reactor.Environment;
import reactor.core.Dispatcher;
import reactor.core.dispatch.SynchronousDispatcher;
import reactor.core.dispatch.TailRecurseDispatcher;
import reactor.fn.Consumer;
import reactor.rx.action.Action;
import reactor.rx.subscription.PushSubscription;

/**
 * @author Stephane Maldini
 * @since 2.0
 */
public class RepeatAction<T> extends Action<T, T> {

	private final long           numRetries;
	private long currentNumRetries = 0;
	private final Publisher<? extends T> rootPublisher;
	private Dispatcher             dispatcher;

	public RepeatAction(Dispatcher dispatcher, int numRetries, Publisher<? extends T> parentStream) {
		this.numRetries = numRetries;
		if (SynchronousDispatcher.INSTANCE == dispatcher) {
			this.dispatcher = Environment.tailRecurse();
		} else {
			this.dispatcher = dispatcher;
		}
		this.rootPublisher = parentStream;
	}

	@Override
	protected void doNext(T ev) {
		broadcastNext(ev);
	}

	@Override
	public final Dispatcher getDispatcher() {
		return dispatcher;
	}

	@Override
	public void onComplete() {
		dispatcher.dispatch(null, new Consumer<Void>() {
			@Override
			public void accept(Void nothing) {
				if (numRetries != -1 && ++currentNumRetries > numRetries) {
					doComplete();
					currentNumRetries = 0;
				} else {
					PushSubscription<T> upstream = upstreamSubscription;
					if (upstream != null) {
						long pendingRequests = upstream.pendingRequestSignals();
						if (rootPublisher != null) {
							if (TailRecurseDispatcher.class.isAssignableFrom(dispatcher.getClass())) {
								dispatcher.shutdown();
								dispatcher = Environment.tailRecurse();
							}
							cancel();
							rootPublisher.subscribe(RepeatAction.this);
							upstream = upstreamSubscription;
						}
						if (upstream != null && pendingRequests >= 0) {
							upstream.request(pendingRequests != Long.MAX_VALUE ? pendingRequests + 1 : pendingRequests);
						}
					}
				}

			}
		}, null);
	}
}
