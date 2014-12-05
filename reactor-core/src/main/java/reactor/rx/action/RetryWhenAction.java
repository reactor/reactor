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

import org.reactivestreams.Publisher;
import org.reactivestreams.Subscriber;
import org.reactivestreams.Subscription;
import reactor.core.Dispatcher;
import reactor.core.Environment;
import reactor.function.Consumer;
import reactor.function.Function;
import reactor.rx.Stream;
import reactor.rx.Streams;
import reactor.rx.action.support.NonBlocking;
import reactor.rx.stream.Broadcaster;

/**
 * @author Stephane Maldini
 * @since 2.0
 */
public class RetryWhenAction<T> extends Action<T, T> {

	private final Broadcaster<Throwable> retryStream;
	private final Publisher<? extends T> rootPublisher;

	private long pendingRequests;

	public RetryWhenAction(Dispatcher dispatcher,
	                       Function<Stream<? extends Throwable>, ? extends Publisher<?>> predicate, Publisher<? extends
			T> rootPublisher) {
		super(dispatcher);
		this.retryStream = Streams.broadcast(null, dispatcher);
		this.rootPublisher = rootPublisher;
		Publisher<?> afterRetryPublisher = predicate.apply(retryStream);
		afterRetryPublisher.subscribe(new RestartSubscriber());
	}

	@Override
	public Action<T, T> env(Environment environment) {
		retryStream.env(environment);
		return super.env(environment);
	}

	@Override
	protected void requestUpstream(long capacity, boolean terminated, long elements) {
		if ((pendingRequests += elements) < 0) pendingRequests = Long.MAX_VALUE;
		super.requestUpstream(capacity, terminated, elements);
	}

	@Override
	protected void doNext(T ev) {
		if (pendingRequests > 0l && pendingRequests != Long.MAX_VALUE) {
			pendingRequests--;
		}
		broadcastNext(ev);
	}

	@Override
	protected void doComplete() {
		retryStream.broadcastComplete();
		super.doComplete();
	}

	protected void doRetry() {
		trySyncDispatch(null, new Consumer<Object>() {
			@Override
			public void accept(Object o) {
				if (rootPublisher != null) {
					if(upstreamSubscription == null) {
						rootPublisher.subscribe(RetryWhenAction.this);
					}
				}
				if (pendingRequests > 0) {
					upstreamSubscription.request(pendingRequests);
				}
			}
		});
	}

	@Override
	@SuppressWarnings("unchecked")
	public void onError(Throwable cause) {
		cancel();
		retryStream.broadcastNext(cause);
	}

	private class RestartSubscriber implements Subscriber<Object>, NonBlocking {
		Subscription s;

		@Override
		public Dispatcher getDispatcher() {
			return dispatcher;
		}

		@Override
		public long getCapacity() {
			return capacity;
		}

		@Override
		public void onSubscribe(Subscription s) {
			this.s = s;
			s.request(1l);
		}

		@Override
		public void onNext(Object o) {
			//s.cancel();
			//publisher.subscribe(this);
			doRetry();
			s.request(1l);
		}

		@Override
		public void onError(Throwable t) {
			s.cancel();
			RetryWhenAction.this.onError(t);
		}

		@Override
		public void onComplete() {
			RetryWhenAction.this.onComplete();
		}
	}
}
