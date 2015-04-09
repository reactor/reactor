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
import org.reactivestreams.Subscriber;
import org.reactivestreams.Subscription;
import reactor.Environment;
import reactor.core.Dispatcher;
import reactor.core.dispatch.SynchronousDispatcher;
import reactor.core.dispatch.TailRecurseDispatcher;
import reactor.core.support.NonBlocking;
import reactor.fn.Consumer;
import reactor.fn.Function;
import reactor.rx.Stream;
import reactor.rx.action.Action;
import reactor.rx.broadcast.Broadcaster;
import reactor.rx.subscription.PushSubscription;

/**
 * @author Stephane Maldini
 * @since 2.0
 */
public class RepeatWhenAction<T> extends Action<T, T> {

	private final Broadcaster<Long>      retryStream;
	private final Publisher<? extends T> rootPublisher;
	private Dispatcher             dispatcher;
	private long pendingRequests = 0l;

	public RepeatWhenAction(Dispatcher dispatcher,
	                        Function<? super Stream<? extends Long>, ? extends Publisher<?>> predicate,
	                        Publisher<? extends T> rootPublisher) {
		this.retryStream = Broadcaster.create(null, dispatcher);
		if (SynchronousDispatcher.INSTANCE == dispatcher) {
			this.dispatcher = Environment.tailRecurse();
		} else {
			this.dispatcher = dispatcher;
		}

		this.rootPublisher = rootPublisher;
		Publisher<?> afterRetryPublisher = predicate.apply(retryStream);
		afterRetryPublisher.subscribe(new RestartSubscriber());
	}

	@Override
	protected void doNext(T ev) {
		broadcastNext(ev);
	}

	@Override
	protected void doComplete() {
		retryStream.onComplete();
		super.doComplete();
	}

	@Override
	public void requestMore(long n) {
		synchronized (this) {
			if ((pendingRequests += n) < 0l) {
				pendingRequests = Long.MAX_VALUE;
			}
		}
		super.requestMore(n);
	}

	@Override
	protected void doOnSubscribe(Subscription subscription) {
		long pendingRequests = this.pendingRequests;
		if(pendingRequests > 0) {
			subscription.request(pendingRequests);
		}
	}


	protected void doRetry() {
		dispatcher.dispatch(null, new Consumer<Void>() {
			@Override
			public void accept(Void aVoid) {
				if (rootPublisher != null) {
					if (TailRecurseDispatcher.class.isAssignableFrom(dispatcher.getClass())) {
						dispatcher.shutdown();
						dispatcher = Environment.tailRecurse();
					}
					rootPublisher.subscribe(RepeatWhenAction.this);
				}
			}
		}, null);

	}

	@Override
	public void onComplete() {
		try {
			cancel();
			retryStream.onNext(System.currentTimeMillis());
		}catch(Exception e){
			doError(e);
		}
	}

	@Override
	public final Dispatcher getDispatcher() {
		return dispatcher;
	}

	private class RestartSubscriber implements Subscriber<Object>, NonBlocking {
		Subscription s;

		@Override
		public boolean isReactivePull(Dispatcher dispatcher, long producerCapacity) {
			return RepeatWhenAction.this.isReactivePull(dispatcher, producerCapacity);
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
			RepeatWhenAction.this.doError(t);
		}

		@Override
		public void onComplete() {
			s.cancel();
			RepeatWhenAction.this.doComplete();
		}
	}
}
