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
package reactor.rx.action.aggregation;

import org.reactivestreams.Publisher;
import org.reactivestreams.Subscriber;
import org.reactivestreams.Subscription;
import reactor.Environment;
import reactor.core.Dispatcher;
import reactor.fn.Consumer;
import reactor.fn.Supplier;
import reactor.rx.Stream;
import reactor.rx.action.Action;
import reactor.rx.broadcast.BehaviorBroadcaster;
import reactor.rx.broadcast.Broadcaster;

import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;

/**
 * WindowAction is forwarding events on a steam until {@param backlog} is reached,
 * after that streams collected events further, complete it and create a fresh new stream.
 *
 * @author Stephane Maldini
 * @since 2.0
 */
public class WindowShiftWhenAction<T> extends Action<T, Stream<T>> {

	private final List<Broadcaster<T>> currentWindows = new LinkedList<>();
	private final Supplier<? extends Publisher<?>> bucketClosing;
	private final Publisher<?>                     bucketOpening;
	private final Environment                      environment;

	public WindowShiftWhenAction(Environment environment, Dispatcher dispatcher,
	                             Publisher<?> bucketOpenings, Supplier<? extends Publisher<?>>
			boundarySupplier) {
		super(dispatcher);
		this.bucketClosing = boundarySupplier;
		this.bucketOpening = bucketOpenings;
		this.environment = environment;
	}

	@Override
	protected void doSubscribe(Subscription subscription) {
		super.doSubscribe(subscription);

		bucketOpening.subscribe(new Subscriber<Object>() {
			Subscription s;

			@Override
			public void onSubscribe(Subscription s) {
				this.s = s;
				s.request(1l);
			}

			@Override
			public void onNext(Object o) {
				dispatch(new Consumer<Void>() {
					@Override
					public void accept(Void aVoid) {
						Broadcaster<T> newBucket = createWindowStream(null);
						bucketClosing.get().subscribe(new BucketConsumer(newBucket));
					}
				});

				if (s != null) {
					s.request(1);
				}
			}

			@Override
			public void onError(Throwable t) {
				if (s != null) {
					s.cancel();
				}
				WindowShiftWhenAction.this.onError(t);
			}

			@Override
			public void onComplete() {
				if (s != null) {
					s.cancel();
				}
				WindowShiftWhenAction.this.onComplete();
			}
		});

	}

	@Override
	protected void doError(Throwable ev) {
		for (Broadcaster<T> bucket : currentWindows) {
			bucket.onError(ev);
		}
		currentWindows.clear();
		super.doError(ev);
	}

	@Override
	protected void doComplete() {
		for (Broadcaster<T> bucket : currentWindows) {
			bucket.onComplete();
		}
		currentWindows.clear();
		broadcastComplete();
	}

	@Override
	protected void doNext(T value) {
		if (!currentWindows.isEmpty()) {
			for (Broadcaster<T> bucket : currentWindows) {
				bucket.onNext(value);
			}
		}
	}

	private class BucketConsumer implements Subscriber<Object> {

		final Broadcaster<T> bucket;
		Subscription s;

		public BucketConsumer(Broadcaster<T> bucket) {
			this.bucket = bucket;
		}

		@Override
		public void onSubscribe(Subscription s) {
			this.s = s;
			s.request(Long.MAX_VALUE);
		}

		@Override
		public void onNext(Object o) {
			onComplete();
		}

		@Override
		public void onError(Throwable t) {
			if (s != null) {
				s.cancel();
			}
			WindowShiftWhenAction.this.onError(t);
		}

		@Override
		public void onComplete() {
			if (s != null) {
				s.cancel();
			}
			dispatch(new Consumer<Void>() {
				@Override
				public void accept(Void aVoid) {
					Iterator<Broadcaster<T>> iterator = currentWindows.iterator();
					while (iterator.hasNext()) {
						Broadcaster<T> itBucket = iterator.next();
						if (itBucket == bucket) {
							iterator.remove();
							bucket.onComplete();
							break;
						}
					}
				}
			});
		}
	}

	@Override
	public Environment getEnvironment() {
		return environment;
	}

	protected Broadcaster<T> createWindowStream(T first) {
		Broadcaster<T> action = BehaviorBroadcaster.first(first, getEnvironment(), getDispatcher());
		currentWindows.add(action);
		broadcastNext(action);
		return action;
	}


}
