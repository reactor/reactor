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
import reactor.function.Consumer;
import reactor.function.Supplier;
import reactor.rx.Stream;
import reactor.rx.subscription.ReactiveSubscription;

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

	private final List<ReactiveSubscription<T>> currentWindows = new LinkedList<>();
	private final Supplier<? extends Publisher<?>> bucketClosing;
	private final Publisher<?>                     bucketOpening;

	public WindowShiftWhenAction(Dispatcher dispatcher, Publisher<?> bucketOpenings, Supplier<? extends Publisher<?>>
			boundarySupplier) {
		super(dispatcher);
		this.bucketClosing = boundarySupplier;
		this.bucketOpening = bucketOpenings;
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
						ReactiveSubscription<T> newBucket = createWindowStream();
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
		for (ReactiveSubscription<T> bucket : currentWindows) {
			bucket.onError(ev);
		}
		currentWindows.clear();
		super.doError(ev);
	}

	@Override
	protected void doComplete() {
		for (ReactiveSubscription<T> bucket : currentWindows) {
			bucket.onComplete();
		}
		currentWindows.clear();
		broadcastComplete();
	}

	@Override
	protected void doNext(T value) {
		if(!currentWindows.isEmpty()){
			for(ReactiveSubscription<T> bucket : currentWindows){
				bucket.onNext(value);
			}
		}
	}

	private class BucketConsumer implements Subscriber<Object> {

		final ReactiveSubscription<T> bucket;
		Subscription s;

		public BucketConsumer(ReactiveSubscription<T> bucket) {
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
					Iterator<ReactiveSubscription<T>> iterator = currentWindows.iterator();
					while(iterator.hasNext()){
						ReactiveSubscription<T> itBucket = iterator.next();
						if(itBucket == bucket){
							iterator.remove();
							bucket.onComplete();
							break;
						}
					}
				}
			});
		}
	}

	protected ReactiveSubscription<T> createWindowStream() {
		Action<T,T> action = Action.<T>broadcast(dispatcher, capacity).env(environment);
		ReactiveSubscription<T> _currentWindow = new ReactiveSubscription<T>(null, action);
		currentWindows.add(_currentWindow);
		action.onSubscribe(_currentWindow);
		broadcastNext(action);
		return _currentWindow;
	}


}
