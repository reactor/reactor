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
package reactor.rx.stream;

import org.reactivestreams.Subscriber;
import reactor.event.dispatch.Dispatcher;
import reactor.function.Consumer;
import reactor.function.Supplier;
import reactor.rx.Stream;
import reactor.rx.StreamSubscription;

/**
 * A Stream that returns the result from {@link reactor.function.Supplier#get()} everytime it is requested via
 * {@link org.reactivestreams.Subscription#request(long)}.
 * <p>
 * The Stream will end when the result is null or {@link reactor.rx.Stream#broadcastComplete()} is called.
 * <p>
 * Create such stream with the provided factory, E.g.:
 * {@code
 * Streams.generate(env, (-> randomNumber()))
 *    .throttle(200)
 *    .consume(System.out::println)
 * }
 * <p>
 * This example will generate a random number every 200ms
 *
 * @author Stephane Maldini
 */
public final class SupplierStream<T> extends Stream<T> {

	private final Supplier<? extends T> supplier;
	private final Consumer<Void> supplierConsumer = new SupplierConsumer();

	public SupplierStream(Dispatcher dispatcher, Supplier<? extends T> supplier) {
		super(dispatcher);
		this.supplier = supplier;
		this.keepAlive = false;
	}

	@Override
	protected void onShutdown() {
		//IGNORE
	}

	@Override
	protected void subscribeWithSubscription(final Subscriber<? super T> subscriber,
	                                      final StreamSubscription<T> streamSubscription) {
		if (supplier != null) {
			super.subscribeWithSubscription(subscriber, streamSubscription);
		} else {
			subscriber.onComplete();
		}
	}

	@Override
	protected StreamSubscription<T> createSubscription(Subscriber<? super T> subscriber, boolean reactivePull) {
		return new StreamSubscription.Firehose<T>(this, subscriber) {
			@Override
			public void request(long elements) {
				dispatch(supplierConsumer);
			}
		};
	}

	private class SupplierConsumer implements Consumer<Void> {
		@Override
		public void accept(Void aVoid) {
			try {
				T supplied = supplier.get();
				if (supplied != null) {
					broadcastNext(supplied);
				} else {
					broadcastComplete();
				}
			} catch (Throwable throwable) {
				broadcastError(throwable);
			}
		}
	}
}
