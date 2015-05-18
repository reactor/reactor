/*
 * Copyright (c) 2011-2015 Pivotal Software Inc, All Rights Reserved.
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
package reactor.core.reactivestreams;

import org.reactivestreams.Publisher;
import org.reactivestreams.Subscriber;
import org.reactivestreams.Subscription;
import reactor.core.support.Assert;
import reactor.core.support.Exceptions;
import reactor.fn.BiConsumer;
import reactor.fn.Consumer;
import reactor.fn.Function;

/**
 * A Reactive Streams {@link org.reactivestreams.Publisher} factory which callbacks on start, request and shutdown
 * <p>
 * The Publisher will directly forward all the signals passed to the subscribers and complete when onComplete is called.
 * <p>
 * Create such publisher with the provided factory, E.g.:
 * <pre>
 * {@code
 * PublisherFactory.create((n, sub) -> {
 *  for(int i = 0; i < n; i++){
 *    sub.onNext(i);
 *  }
 * }
 * }
 * </pre>
 *
 * @author Stephane Maldini
 * @since 2.0.2
 */
public class PublisherFactory<T, C> implements Publisher<T> {

	/**
	 * Create a {@link Publisher} reacting on requests with the passed {@link BiConsumer}
	 *
	 * @param requestConsumer A {@link BiConsumer} with left argument request and right argument target subscriber
	 * @param <T> The type of the data sequence
	 *
	 * @return a fresh Reactive Streams publisher ready to be subscribed
	 */
	public static <T> Publisher<T> create(BiConsumer<Long, SubscriberWithContext<T, Void>> requestConsumer) {
		return create(requestConsumer, null, null);
	}

	/**
	 * Create a {@link Publisher} reacting on requests with the passed {@link BiConsumer}
	 * The argument {@code contextFactory} is executed once by new subscriber to generate a context shared by every request calls.
	 *
	 * @param requestConsumer A {@link BiConsumer} with left argument request and right argument target subscriber
	 * @param contextFactory A {@link Function} called for every new subscriber returning an immutable context (IO connection...)
	 * @param <T> The type of the data sequence
	 * @param <C> The type of contextual information to be read by the requestConsumer
	 * @return a fresh Reactive Streams publisher ready to be subscribed
	 */
	public static <T, C> Publisher<T> create(BiConsumer<Long, SubscriberWithContext<T, C>> requestConsumer,
	                                         Function<Subscriber<? super T>, C> contextFactory) {
		return create(requestConsumer, contextFactory, null);
	}


	/**
	 * Create a {@link Publisher} reacting on requests with the passed {@link BiConsumer}.
	 * The argument {@code contextFactory} is executed once by new subscriber to generate a context shared by every request calls.
	 * The argument {@code shutdownConsumer} is executed once by subscriber termination event (cancel, onComplete, onError).
	 *
	 * @param requestConsumer A {@link BiConsumer} with left argument request and right argument target subscriber
	 * @param contextFactory A {@link Function} called once for every new subscriber returning an immutable context (IO connection...)
	 * @param shutdownConsumer A {@link Consumer} called once everytime a subscriber terminates: cancel, onComplete(), onError()
	 * @param <T> The type of the data sequence
	 * @param <C> The type of contextual information to be read by the requestConsumer
	 * @return a fresh Reactive Streams publisher ready to be subscribed
	 */
	public static <T, C> Publisher<T> create(BiConsumer<Long, SubscriberWithContext<T, C>> requestConsumer,
	                                         Function<Subscriber<? super T>, C> contextFactory,
	                                         Consumer<C> shutdownConsumer) {

		return new PublisherFactory<T, C>(requestConsumer, contextFactory, shutdownConsumer);
	}

	protected final Function<Subscriber<? super T>, C>            contextFactory;
	protected final BiConsumer<Long, SubscriberWithContext<T, C>> requestConsumer;
	protected final Consumer<C>                                   shutdownConsumer;

	protected PublisherFactory(BiConsumer<Long, SubscriberWithContext<T, C>> requestConsumer,
	                           Function<Subscriber<? super T>, C> contextFactory,
	                           Consumer<C> shutdownConsumer) {
		Assert.notNull(requestConsumer, "A data producer must be provided");
		this.requestConsumer = requestConsumer;
		this.contextFactory = contextFactory;
		this.shutdownConsumer = shutdownConsumer;
	}

	@Override
	public void subscribe(final Subscriber<? super T> subscriber) {
		try {
			final C context = contextFactory != null ? contextFactory.apply(subscriber) : null;
			subscriber.onSubscribe(new SubscriberProxy<>(subscriber, context, requestConsumer, shutdownConsumer));
		} catch (Throwable throwable) {
			Exceptions.throwIfFatal(throwable);
			subscriber.onError(throwable);
		}
	}

	private final static class SubscriberProxy<T, C> extends SubscriberWithContext<T, C> implements Subscription {

		private final BiConsumer<Long, SubscriberWithContext<T, C>> requestConsumer;
		private final Consumer<C>                                   shutdownConsumer;


		public SubscriberProxy(Subscriber<? super T> subscriber,
		                       C context,
		                       BiConsumer<Long, SubscriberWithContext<T, C>> requestConsumer,
		                       Consumer<C> shutdownConsumer
		) {
			super(context, subscriber);
			this.requestConsumer = requestConsumer;
			this.shutdownConsumer = shutdownConsumer;
		}

		@Override
		public void request(long n) {
			if(isCancelled())
				return;

			requestConsumer.accept(n, this);
		}

		@Override
		public void cancel() {
			if (TERMINAL_UPDATER.compareAndSet(this, 0, 1)) {
				doShutdown();
			}
		}

		@Override
		public void onError(Throwable t) {
			if (TERMINAL_UPDATER.compareAndSet(this, 0, 1)) {
				doShutdown();
				subscriber.onError(t);
			}
		}

		@Override
		public void onComplete() {
			if (TERMINAL_UPDATER.compareAndSet(this, 0, 1)) {
				doShutdown();
				try {
					subscriber.onComplete();
				} catch (Throwable t) {
					subscriber.onError(t);
				}
			}
		}

		private void doShutdown() {
			if (shutdownConsumer == null) return;

			try {
				shutdownConsumer.accept(context);
			} catch (Throwable t) {
				subscriber.onError(t);
			}
		}

		@Override
		public void onSubscribe(Subscription s) {
			throw new UnsupportedOperationException(" the delegate subscriber is already subscribed");
		}
	}
}
