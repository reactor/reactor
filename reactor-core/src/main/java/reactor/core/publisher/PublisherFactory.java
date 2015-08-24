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
package reactor.core.publisher;

import org.reactivestreams.Publisher;
import org.reactivestreams.Subscriber;
import org.reactivestreams.Subscription;
import reactor.core.subscriber.SubscriberBarrier;
import reactor.core.subscriber.SubscriberWithContext;
import reactor.core.support.Assert;
import reactor.core.error.Exceptions;
import reactor.core.error.SpecificationExceptions;
import reactor.fn.BiConsumer;
import reactor.fn.Consumer;
import reactor.fn.Function;

import java.util.concurrent.atomic.AtomicLongFieldUpdater;

/**
 * A Reactive Streams {@link org.reactivestreams.Publisher} factory which callbacks on start, request and shutdown
 * <p>
 * The Publisher will directly forward all the signals passed to the subscribers and complete when onComplete is called.
 * <p>
 * Create such publisher with the provided factory, E.g.:
 * <pre>
 * {@code
 * PublisherFactory.createWithDemand((n, sub) -> {
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
public abstract class PublisherFactory {

	/**
	 * Create a {@link Publisher} reacting on requests with the passed {@link BiConsumer}
	 *
	 * @param requestConsumer A {@link BiConsumer} with left argument request and right argument target subscriber
	 * @param <T>             The type of the data sequence
	 * @return a fresh Reactive Streams publisher ready to be subscribed
	 */
	public static <T> Publisher<T> createWithDemand(BiConsumer<Long, SubscriberWithContext<T, Void>> requestConsumer) {
		return createWithDemand(requestConsumer, null, null);
	}

	/**
	 * Create a {@link Publisher} reacting on requests with the passed {@link BiConsumer}
	 * The argument {@code contextFactory} is executed once by new subscriber to generate a context shared by every
	 * request calls.
	 *
	 * @param requestConsumer A {@link BiConsumer} with left argument request and right argument target subscriber
	 * @param contextFactory  A {@link Function} called for every new subscriber returning an immutable context (IO
	 *                        connection...)
	 * @param <T>             The type of the data sequence
	 * @param <C>             The type of contextual information to be read by the requestConsumer
	 * @return a fresh Reactive Streams publisher ready to be subscribed
	 */
	public static <T, C> Publisher<T> createWithDemand(BiConsumer<Long, SubscriberWithContext<T, C>> requestConsumer,
	                                                   Function<Subscriber<? super T>, C> contextFactory) {
		return createWithDemand(requestConsumer, contextFactory, null);
	}


	/**
	 * Create a {@link Publisher} reacting on requests with the passed {@link BiConsumer}.
	 * The argument {@code contextFactory} is executed once by new subscriber to generate a context shared by every
	 * request calls.
	 * The argument {@code shutdownConsumer} is executed once by subscriber termination event (cancel, onComplete,
	 * onError).
	 *
	 * @param requestConsumer  A {@link BiConsumer} with left argument request and right argument target subscriber
	 * @param contextFactory   A {@link Function} called once for every new subscriber returning an immutable context
	 *                         (IO connection...)
	 * @param shutdownConsumer A {@link Consumer} called once everytime a subscriber terminates: cancel, onComplete(),
	 *                         onError()
	 * @param <T>              The type of the data sequence
	 * @param <C>              The type of contextual information to be read by the requestConsumer
	 * @return a fresh Reactive Streams publisher ready to be subscribed
	 */
	public static <T, C> Publisher<T> createWithDemand(BiConsumer<Long, SubscriberWithContext<T, C>> requestConsumer,
	                                                   Function<Subscriber<? super T>, C> contextFactory,
	                                                   Consumer<C> shutdownConsumer) {

		return new ReactorPublisher<T, C>(requestConsumer, contextFactory, shutdownConsumer);
	}


	/**
	 * Create a {@link Publisher} reacting on each available {@link Subscriber} read derived with the passed {@link
	 * Consumer}. If a previous request is still running, avoid recursion and extend the previous request iterations.
	 *
	 * @param requestConsumer A {@link Consumer} invoked when available read with the target subscriber
	 * @param <T>             The type of the data sequence
	 * @return a fresh Reactive Streams publisher ready to be subscribed
	 */
	public static <T> Publisher<T> create(Consumer<SubscriberWithContext<T, Void>> requestConsumer) {
		return create(requestConsumer, null, null);
	}

	/**
	 * Create a {@link Publisher} reacting on each available {@link Subscriber} read derived with the passed {@link
	 * Consumer}. If a previous request is still running, avoid recursion and extend the previous request iterations.
	 * The argument {@code contextFactory} is executed once by new subscriber to generate a context shared by every
	 * request calls.
	 *
	 * @param requestConsumer A {@link Consumer} invoked when available read with the target subscriber
	 * @param contextFactory  A {@link Function} called for every new subscriber returning an immutable context (IO
	 *                        connection...)
	 * @param <T>             The type of the data sequence
	 * @param <C>             The type of contextual information to be read by the requestConsumer
	 * @return a fresh Reactive Streams publisher ready to be subscribed
	 */
	public static <T, C> Publisher<T> create(Consumer<SubscriberWithContext<T, C>> requestConsumer,
	                                         Function<Subscriber<? super T>, C> contextFactory) {
		return create(requestConsumer, contextFactory, null);
	}


	/**
	 * Create a {@link Publisher} reacting on each available {@link Subscriber} read derived with the passed {@link
	 * Consumer}. If a previous request is still running, avoid recursion and extend the previous request iterations.
	 * The argument {@code contextFactory} is executed once by new subscriber to generate a context shared by every
	 * request calls.
	 * The argument {@code shutdownConsumer} is executed once by subscriber termination event (cancel, onComplete,
	 * onError).
	 *
	 * @param requestConsumer  A {@link Consumer} invoked when available read with the target subscriber
	 * @param contextFactory   A {@link Function} called once for every new subscriber returning an immutable context
	 *                         (IO connection...)
	 * @param shutdownConsumer A {@link Consumer} called once everytime a subscriber terminates: cancel, onComplete(),
	 *                         onError()
	 * @param <T>              The type of the data sequence
	 * @param <C>              The type of contextual information to be read by the requestConsumer
	 * @return a fresh Reactive Streams publisher ready to be subscribed
	 */
	public static <T, C> Publisher<T> create(final Consumer<SubscriberWithContext<T, C>> requestConsumer,
	                                         Function<Subscriber<? super T>, C> contextFactory,
	                                         Consumer<C> shutdownConsumer) {
		Assert.notNull(requestConsumer, "A data producer must be provided");
		return new ForEachPublisher<T, C>(requestConsumer, contextFactory, shutdownConsumer);
	}


	/**
	 * Intercept a source {@link Publisher} onNext signal to eventually transform, forward or filter the data by
	 * calling
	 * or not
	 * the right operand {@link Subscriber}.
	 *
	 * @param dataConsumer A {@link BiConsumer} with left argument onNext data and right argument output subscriber
	 * @param <I>          The source type of the data sequence
	 * @param <O>          The target type of the data sequence
	 * @return a fresh Reactive Streams publisher ready to be subscribed
	 */
	public static <I, O> Publisher<O> barrier(Publisher<I> source, BiConsumer<I, Subscriber<? super O>> dataConsumer) {
		return barrier(source, dataConsumer, null, null);
	}

	/**
	 * Intercept a source {@link Publisher} onNext signal to eventually transform, forward or filter the data by
	 * calling
	 * or not
	 * the right operand {@link Subscriber}.
	 *
	 * @param dataConsumer  A {@link BiConsumer} with left argument onNext data and right argument output subscriber
	 * @param errorConsumer A {@link BiConsumer} with left argument onError throwable and right argument output sub
	 * @param <I>           The source type of the data sequence
	 * @param <O>           The target type of the data sequence
	 * @return a fresh Reactive Streams publisher ready to be subscribed
	 */
	public static <I, O> Publisher<O> barrier(Publisher<I> source,
	                                          BiConsumer<I, Subscriber<? super O>> dataConsumer,
	                                          BiConsumer<Throwable, Subscriber<? super O>> errorConsumer) {
		return barrier(source, dataConsumer, errorConsumer, null);
	}


	/**
	 * Intercept a source {@link Publisher} onNext signal to eventually transform, forward or filter the data by
	 * calling
	 * or not
	 * the right operand {@link Subscriber}.
	 * <p>
	 * The argument {@code subscriptionHandler} is executed once by new subscriber to generate a context shared by
	 * every
	 * request calls.
	 *
	 * @param dataConsumer     A {@link BiConsumer} with left argument onNext data and right argument output subscriber
	 * @param errorConsumer    A {@link BiConsumer} with left argument onError throwable and right argument output sub
	 * @param completeConsumer A {@link Consumer} called onComplete with the actual output subscriber
	 * @param <I>              The source type of the data sequence
	 * @param <O>              The target type of the data sequence
	 * @return a fresh Reactive Streams publisher ready to be subscribed
	 */
	public static <I, O> Publisher<O> barrier(Publisher<I> source,
	                                          final BiConsumer<I, Subscriber<? super O>> dataConsumer,
	                                          final BiConsumer<Throwable, Subscriber<? super O>> errorConsumer,
	                                          final Consumer<Subscriber<? super O>> completeConsumer) {
		return intercept(
		  source,
		  new Function<Subscriber<? super O>, SubscriberBarrier<I, O>>() {
			  @Override
			  public SubscriberBarrier<I, O> apply(final Subscriber<? super O> subscriber) {
				  return new ConsumerSubscriberBarrier<>(subscriber, dataConsumer, errorConsumer, completeConsumer);
			  }
		  }
		);
	}

	/**
	 * Create a {@link Publisher} intercepting all source signals with a {@link SubscriberBarrier} per Subscriber
	 * provided by the given barrierProvider.
	 *
	 * @param source          A {@link Publisher} source delegate
	 * @param barrierProvider A {@link Function} called once for every new subscriber returning a unique {@link
	 *                        SubscriberBarrier}
	 * @param <I>             The type of the data sequence
	 * @param <O>             The type of contextual information to be read by the requestConsumer
	 * @return a fresh Reactive Streams publisher ready to be subscribed
	 */
	public static <I, O> Publisher<O> intercept(Publisher<? extends I> source,
	                                            Function<Subscriber<? super O>, SubscriberBarrier<I, O>>
	                                              barrierProvider) {
		Assert.notNull(source, "A data source must be provided");
		Assert.notNull(barrierProvider, "A barrier interceptor must be provided");
		return new ProxyPublisher<>(source, barrierProvider);
	}

	/**
	 * A singleton noop subscription
	 */
	protected static final Subscription NOOP_SUBSCRIPTION = new Subscription() {
		@Override
		public void request(long n) {
		}

		@Override
		public void cancel() {
		}
	};

	private static class ReactorPublisher<T, C> implements Publisher<T> {

		protected final Function<Subscriber<? super T>, C>            contextFactory;
		protected final BiConsumer<Long, SubscriberWithContext<T, C>> requestConsumer;
		protected final Consumer<C>                                   shutdownConsumer;

		protected ReactorPublisher(BiConsumer<Long, SubscriberWithContext<T, C>> requestConsumer,
		                           Function<Subscriber<? super T>, C> contextFactory,
		                           Consumer<C> shutdownConsumer) {
			this.requestConsumer = requestConsumer;
			this.contextFactory = contextFactory;
			this.shutdownConsumer = shutdownConsumer;
		}

		@Override
		final public void subscribe(final Subscriber<? super T> subscriber) {
			try {
				final C context = contextFactory != null ? contextFactory.apply(subscriber) : null;
				subscriber.onSubscribe(createSubscription(subscriber, context));
			} catch (PrematureCompleteException pce) {
				//IGNORE
			} catch (Throwable throwable) {
				Exceptions.throwIfFatal(throwable);
				subscriber.onError(throwable);
			}
		}

		protected Subscription createSubscription(Subscriber<? super T> subscriber, C context) {
			return new SubscriberProxy<>(subscriber, context, requestConsumer, shutdownConsumer);
		}
	}

	private static final class ForEachPublisher<T, C> extends ReactorPublisher<T, C> {

		final Consumer<SubscriberWithContext<T, C>> forEachConsumer;


		public ForEachPublisher(Consumer<SubscriberWithContext<T, C>> forEachConsumer, Function<Subscriber<? super
		  T>, C> contextFactory, Consumer<C> shutdownConsumer) {
			super(null, contextFactory, shutdownConsumer);
			this.forEachConsumer = forEachConsumer;
		}

		@Override
		protected Subscription createSubscription(Subscriber<? super T> subscriber, C context) {
			return new SubscriberProxy<>(subscriber, context, new ForEachBiConsumer<>(forEachConsumer),
			  shutdownConsumer);
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
			if (isCancelled()) {
				return;
			}

			if (n <= 0) {
				onError(SpecificationExceptions.spec_3_09_exception(n));
				return;
			}

			try {
				requestConsumer.accept(n, this);
			} catch (Throwable t) {
				onError(t);
			}
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

		@Override
		public String toString() {
			return context != null ? context.toString() : ("SubscriberProxy{" +
			  "requestConsumer=" + requestConsumer +
			  ", shutdownConsumer=" + shutdownConsumer +
			  '}');
		}
	}

	private final static class ForEachBiConsumer<T, C> implements BiConsumer<Long, SubscriberWithContext<T, C>> {

		private final Consumer<SubscriberWithContext<T, C>> requestConsumer;

		private volatile long pending = 0L;

		private final static AtomicLongFieldUpdater<ForEachBiConsumer> PENDING_UPDATER =
		  AtomicLongFieldUpdater.newUpdater(ForEachBiConsumer.class, "pending");

		public ForEachBiConsumer(Consumer<SubscriberWithContext<T, C>> requestConsumer) {
			this.requestConsumer = requestConsumer;
		}

		@Override
		public void accept(Long n, SubscriberWithContext<T, C> sub) {

			if (pending == Long.MAX_VALUE) {
				return;
			}

			long demand = n;
			long afterAdd;
			if (!PENDING_UPDATER.compareAndSet(this, 0L, demand)
			  && (afterAdd = PENDING_UPDATER.addAndGet(this, demand)) != demand) {
				if (afterAdd < 0L) {
					if (!PENDING_UPDATER.compareAndSet(this, afterAdd, Long.MAX_VALUE)) {
						return;
					}
				} else {
					return;
				}
			}

			do {
				long requestCursor = 0l;
				while ((requestCursor++ < demand || demand == Long.MAX_VALUE) && !sub.isCancelled()) {
					requestConsumer.accept(sub);
				}
			} while ((demand = PENDING_UPDATER.addAndGet(this, -demand)) > 0L && !sub.isCancelled());

		}
	}

	private final static class ProxyPublisher<I, O> implements Publisher<O> {

		final private Publisher<? extends I>                                   source;
		final private Function<Subscriber<? super O>, SubscriberBarrier<I, O>> barrierProvider;

		public ProxyPublisher(Publisher<? extends I> source,
		                      Function<Subscriber<? super O>, SubscriberBarrier<I, O>> barrierProvider) {
			this.source = source;
			this.barrierProvider = barrierProvider;
		}

		@Override
		public void subscribe(Subscriber<? super O> s) {
			if (s == null) {
				throw SpecificationExceptions.spec_2_13_exception();
			}
			source.subscribe(barrierProvider.apply(s));
		}

		@Override
		public String toString() {
			return "ProxyPublisher{" +
			  "source=" + source +
			  '}';
		}
	}

	private static final class ConsumerSubscriberBarrier<I, O> extends SubscriberBarrier<I, O> {
		private final BiConsumer<I, Subscriber<? super O>>         dataConsumer;
		private final BiConsumer<Throwable, Subscriber<? super O>> errorConsumer;
		private final Consumer<Subscriber<? super O>>              completeConsumer;

		public ConsumerSubscriberBarrier(Subscriber<? super O> subscriber, BiConsumer<I, Subscriber<? super O>>
		  dataConsumer, BiConsumer<Throwable, Subscriber<? super O>> errorConsumer, Consumer<Subscriber<? super O>>
		                                   completeConsumer) {
			super(subscriber);
			this.dataConsumer = dataConsumer;
			this.errorConsumer = errorConsumer;
			this.completeConsumer = completeConsumer;
		}

		@Override
		protected void doNext(I o) {
			if (dataConsumer != null) {
				dataConsumer.accept(o, subscriber);
			} else {
				super.doNext(o);
			}
		}

		@Override
		protected void doError(Throwable throwable) {
			if (errorConsumer != null) {
				errorConsumer.accept(throwable, subscriber);
			} else {
				super.doError(throwable);
			}
		}

		@Override
		protected void doComplete() {
			if (completeConsumer != null) {
				completeConsumer.accept(subscriber);
			} else {
				super.doComplete();
			}
		}

		@Override
		public String toString() {
			return "ConsumerSubscriberBarrier{" +
			  "subscriber=" + subscriber +
			  ", dataConsumer=" + dataConsumer +
			  ", errorConsumer=" + errorConsumer +
			  ", completeConsumer=" + completeConsumer +
			  '}';
		}
	}

	public static class PrematureCompleteException extends RuntimeException {
		static public final PrematureCompleteException INSTANCE = new PrematureCompleteException();

		private PrematureCompleteException() {
		}

		@Override
		public synchronized Throwable fillInStackTrace() {
			return this;
		}
	}
}
