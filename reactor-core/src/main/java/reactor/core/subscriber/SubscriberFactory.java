/*
 * Copyright (c) 2011-2016 Pivotal Software Inc, All Rights Reserved.
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
package reactor.core.subscriber;

import org.reactivestreams.Publisher;
import org.reactivestreams.Subscriber;
import org.reactivestreams.Subscription;
import reactor.core.error.CancelException;
import reactor.core.error.Exceptions;
import reactor.core.error.ReactorFatalException;
import reactor.core.publisher.PublisherFactory;
import reactor.core.subscription.SubscriptionWithContext;
import reactor.core.support.Assert;
import reactor.core.support.BackpressureUtils;
import reactor.core.support.Bounded;
import reactor.core.support.Publishable;
import reactor.fn.BiConsumer;
import reactor.fn.Consumer;
import reactor.fn.Function;

import java.util.concurrent.atomic.AtomicLong;

/**
 * A Reactive Streams {@link Subscriber} factory which callbacks on start, onNext, onError and shutdown
 * <p>
 * The Publisher will directly forward all the signals passed to the subscribers and complete when onComplete is called.
 * <p>
 * Create such publisher with the provided factory, E.g.:
 * <pre>
 * {@code
 * PublisherFactory.create(sub ->
 *    sub.onNext("hello")
 * ).subscribe( SubscriberFactory.unbounded(
 *    System.out::println
 * ));
 * }
 * </pre>
 *
 * @author Stephane Maldini
 * @since 2.0.3
 */
public abstract class SubscriberFactory {

	/**
	 * Create a {@link Subscriber} reacting onSubscribe with the passed {@link Consumer}
	 *
	 * @param subscriptionHandler A {@link Consumer} called once for every new subscription returning
	 *                            (IO connection...)
	 * @param <T>                 The type of the data sequence
	 * @return a fresh Reactive Streams subscriber ready to be subscribed
	 */
	public static <T> Subscriber<T> create(final Consumer<Subscription> subscriptionHandler) {
		return create(new Function<Subscription, Void>() {
			@Override
			public Void apply(Subscription subscription) {
				subscriptionHandler.accept(subscription);
				return null;
			}
		}, null, null, null);
	}


	/**
	 * Create a {@link Subscriber} reacting onSubscribe and onNext, eventually sharing a context.
	 * The argument {@code subscriptionHandler} is executed once onSubscribe to generate a context shared by every
	 * onNext calls.
	 *
	 * @param subscriptionHandler A {@link Function} called once for every new subscription returning an immutable
	 *                            context
	 * @param dataConsumer        A {@link BiConsumer} with left argument onNext data and right argument upstream
	 *                            subscription
	 *                            (IO connection...)
	 * @param <T>                 The type of the data sequence
	 * @param <C>                 The type of contextual information to be read by the requestConsumer
	 * @return a fresh Reactive Streams subscriber ready to be subscribed
	 */
	public static <T, C> Subscriber<T> create(Function<Subscription, C> subscriptionHandler,
	                                          BiConsumer<T, SubscriptionWithContext<C>> dataConsumer) {
		return create(subscriptionHandler, dataConsumer, null, null);
	}

	/**
	 * Create a {@link Subscriber} reacting onNext, onError.
	 * <p>
	 * The argument {@code subscriptionHandler} is executed onSubscribe to
	 * request initial data on the subscription and eventually generate a context shared by every
	 * request calls.
	 *
	 * @param subscriptionHandler A {@link Function} called once for every new subscription returning an immutable
	 *                            context
	 * @param dataConsumer        A {@link BiConsumer} with left argument onNext data and right argument upstream
	 *                            subscription
	 *                            (IO connection...)
	 * @param errorConsumer       A {@link Consumer} called onError
	 * @param <T>                 The type of the data sequence
	 * @param <C>                 The type of contextual information to be read by the requestConsumer
	 * @return a fresh Reactive Streams subscriber ready to be subscribed
	 */
	public static <T, C> Subscriber<T> create(Function<Subscription, C> subscriptionHandler,
	                                          BiConsumer<T, SubscriptionWithContext<C>> dataConsumer,
	                                          BiConsumer<Throwable, C> errorConsumer) {
		return create(subscriptionHandler, dataConsumer, errorConsumer, null);
	}

	/**
	 * Create a {@link Subscriber} that will will automatically request Long.MAX_VALUE onSubscribe.
	 *
	 * @param <T> The type of the data sequence
	 * @return a fresh Reactive Streams subscriber ready to be subscribed
	 */
	public static <T> Subscriber<T> unbounded() {
		return unbounded(null, null, null);
	}

	/**
	 * Create a {@link Subscriber} reacting onNext. The subscriber will automatically
	 * request Long.MAX_VALUE onSubscribe.
	 *
	 * @param dataConsumer A {@link BiConsumer} with left argument onNext data and right argument upstream subscription
	 * @param <T>          The type of the data sequence
	 * @return a fresh Reactive Streams subscriber ready to be subscribed
	 */
	public static <T> Subscriber<T> unbounded(BiConsumer<T, SubscriptionWithContext<Void>> dataConsumer) {
		return unbounded(dataConsumer, null, null);
	}


	/**
	 * Create a {@link Subscriber} reacting onNext and onError. The subscriber will automatically
	 * request Long.MAX_VALUE onSubscribe.
	 *
	 * @param dataConsumer  A {@link BiConsumer} with left argument onNext data and right argument upstream
	 *                         subscription
	 * @param errorConsumer A {@link Consumer} called onError
	 * @param <T>           The type of the data sequence
	 * @return a fresh Reactive Streams subscriber ready to be subscribed
	 */
	public static <T> Subscriber<T> unbounded(BiConsumer<T, SubscriptionWithContext<Void>> dataConsumer,
	                                          Consumer<Throwable> errorConsumer) {
		return unbounded(dataConsumer, errorConsumer, null);
	}


	/**
	 * Create a {@link Subscriber} reacting onNext, onError and onComplete. The subscriber will automatically
	 * request Long.MAX_VALUE onSubscribe.
	 * <p>
	 * The argument {@code subscriptionHandler} is executed once by new subscriber to generate a context shared by
	 * every
	 * request calls.
	 *
	 * @param dataConsumer     A {@link BiConsumer} with left argument onNext data and right argument upstream
	 *                         subscription
	 * @param errorConsumer    A {@link Consumer} called onError
	 * @param completeConsumer A {@link Consumer} called onComplete with the actual context if any
	 * @param <T>              The type of the data sequence
	 * @return a fresh Reactive Streams subscriber ready to be subscribed
	 */
	public static <T> Subscriber<T> unbounded(BiConsumer<T, SubscriptionWithContext<Void>> dataConsumer,
	                                          final Consumer<Throwable> errorConsumer,
	                                          Consumer<Void> completeConsumer) {
		return create(
		  UNBOUNDED_REQUEST_FUNCTION,
		  dataConsumer,
		  errorConsumer != null ? new BiConsumer<Throwable, Void>() {
			  @Override
			  public void accept(Throwable throwable, Void aVoid) {
				  errorConsumer.accept(throwable);
			  }
		  } : null,
		  completeConsumer
		);
	}

	/**
	 * Create a {@link Subscriber} that will will automatically request Long.MAX_VALUE onSubscribe.
	 *
	 * @param <T> The type of the data sequence
	 * @return a fresh Reactive Streams subscriber ready to be subscribed
	 */
	public static <T> Subscriber<T> consumer() {
		return consumer(null, null, null);
	}

	/**
	 * Create a {@link Subscriber} reacting onNext. The subscriber will automatically
	 * request Long.MAX_VALUE onSubscribe.
	 *
	 * @param dataConsumer A {@link Consumer} with argument onNext data
	 * @param <T>          The type of the data sequence
	 * @return a fresh Reactive Streams subscriber ready to be subscribed
	 */
	public static <T> Subscriber<T> consumer(Consumer<T> dataConsumer) {
		return consumer(dataConsumer, null, null);
	}


	/**
	 * Create a {@link Subscriber} reacting onNext and onError. The subscriber will automatically
	 * request Long.MAX_VALUE onSubscribe.
	 *
	 * @param dataConsumer  A {@link Consumer} with argument onNext data
	 * @param errorConsumer A {@link Consumer} called onError
	 * @param <T>           The type of the data sequence
	 * @return a fresh Reactive Streams subscriber ready to be subscribed
	 */
	public static <T> Subscriber<T> consumer(Consumer<T> dataConsumer,
			Consumer<Throwable> errorConsumer) {
		return consumer(dataConsumer, errorConsumer, null);
	}


	/**
	 * Create a {@link Subscriber} reacting onNext, onError and onComplete. The subscriber will automatically
	 * request Long.MAX_VALUE onSubscribe.
	 * <p>
	 * The argument {@code subscriptionHandler} is executed once by new subscriber to generate a context shared by
	 * every
	 * request calls.
	 *
	 * @param dataConsumer     A {@link Consumer} with argument onNext data
	 * @param errorConsumer    A {@link Consumer} called onError
	 * @param completeConsumer A {@link Consumer} called onComplete with the actual context if any
	 * @param <T>              The type of the data sequence
	 * @return a fresh Reactive Streams subscriber ready to be subscribed
	 */
	public static <T> Subscriber<T> consumer(Consumer<T> dataConsumer,
			final Consumer<Throwable> errorConsumer,
			Consumer<Void> completeConsumer) {
		return new ConsumerSubscriber<>(
				dataConsumer,
				errorConsumer,
				completeConsumer
		);
	}

	/**
	 * Create a {@link Subscriber} reacting onNext, onSubscribe, onError, onComplete with the passed {@link
	 * BiConsumer}.
	 * The argument {@code subscriptionHandler} is executed once by new subscriber to generate a context shared by
	 * every
	 * request calls.
	 * The argument {@code shutdownConsumer} is executed once by subscriber termination event (cancel, onComplete,
	 * onError).
	 *
	 * @param dataConsumer        A {@link BiConsumer} with left argument onNext data and right argument upstream
	 *                            subscription
	 * @param subscriptionHandler A {@link Function} called once for every new subscription returning an immutable
	 *                            context
	 *                            (IO connection...)
	 * @param errorConsumer       A {@link BiConsumer} called onError with the actual error as left operand and a given
	 *                            context as right operand
	 * @param completeConsumer    A {@link Consumer} called onComplete with the actual context if any
	 * @param <T>                 The type of the data sequence
	 * @param <C>                 The type of contextual information to be read by the requestConsumer
	 * @return a fresh Reactive Streams subscriber ready to be subscribed
	 */
	public static <T, C> Subscriber<T> create(Function<Subscription, C> subscriptionHandler,
	                                          BiConsumer<T, SubscriptionWithContext<C>> dataConsumer,
	                                          BiConsumer<Throwable, C> errorConsumer,
	                                          Consumer<C> completeConsumer) {

		return new SubscriberWithSubscriptionContext<T, C>(dataConsumer, subscriptionHandler, errorConsumer, completeConsumer);
	}

	private static final Function<Subscription, Void> UNBOUNDED_REQUEST_FUNCTION = new Function<Subscription, Void>() {
		@Override
		public Void apply(Subscription subscription) {
			subscription.request(Long.MAX_VALUE);
			return null;
		}
	};

	private static final class SubscriberWithSubscriptionContext<T, C> extends BaseSubscriber<T>
			implements Bounded, Publishable<T> {

		protected final Function<Subscription, C>                 subscriptionHandler;
		protected final BiConsumer<T, SubscriptionWithContext<C>> dataConsumer;
		protected final BiConsumer<Throwable, C>                  errorConsumer;
		protected final Consumer<C>                               completeConsumer;

		private SubscriptionWithContext<C> subscriptionWithContext;

		protected SubscriberWithSubscriptionContext(BiConsumer<T, SubscriptionWithContext<C>> dataConsumer,
		                            Function<Subscription, C> subscriptionHandler,
		                            BiConsumer<Throwable, C> errorConsumer,
		                            Consumer<C> completeConsumer) {
			Assert.notNull(subscriptionHandler, "A subscription handler must be provided");
			this.dataConsumer = dataConsumer;
			this.subscriptionHandler = subscriptionHandler;
			this.errorConsumer = errorConsumer;
			this.completeConsumer = completeConsumer;
		}


		@Override
		public Publisher<T> upstream() {
			return PublisherFactory.fromSubscription(subscriptionWithContext);
		}

		@Override
		public void onSubscribe(Subscription s) {
			try {
				if(BackpressureUtils.checkSubscription(subscriptionWithContext, s)) {
					final AtomicLong proxyRequest = new AtomicLong();
					final C context = subscriptionHandler.apply(new Subscription() {
						@Override
						public void request(long n) {
							if (subscriptionWithContext == null && proxyRequest.get() != Long.MIN_VALUE) {
								BackpressureUtils.getAndAdd(proxyRequest, n);
							} else {
								subscriptionWithContext.request(n);
							}
						}

						@Override
						public void cancel() {
							if (subscriptionWithContext == null) {
								proxyRequest.set(Long.MIN_VALUE);
							} else {
								subscriptionWithContext.cancel();
							}
						}
					});

					this.subscriptionWithContext = SubscriptionWithContext.create(s, context);
					if (proxyRequest.compareAndSet(Long.MIN_VALUE, 0)) {
						subscriptionWithContext.cancel();
					} else if (proxyRequest.get() > 0) {
						subscriptionWithContext.request(proxyRequest.get());
					}
				}
			} catch (Throwable throwable) {
				Exceptions.throwIfFatal(throwable);
				onError(throwable);
			}

		}

		@Override
		public void onNext(T t) {
			super.onNext(t);

			if (dataConsumer != null) {
				try {
					dataConsumer.accept(t, subscriptionWithContext);
				}
				catch (CancelException ce){
					throw ce;
				}
				catch (Throwable error) {
					onError(Exceptions.addValueAsLastCause(error, t));
				}
			}
		}

		@Override
		public void onError(Throwable t) {
			super.onError(t);

			if (errorConsumer != null) {
				errorConsumer.accept(t, subscriptionWithContext != null ? subscriptionWithContext.context() : null);
			} else {
				throw ReactorFatalException.create(t);
			}
		}

		@Override
		public void onComplete() {
			if (completeConsumer != null) {
				try {
					completeConsumer.accept(subscriptionWithContext != null ? subscriptionWithContext.context() :
					  null);
				} catch (Throwable t) {
					onError(t);
				}
			}
		}

		@Override
		public boolean isExposedToOverflow(Bounded parent) {
			return false;
		}

		@Override
		public long getCapacity() {
			return Long.MAX_VALUE;
		}
	}
}
