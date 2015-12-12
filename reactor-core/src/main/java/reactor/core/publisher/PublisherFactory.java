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

package reactor.core.publisher;

import java.util.concurrent.atomic.AtomicIntegerFieldUpdater;
import java.util.concurrent.atomic.AtomicLongFieldUpdater;

import org.reactivestreams.Publisher;
import org.reactivestreams.Subscriber;
import org.reactivestreams.Subscription;
import reactor.core.error.CancelException;
import reactor.core.error.Exceptions;
import reactor.core.error.SpecificationExceptions;
import reactor.core.subscriber.SubscriberBarrier;
import reactor.core.subscriber.SubscriberWithContext;
import reactor.core.subscription.ReactiveSession;
import reactor.core.support.Assert;
import reactor.core.support.BackpressureUtils;
import reactor.core.support.ReactiveState;
import reactor.core.support.ReactiveStateUtils;
import reactor.core.support.SignalType;
import reactor.fn.BiConsumer;
import reactor.fn.Consumer;
import reactor.fn.Function;

/**
 * A Reactive Streams {@link org.reactivestreams.Publisher} factory which callbacks on start, request and shutdown <p>
 * The Publisher will directly forward all the signals passed to the subscribers and complete when onComplete is called.
 * <p> Create such publisher with the provided factory, E.g.:
 * <pre>
 * {@code
 * PublisherFactory.createWithDemand((n, sub) -> {
 *  for(int i = 0; i < n; i++){
 *    sub.onNext(i);
 *  }
 * }
 * }
 * </pre>
 * @author Stephane Maldini
 * @since 2.0.2
 */
public abstract class PublisherFactory implements ReactiveState {

	/**
	 * Create a {@link Publisher} reacting on requests with the passed {@link BiConsumer}
	 * @param requestConsumer A {@link BiConsumer} with left argument request and right argument target subscriber
	 * @param <T> The type of the data sequence
	 * @return a fresh Reactive Streams publisher ready to be subscribed
	 */
	public static <T> Publisher<T> createWithDemand(BiConsumer<Long, SubscriberWithContext<T, Void>> requestConsumer) {
		return createWithDemand(requestConsumer, null, null);
	}

	/**
	 * Create a {@link Publisher} reacting on requests with the passed {@link BiConsumer} The argument {@code
	 * contextFactory} is executed once by new subscriber to generate a context shared by every request calls.
	 * @param requestConsumer A {@link BiConsumer} with left argument request and right argument target subscriber
	 * @param contextFactory A {@link Function} called for every new subscriber returning an immutable context (IO
	 * connection...)
	 * @param <T> The type of the data sequence
	 * @param <C> The type of contextual information to be read by the requestConsumer
	 * @return a fresh Reactive Streams publisher ready to be subscribed
	 */
	public static <T, C> Publisher<T> createWithDemand(BiConsumer<Long, SubscriberWithContext<T, C>> requestConsumer,
			Function<Subscriber<? super T>, C> contextFactory) {
		return createWithDemand(requestConsumer, contextFactory, null);
	}

	/**
	 * Create a {@link Publisher} reacting on requests with the passed {@link BiConsumer}. The argument {@code
	 * contextFactory} is executed once by new subscriber to generate a context shared by every request calls. The
	 * argument {@code shutdownConsumer} is executed once by subscriber termination event (cancel, onComplete,
	 * onError).
	 * @param requestConsumer A {@link BiConsumer} with left argument request and right argument target subscriber
	 * @param contextFactory A {@link Function} called once for every new subscriber returning an immutable context (IO
	 * connection...)
	 * @param shutdownConsumer A {@link Consumer} called once everytime a subscriber terminates: cancel, onComplete(),
	 * onError()
	 * @param <T> The type of the data sequence
	 * @param <C> The type of contextual information to be read by the requestConsumer
	 * @return a fresh Reactive Streams publisher ready to be subscribed
	 */
	public static <T, C> Publisher<T> createWithDemand(BiConsumer<Long, SubscriberWithContext<T, C>> requestConsumer,
			Function<Subscriber<? super T>, C> contextFactory,
			Consumer<C> shutdownConsumer) {

		return new ReactorPublisher<T, C>(new RecursiveConsumer<>(requestConsumer), contextFactory, shutdownConsumer);
	}

	/**
	 * Create a {@link Publisher} reacting on subscribe with the passed {@link Consumer}. The argument {@code
	 * sessionConsumer} is executed once by new subscriber to generate a {@link ReactiveSession} context ready to accept
	 * signals.
	 * @param sessionConsumer A {@link Consumer} called once everytime a subscriber subscribes
	 * @param <T> The type of the data sequence
	 * @return a fresh Reactive Streams publisher ready to be subscribed
	 */
	public static <T> Publisher<T> yield(Consumer<? super ReactiveSession<T>> sessionConsumer) {

		return new SessionPublisher<>(sessionConsumer);
	}

	/**
	 * Create a {@link Publisher} reacting on each available {@link Subscriber} read derived with the passed {@link
	 * Consumer}. If a previous request is still running, avoid recursion and extend the previous request iterations.
	 * @param requestConsumer A {@link Consumer} invoked when available read with the target subscriber
	 * @param <T> The type of the data sequence
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
	 * @param requestConsumer A {@link Consumer} invoked when available read with the target subscriber
	 * @param contextFactory A {@link Function} called for every new subscriber returning an immutable context (IO
	 * connection...)
	 * @param <T> The type of the data sequence
	 * @param <C> The type of contextual information to be read by the requestConsumer
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
	 * request calls. The argument {@code shutdownConsumer} is executed once by subscriber termination event (cancel,
	 * onComplete, onError).
	 * @param requestConsumer A {@link Consumer} invoked when available read with the target subscriber
	 * @param contextFactory A {@link Function} called once for every new subscriber returning an immutable context (IO
	 * connection...)
	 * @param shutdownConsumer A {@link Consumer} called once everytime a subscriber terminates: cancel, onComplete(),
	 * onError()
	 * @param <T> The type of the data sequence
	 * @param <C> The type of contextual information to be read by the requestConsumer
	 * @return a fresh Reactive Streams publisher ready to be subscribed
	 */
	public static <T, C> Publisher<T> create(final Consumer<SubscriberWithContext<T, C>> requestConsumer,
			Function<Subscriber<? super T>, C> contextFactory,
			Consumer<C> shutdownConsumer) {
		Assert.notNull(requestConsumer, "A data producer must be provided");
		return new ForEachPublisher<T, C>(requestConsumer, contextFactory, shutdownConsumer);
	}

	/**
	 * Intercept a source {@link Publisher} onNext signal to eventually transform, forward or filter the data by calling
	 * or not the right operand {@link Subscriber}.
	 * @param dataConsumer A {@link BiConsumer} with left argument onNext data and right argument output subscriber
	 * @param <I> The source type of the data sequence
	 * @param <O> The target type of the data sequence
	 * @return a fresh Reactive Streams publisher ready to be subscribed
	 */
	public static <I, O> Publisher<O> lift(Publisher<I> source, BiConsumer<I, Subscriber<? super O>> dataConsumer) {
		return lift(source, dataConsumer, null, null);
	}

	/**
	 * Intercept a source {@link Publisher} onNext signal to eventually transform, forward or filter the data by calling
	 * or not the right operand {@link Subscriber}.
	 * @param dataConsumer A {@link BiConsumer} with left argument onNext data and right argument output subscriber
	 * @param errorConsumer A {@link BiConsumer} with left argument onError throwable and right argument output sub
	 * @param <I> The source type of the data sequence
	 * @param <O> The target type of the data sequence
	 * @return a fresh Reactive Streams publisher ready to be subscribed
	 */
	public static <I, O> Publisher<O> lift(Publisher<I> source,
			BiConsumer<I, Subscriber<? super O>> dataConsumer,
			BiConsumer<Throwable, Subscriber<? super O>> errorConsumer) {
		return lift(source, dataConsumer, errorConsumer, null);
	}

	/**
	 * Intercept a source {@link Publisher} onNext signal to eventually transform, forward or filter the data by calling
	 * or not the right operand {@link Subscriber}. <p> The argument {@code subscriptionHandler} is executed once by new
	 * subscriber to generate a context shared by every request calls.
	 * @param dataConsumer A {@link BiConsumer} with left argument onNext data and right argument output subscriber
	 * @param errorConsumer A {@link BiConsumer} with left argument onError throwable and right argument output sub
	 * @param completeConsumer A {@link Consumer} called onComplete with the actual output subscriber
	 * @param <I> The source type of the data sequence
	 * @param <O> The target type of the data sequence
	 * @return a fresh Reactive Streams publisher ready to be subscribed
	 */
	public static <I, O> Publisher<O> lift(Publisher<I> source,
			final BiConsumer<I, Subscriber<? super O>> dataConsumer,
			final BiConsumer<Throwable, Subscriber<? super O>> errorConsumer,
			final Consumer<Subscriber<? super O>> completeConsumer) {
		return lift(source, new Function<Subscriber<? super O>, Subscriber<? super I>>() {
			@Override
			public SubscriberBarrier<I, O> apply(final Subscriber<? super O> subscriber) {
				return new ConsumerSubscriberBarrier<>(subscriber, dataConsumer, errorConsumer, completeConsumer);
			}
		});
	}

	/**
	 * Create a {@link Publisher} intercepting all source signals with the returned Subscriber that might choose to pass
	 * them alone to the provided Subscriber (given to the returned {@link Publisher#subscribe(Subscriber)}.
	 * @param source A {@link Publisher} source delegate
	 * @param barrierProvider A {@link Function} called once for every new subscriber returning a unique {@link
	 * Subscriber} to intercept upstream signals
	 * @param <I> The type of the data sequence
	 * @param <O> The type of contextual information to be read by the requestConsumer
	 * @return a fresh Reactive Streams publisher ready to be subscribed
	 */
	public static <I, O> Publisher<O> lift(Publisher<I> source,
			Function<Subscriber<? super O>, Subscriber<? super I>> barrierProvider) {
		Assert.notNull(source, "A data source must be provided");
		Assert.notNull(barrierProvider, "A lift interceptor must be provided");
		return new PublisherOperator<>(source, barrierProvider);
	}

	/**
	 *
	 * @param source
	 * @param <I>
	 * @return
	 */
	public static <I> Publisher<I> unbounded(Publisher<I> source) {
		Assert.notNull(source, "A data source must be provided");
		return capacity(source, Long.MAX_VALUE);
	}

	/**
	 *
	 * @param source
	 * @param capacity
	 * @param <I>
	 * @return
	 */
	public static <I> Publisher<I> capacity(Publisher<I> source, long capacity) {
		Assert.notNull(source, "A data source must be provided");
		return new BoundedPublisher<>(source, capacity);
	}


	/**
	 *
	 * @param left
	 * @param right
	 * @param <I>
	 * @param <O>
	 * @param <E>
	 * @return
	 */
	public static <I, O, E> Function<Subscriber<? super I>, Subscriber<? super O>> opFusion(final Function<Subscriber<? super I>, Subscriber<? super E>> left,
			final Function<Subscriber<? super E>, Subscriber<? super O>> right) {
		return new Function<Subscriber<? super I>, Subscriber<? super O>>() {
			@Override
			public Subscriber<? super O> apply(Subscriber<? super I> subscriber) {
				return right.apply(left.apply(subscriber));
			}
		};
	}

	/**
	 * Marker interface for publishers refering to an operator {@link Function} used to augment {@link Subscriber}
	 */
	public interface LiftOperator<I, O> extends Publisher<O> {

		Function<Subscriber<? super O>, Subscriber<? super I>> operator();
	}

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
			}
			catch (PrematureCompleteException pce) {
				subscriber.onSubscribe(SignalType.NOOP_SUBSCRIPTION);
				subscriber.onComplete();
			}
			catch (Throwable throwable) {
				Exceptions.<T>publisher(throwable).subscribe(subscriber);
			}
		}

		protected Subscription createSubscription(Subscriber<? super T> subscriber, C context) {
			return new SubscriberProxy<>(this, subscriber, context, requestConsumer, shutdownConsumer);
		}
	}

	private static final class ForEachPublisher<T, C> extends ReactorPublisher<T, C>
			implements Upstream{

		final Consumer<SubscriberWithContext<T, C>> forEachConsumer;

		public ForEachPublisher(Consumer<SubscriberWithContext<T, C>> forEachConsumer, Function<Subscriber<? super
				T>, C> contextFactory, Consumer<C> shutdownConsumer) {
			super(null, contextFactory, shutdownConsumer);
			this.forEachConsumer = forEachConsumer;
		}

		@Override
		protected Subscription createSubscription(Subscriber<? super T> subscriber, C context) {
			return new SubscriberProxy<>(this,
					subscriber,
					context,
					new ForEachBiConsumer<>(forEachConsumer),
					shutdownConsumer);
		}

		@Override
		public Object upstream() {
			return forEachConsumer;
		}

		@Override
		public String toString() {
			return forEachConsumer.toString();
		}
	}

	private final static class PublisherOperator<I, O>
			implements Bounded,
			           Named,
			           Upstream,
			           LiftOperator<I, O> {

		final private Publisher<I>                                           source;
		final private Function<Subscriber<? super O>, Subscriber<? super I>> barrierProvider;

		public PublisherOperator(Publisher<I> source,
				Function<Subscriber<? super O>, Subscriber<? super I>> barrierProvider) {
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
		public Object upstream() {
			return source;
		}

		@Override
		public String getName() {
			return barrierProvider.getClass().getSimpleName().replaceAll("Operator", "");
		}

		@Override
		public Function<Subscriber<? super O>, Subscriber<? super I>> operator() {
			return barrierProvider;
		}

		@Override
		public String toString() {
			return "{" +
					" operator : \"" +getName() + "\" " +
					'}';
		}

		@Override
		public long getCapacity() {
			return Bounded.class.isAssignableFrom(source.getClass()) ? ((Bounded) source).getCapacity() :
					Long.MAX_VALUE;
		}
	}

	private final static class BoundedPublisher<I>
			implements Publisher<I>, Bounded, Named,
			           Upstream {

		final private Publisher<I>                                           source;
		final private long capacity;

		public BoundedPublisher(Publisher<I> source, long capacity) {
			this.source = source;
			this.capacity = capacity;
		}

		@Override
		public void subscribe(Subscriber<? super I> s) {
			source.subscribe(s);
		}

		@Override
		public long getCapacity() {
			return capacity;
		}

		@Override
		public Object upstream() {
			return source;
		}

		@Override
		public String getName() {
			return "Bounded";
		}
	}
	private final static class SubscriberProxy<T, C> extends SubscriberWithContext<T, C>
			implements Subscription, Upstream, ActiveUpstream, Named {

		private final BiConsumer<Long, SubscriberWithContext<T, C>> requestConsumer;

		private final Consumer<C> shutdownConsumer;

		private final Publisher<T> source;

		public SubscriberProxy(Publisher<T> source,
				Subscriber<? super T> subscriber,
				C context,
				BiConsumer<Long, SubscriberWithContext<T, C>> requestConsumer,
				Consumer<C> shutdownConsumer) {
			super(context, subscriber);
			this.source = source;
			this.requestConsumer = requestConsumer;
			this.shutdownConsumer = shutdownConsumer;
		}

		@Override
		public Publisher<T> upstream() {
			return source;
		}

		@Override
		public String getName() {
			return ReactiveStateUtils.getName(requestConsumer);
		}

		@Override
		public void request(long n) {
			if (isCancelled()) {
				return;
			}

			if (BackpressureUtils.checkRequest(n, this)) {
				try {
					requestConsumer.accept(n, this);
				}
				catch (Throwable t) {
					onError(t);
				}
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
				if (CancelException.class != t.getClass()) {
					subscriber.onError(t);
				}
			}
		}

		@Override
		public void onComplete() {
			if (TERMINAL_UPDATER.compareAndSet(this, 0, 1)) {
				doShutdown();
				try {
					subscriber.onComplete();
				}
				catch (Throwable t) {
					Exceptions.throwIfFatal(t);
					subscriber.onError(t);
				}
			}
		}

		private void doShutdown() {
			if (shutdownConsumer == null) {
				return;
			}

			try {
				shutdownConsumer.accept(context);
			}
			catch (Throwable t) {
				Exceptions.throwIfFatal(t);
				subscriber.onError(t);
			}
		}

		@Override
		public void onSubscribe(Subscription s) {
			throw new UnsupportedOperationException(" the delegate subscriber is already subscribed");
		}

		@Override
		public String toString() {
			return source.toString();
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

			if (n == Long.MAX_VALUE) {
				while (!sub.isCancelled()) {
					requestConsumer.accept(sub);
				}
				return;
			}

			if (BackpressureUtils.getAndAdd(PENDING_UPDATER, this, n) > 0) {
				return;
			}

			long demand = n;
			do {
				long requestCursor = 0L;
				while ((demand == Long.MAX_VALUE || requestCursor++ < demand) && !sub.isCancelled()) {
					requestConsumer.accept(sub);
				}
			}
			while ((demand == Long.MAX_VALUE || (demand =
					PENDING_UPDATER.addAndGet(this, -demand)) > 0L) && !sub.isCancelled());

		}

	}

	private final static class RecursiveConsumer<T, C> implements BiConsumer<Long, SubscriberWithContext<T, C>> {

		private final BiConsumer<Long, SubscriberWithContext<T, C>> requestConsumer;

		@SuppressWarnings("unused")
		private volatile int running = 0;

		private final static AtomicIntegerFieldUpdater<RecursiveConsumer> RUNNING =
				AtomicIntegerFieldUpdater.newUpdater(RecursiveConsumer.class, "running");

		@SuppressWarnings("unused")
		private volatile long pending = 0L;

		private final static AtomicLongFieldUpdater<RecursiveConsumer> PENDING_UPDATER =
				AtomicLongFieldUpdater.newUpdater(RecursiveConsumer.class, "pending");

		public RecursiveConsumer(BiConsumer<Long, SubscriberWithContext<T, C>> requestConsumer) {
			this.requestConsumer = requestConsumer;
		}

		@Override
		public void accept(Long n, SubscriberWithContext<T, C> sub) {
			BackpressureUtils.getAndAdd(PENDING_UPDATER, this, n);
			if (RUNNING.getAndIncrement(this) == 0) {
				int missed = 1;
				long r;
				for (; ; ) {
					if (sub.isCancelled()) {
						return;
					}

					r = PENDING_UPDATER.getAndSet(this, 0L);
					if (r == Long.MAX_VALUE) {
						requestConsumer.accept(Long.MAX_VALUE, sub);
						return;
					}

					if (r != 0L) {
						requestConsumer.accept(r, sub);
					}

					missed = RUNNING.addAndGet(this, -missed);
					if (missed == 0) {
						break;
					}
				}
			}

		}

	}

	private static final class ConsumerSubscriberBarrier<I, O> extends SubscriberBarrier<I, O> {

		private final BiConsumer<I, Subscriber<? super O>>         dataConsumer;
		private final BiConsumer<Throwable, Subscriber<? super O>> errorConsumer;
		private final Consumer<Subscriber<? super O>>              completeConsumer;

		public ConsumerSubscriberBarrier(Subscriber<? super O> subscriber,
				BiConsumer<I, Subscriber<? super O>> dataConsumer,
				BiConsumer<Throwable, Subscriber<? super O>> errorConsumer,
				Consumer<Subscriber<? super O>> completeConsumer) {
			super(subscriber);
			this.dataConsumer = dataConsumer;
			this.errorConsumer = errorConsumer;
			this.completeConsumer = completeConsumer;
		}

		@Override
		protected void doNext(I o) {
			if (dataConsumer != null) {
				dataConsumer.accept(o, subscriber);
			}
			else {
				super.doNext(o);
			}
		}

		@Override
		protected void doError(Throwable throwable) {
			if (errorConsumer != null) {
				errorConsumer.accept(throwable, subscriber);
			}
			else {
				super.doError(throwable);
			}
		}

		@Override
		protected void doComplete() {
			if (completeConsumer != null) {
				completeConsumer.accept(subscriber);
			}
			else {
				super.doComplete();
			}
		}

		@Override
		public String toString() {
			return "ConsumerSubscriberBarrier{" +
					"dataConsumer=" + dataConsumer +
					", errorConsumer=" + errorConsumer +
					", completeConsumer=" + completeConsumer +
					'}';
		}
	}

	private static class SessionPublisher<T> implements Publisher<T> {

		final Consumer<? super ReactiveSession<T>> onSubscribe;

		public SessionPublisher(Consumer<? super ReactiveSession<T>> onSubscribe) {
			this.onSubscribe = onSubscribe;
		}

		@Override
		public void subscribe(Subscriber<? super T> subscriber) {
			try {
				ReactiveSession<T> session = new YieldingReactiveSession<>(onSubscribe, subscriber);
				session.start();

			}
			catch (PrematureCompleteException pce) {
				subscriber.onSubscribe(SignalType.NOOP_SUBSCRIPTION);
				subscriber.onComplete();
			}
			catch (Throwable throwable) {
				Exceptions.<T>publisher(throwable).subscribe(subscriber);
			}
		}
	}

	private static class YieldingReactiveSession<T> extends ReactiveSession<T> {

		final Consumer<? super ReactiveSession<T>> onSubscribe;

		@SuppressWarnings("unused")
		private volatile int running = 0;

		private final static AtomicIntegerFieldUpdater<YieldingReactiveSession> RUNNING =
				AtomicIntegerFieldUpdater.newUpdater(YieldingReactiveSession.class, "running");

		public YieldingReactiveSession(Consumer<? super ReactiveSession<T>> onSubscribe, Subscriber<? super T> actual) {
			super(actual);
			this.onSubscribe = onSubscribe;
		}

		@Override
		public void request(long n) {
			super.request(n);
			if (RUNNING.getAndIncrement(this) == 0) {
				int missed = 1;

				onSubscribe.accept(this);

				for (; ; ) {
					missed = RUNNING.addAndGet(this, -missed);
					if (missed == 0) {
						break;
					}
				}

			}
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
