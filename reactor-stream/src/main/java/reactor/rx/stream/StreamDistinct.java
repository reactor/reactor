package reactor.rx.stream;

import java.util.*;
import reactor.fn.*;

import org.reactivestreams.*;

import reactor.core.error.Exceptions;
import reactor.core.subscription.EmptySubscription;
import reactor.core.support.BackpressureUtils;

/**
 * For each subscriber, tracks the source values that have been seen and
 * filters out duplicates.
 *
 * @param <T> the source value type
 * @param <K> the key extacted from the source value to be used for duplicate testing
 * @param <C> the collection type whose add() method is used for testing for duplicates
 */

/**
 * {@see <a href='https://github.com/reactor/reactive-streams-commons'>https://github.com/reactor/reactive-streams-commons</a>}
 * @since 2.5
 */
public final class StreamDistinct<T, K, C extends Collection<? super K>> extends StreamBarrier<T, T> {

	final Function<? super T, ? extends K> keyExtractor;

	final Supplier<C> collectionSupplier;

	public StreamDistinct(Publisher<? extends T> source, Function<? super T, ? extends K> keyExtractor,
							 Supplier<C> collectionSupplier) {
		super(source);
		this.keyExtractor = Objects.requireNonNull(keyExtractor, "keyExtractor");
		this.collectionSupplier = Objects.requireNonNull(collectionSupplier, "collectionSupplier");
	}

	@Override
	public void subscribe(Subscriber<? super T> s) {
		C collection;

		try {
			collection = collectionSupplier.get();
		} catch (Throwable e) {
			EmptySubscription.error(s, e);
			return;
		}

		if (collection == null) {
			EmptySubscription.error(s, new NullPointerException("The collectionSupplier returned a null collection"));
			return;
		}

		source.subscribe(new StreamDistinctSubscriber<>(s, collection, keyExtractor));
	}

	static final class StreamDistinctSubscriber<T, K, C extends Collection<? super K>>
			implements Subscriber<T>, Downstream, FeedbackLoop, Upstream, ActiveUpstream {
		final Subscriber<? super T> actual;

		final C collection;

		final Function<? super T, ? extends K> keyExtractor;

		Subscription s;

		boolean done;

		public StreamDistinctSubscriber(Subscriber<? super T> actual, C collection,
										   Function<? super T, ? extends K> keyExtractor) {
			this.actual = actual;
			this.collection = collection;
			this.keyExtractor = keyExtractor;
		}

		@Override
		public void onSubscribe(Subscription s) {
			if (BackpressureUtils.validate(this.s, s)) {
				this.s = s;

				actual.onSubscribe(s);
			}
		}

		@Override
		public void onNext(T t) {
			if (done) {
				Exceptions.onNextDropped(t);
				return;
			}

			K k;

			try {
				k = keyExtractor.apply(t);
			} catch (Throwable e) {
				s.cancel();

				onError(e);
				return;
			}

			boolean b;

			try {
				b = collection.add(k);
			} catch (Throwable e) {
				s.cancel();

				onError(e);
				return;
			}


			if (b) {
				actual.onNext(t);
			} else {
				s.request(1);
			}
		}

		@Override
		public void onError(Throwable t) {
			if (done) {
				Exceptions.onErrorDropped(t);
				return;
			}
			done = true;

			actual.onError(t);
		}

		@Override
		public void onComplete() {
			if (done) {
				return;
			}
			done = true;

			actual.onComplete();
		}

		@Override
		public boolean isStarted() {
			return s != null && !done;
		}

		@Override
		public boolean isTerminated() {
			return done;
		}

		@Override
		public Object downstream() {
			return actual;
		}

		@Override
		public Object delegateInput() {
			return keyExtractor;
		}

		@Override
		public Object delegateOutput() {
			return null;
		}

		@Override
		public Object upstream() {
			return s;
		}
	}
}
