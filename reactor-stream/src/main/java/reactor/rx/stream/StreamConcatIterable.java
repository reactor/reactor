package reactor.rx.stream;

import java.util.*;
import java.util.concurrent.atomic.AtomicIntegerFieldUpdater;

import org.reactivestreams.*;

import reactor.core.subscriber.SubscriberMultiSubscription;
import reactor.core.subscription.EmptySubscription;
import reactor.core.support.ReactiveState;

/**
 * Concatenates a fixed array of Publishers' values.
 *
 * @param <T> the value type
 */

/**
 * {@see <a href='https://github.com/reactor/reactive-streams-commons'>https://github.com/reactor/reactive-streams-commons</a>}
 * @since 2.5
 */
public final class StreamConcatIterable<T> 
extends reactor.rx.Stream<T>
implements 
														 ReactiveState.Factory,
														 ReactiveState.LinkedUpstreams {

	final Iterable<? extends Publisher<? extends T>> iterable;

	public StreamConcatIterable(Iterable<? extends Publisher<? extends T>> iterable) {
		this.iterable = Objects.requireNonNull(iterable, "iterable");
	}

	@Override
	public Iterator<?> upstreams() {
		return iterable.iterator();
	}

	@Override
	public long upstreamsCount() {
		return -1L;
	}

	@Override
	public void subscribe(Subscriber<? super T> s) {

		Iterator<? extends Publisher<? extends T>> it;

		try {
			it = iterable.iterator();
		} catch (Throwable e) {
			EmptySubscription.error(s, e);
			return;
		}

		if (it == null) {
			EmptySubscription.error(s, new NullPointerException("The Iterator returned is null"));
			return;
		}

		StreamConcatIterableSubscriber<T> parent = new StreamConcatIterableSubscriber<>(s, it);

		s.onSubscribe(parent);

		if (!parent.isCancelled()) {
			parent.onComplete();
		}
	}

	static final class StreamConcatIterableSubscriber<T>
	  extends SubscriberMultiSubscription<T, T> {

		final Iterator<? extends Publisher<? extends T>> it;

		volatile int wip;
		@SuppressWarnings("rawtypes")
		static final AtomicIntegerFieldUpdater<StreamConcatIterableSubscriber> WIP =
		  AtomicIntegerFieldUpdater.newUpdater(StreamConcatIterableSubscriber.class, "wip");

		long produced;

		public StreamConcatIterableSubscriber(Subscriber<? super T> actual, Iterator<? extends Publisher<? extends
		  T>> it) {
			super(actual);
			this.it = it;
		}

		@Override
		public void onNext(T t) {
			produced++;

			subscriber.onNext(t);
		}

		@Override
		public void onComplete() {
			if (WIP.getAndIncrement(this) == 0) {
				Iterator<? extends Publisher<? extends T>> a = this.it;
				do {
					if (isCancelled()) {
						return;
					}

					boolean b;

					try {
						b = a.hasNext();
					} catch (Throwable e) {
						onError(e);
						return;
					}

					if (isCancelled()) {
						return;
					}


					if (!b) {
						subscriber.onComplete();
						return;
					}

					Publisher<? extends T> p;

					try {
						p = it.next();
					} catch (Throwable e) {
						subscriber.onError(e);
						return;
					}

					if (isCancelled()) {
						return;
					}

					if (p == null) {
						subscriber.onError(new NullPointerException("The Publisher returned by the iterator is null"));
						return;
					}

					long c = produced;
					if (c != 0L) {
						produced = 0L;
						produced(c);
					}

					p.subscribe(this);

					if (isCancelled()) {
						return;
					}

				} while (WIP.decrementAndGet(this) != 0);
			}

		}
	}
}
