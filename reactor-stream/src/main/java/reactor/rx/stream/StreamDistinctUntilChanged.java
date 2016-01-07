package reactor.rx.stream;

import java.util.Objects;
import reactor.fn.Function;

import org.reactivestreams.*;

import reactor.core.error.Exceptions;
import reactor.core.support.BackpressureUtils;

/**
 * Filters out subsequent and repeated elements.
 *
 * @param <T> the value type
 * @param <K> the key type used for comparing subsequent elements
 */

/**
 * {@see <a href='https://github.com/reactor/reactive-streams-commons'>https://github.com/reactor/reactive-streams-commons</a>}
 * @since 2.5
 */
public final class StreamDistinctUntilChanged<T, K> extends StreamBarrier<T, T> {

	final Function<? super T, K> keyExtractor;

	public StreamDistinctUntilChanged(Publisher<? extends T> source, Function<? super T, K> keyExtractor) {
		super(source);
		this.keyExtractor = Objects.requireNonNull(keyExtractor, "keyExtractor");
	}

	@Override
	public void subscribe(Subscriber<? super T> s) {
		source.subscribe(new StreamDistinctUntilChangedSubscriber<>(s, keyExtractor));
	}

	static final class StreamDistinctUntilChangedSubscriber<T, K>
			implements Subscriber<T>, Downstream, FeedbackLoop, Upstream, ActiveUpstream {
		final Subscriber<? super T> actual;

		final Function<? super T, K> keyExtractor;

		Subscription s;

		boolean done;

		K lastKey;

		public StreamDistinctUntilChangedSubscriber(Subscriber<? super T> actual,
													   Function<? super T, K> keyExtractor) {
			this.actual = actual;
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


			if (Objects.equals(lastKey, k)) {
				lastKey = k;
				s.request(1);
			} else {
				lastKey = k;
				actual.onNext(t);
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
			return lastKey;
		}

		@Override
		public Object upstream() {
			return null;
		}
	}
}
