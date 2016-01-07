package reactor.rx.stream;

import java.util.Objects;
import reactor.fn.Predicate;

import org.reactivestreams.*;

import reactor.core.error.Exceptions;
import reactor.core.support.BackpressureUtils;

/**
 * Relays values until a predicate returns
 * true, indicating the sequence should stop
 * (checked after each value has been delivered).
 *
 * @param <T> the value type
 */

/**
 * {@see <a href='https://github.com/reactor/reactive-streams-commons'>https://github.com/reactor/reactive-streams-commons</a>}
 * @since 2.5
 */
public final class StreamTakeUntilPredicate<T> extends StreamBarrier<T, T> {

	final Predicate<? super T> predicate;

	public StreamTakeUntilPredicate(Publisher<? extends T> source, Predicate<? super T> predicate) {
		super(source);
		this.predicate = Objects.requireNonNull(predicate, "predicate");
	}

	public Predicate<? super T> predicate() {
		return predicate;
	}

	@Override
	public void subscribe(Subscriber<? super T> s) {
		source.subscribe(new StreamTakeUntilPredicateSubscriber<>(s, predicate));
	}

	static final class StreamTakeUntilPredicateSubscriber<T>
			implements Subscriber<T>, Downstream, Upstream, FeedbackLoop, ActiveUpstream{
		final Subscriber<? super T> actual;

		final Predicate<? super T> predicate;

		Subscription s;

		boolean done;

		public StreamTakeUntilPredicateSubscriber(Subscriber<? super T> actual, Predicate<? super T> predicate) {
			this.actual = actual;
			this.predicate = predicate;
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

			actual.onNext(t);

			boolean b;

			try {
				b = predicate.test(t);
			} catch (Throwable e) {
				s.cancel();

				onError(e);

				return;
			}

			if (b) {
				s.cancel();

				onComplete();

				return;
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
			return predicate;
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
