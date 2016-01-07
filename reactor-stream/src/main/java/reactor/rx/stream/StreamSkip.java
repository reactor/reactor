package reactor.rx.stream;

import java.util.Objects;

import org.reactivestreams.*;

/**
 * Skips the first N elements from a reactive stream.
 *
 * @param <T> the value type
 */

/**
 * {@see <a href='https://github.com/reactor/reactive-streams-commons'>https://github.com/reactor/reactive-streams-commons</a>}
 * @since 2.5
 */
public final class StreamSkip<T> extends StreamBarrier<T, T> {

	final Publisher<? extends T> source;

	final long n;

	public StreamSkip(Publisher<? extends T> source, long n) {
		super(source);
		if (n < 0) {
			throw new IllegalArgumentException("n >= 0 required but it was " + n);
		}
		this.source = Objects.requireNonNull(source, "source");
		this.n = n;
	}

	public long n() {
		return n;
	}

	@Override
	public void subscribe(Subscriber<? super T> s) {
		if (n == 0) {
			source.subscribe(s);
		} else {
			source.subscribe(new StreamSkipSubscriber<>(s, n));
		}
	}

	static final class StreamSkipSubscriber<T> implements Subscriber<T>, Downstream, UpstreamDemand, Bounded,
															 ActiveUpstream {

		final Subscriber<? super T> actual;

		final long n;

		long remaining;

		public StreamSkipSubscriber(Subscriber<? super T> actual, long n) {
			this.actual = actual;
			this.n = n;
			this.remaining = n;
		}

		@Override
		public void onSubscribe(Subscription s) {
			actual.onSubscribe(s);

			s.request(n);
		}

		@Override
		public void onNext(T t) {
			long r = remaining;
			if (r == 0L) {
				actual.onNext(t);
			} else {
				remaining = r - 1;
			}
		}

		@Override
		public void onError(Throwable t) {
			actual.onError(t);
		}

		@Override
		public void onComplete() {
			actual.onComplete();
		}

		@Override
		public boolean isStarted() {
			return remaining != n;
		}

		@Override
		public boolean isTerminated() {
			return remaining == 0;
		}

		@Override
		public long getCapacity() {
			return n;
		}

		@Override
		public Object downstream() {
			return actual;
		}

		@Override
		public long expectedFromUpstream() {
			return remaining;
		}
	}
}
