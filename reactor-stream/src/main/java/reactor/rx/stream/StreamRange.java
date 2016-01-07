package reactor.rx.stream;

import java.util.concurrent.atomic.AtomicLongFieldUpdater;

import org.reactivestreams.*;

import reactor.core.support.*;

/**
 * Emits a range of integer values.
 */

/**
 * {@see <a href='https://github.com/reactor/reactive-streams-commons'>https://github.com/reactor/reactive-streams-commons</a>}
 * @since 2.5
 */
public final class StreamRange 
extends reactor.rx.Stream<Integer>
implements 
											 ReactiveState.Factory {

	final long start;

	final long end;

	public StreamRange(int start, int count) {
		if (count < 0) {
			throw new IllegalArgumentException("count >= required but it was " + count);
		}
		long e = (long) start + count;
		if (e - 1 > Integer.MAX_VALUE) {
			throw new IllegalArgumentException("start + count must be less than Integer.MAX_VALUE + 1");
		}

		this.start = start;
		this.end = e;
	}

	@Override
	public void subscribe(Subscriber<? super Integer> s) {
		s.onSubscribe(new StreamRangeSubscription<>(s, start, end));
	}

	static final class StreamRangeSubscription<T>
	  implements Subscription, ActiveDownstream, DownstreamDemand, ActiveUpstream, Downstream {

		final Subscriber<? super Integer> actual;

		final long end;

		volatile boolean cancelled;

		long index;

		volatile long requested;
		@SuppressWarnings("rawtypes")
		static final AtomicLongFieldUpdater<StreamRangeSubscription> REQUESTED =
		  AtomicLongFieldUpdater.newUpdater(StreamRangeSubscription.class, "requested");

		public StreamRangeSubscription(Subscriber<? super Integer> actual, long start, long end) {
			this.actual = actual;
			this.index = start;
			this.end = end;
		}

		@Override
		public void request(long n) {
			if (BackpressureUtils.validate(n)) {
				if (BackpressureUtils.addAndGet(REQUESTED, this, n) == 0) {
					if (n == Long.MAX_VALUE) {
						fastPath();
					} else {
						slowPath(n);
					}
				}
			}
		}

		@Override
		public void cancel() {
			cancelled = true;
		}

		void fastPath() {
			final long e = end;
			final Subscriber<? super Integer> a = actual;

			for (long i = index; i != e; i++) {
				if (cancelled) {
					return;
				}

				a.onNext((int) i);
			}

			if (cancelled) {
				return;
			}

			a.onComplete();
		}

		void slowPath(long n) {
			final Subscriber<? super Integer> a = actual;

			long f = end;
			long e = 0;
			long i = index;

			for (; ; ) {

				if (cancelled) {
					return;
				}

				while (e != n && i != f) {

					a.onNext((int) i);

					if (cancelled) {
						return;
					}

					e++;
					i++;
				}

				if (cancelled) {
					return;
				}

				if (i == f) {
					a.onComplete();
					return;
				}

				n = requested;
				if (n == e) {
					index = i;
					n = REQUESTED.addAndGet(this, -e);
					if (n == 0) {
						return;
					}
					e = 0;
				}
			}
		}

		@Override
		public boolean isCancelled() {
			return cancelled;
		}

		@Override
		public boolean isStarted() {
			return end != index;
		}

		@Override
		public boolean isTerminated() {
			return end == index;
		}

		@Override
		public Object downstream() {
			return actual;
		}

		@Override
		public long requestedFromDownstream() {
			return requested;
		}
	}
}
