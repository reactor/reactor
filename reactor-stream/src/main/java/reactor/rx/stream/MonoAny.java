package reactor.rx.stream;

import java.util.Objects;
import reactor.fn.Predicate;

import org.reactivestreams.*;

import reactor.core.error.Exceptions;
import reactor.core.subscriber.SubscriberDeferredScalar;
import reactor.core.support.BackpressureUtils;

/**
 * Emits a single boolean true if any of the values of the source sequence match
 * the predicate.
 * <p>
 * The implementation uses short-circuit logic and completes with true if
 * the predicate matches a value.
 *
 * @param <T> the source value type
 */

/**
 * {@see https://github.com/reactor/reactive-streams-commons}
 * @since 2.5
 */
public final class MonoAny<T> extends reactor.Mono.MonoBarrier<T, Boolean> {

	final Predicate<? super T> predicate;

	public MonoAny(Publisher<? extends T> source, Predicate<? super T> predicate) {
		super(source);
		this.predicate = Objects.requireNonNull(predicate, "predicate");
	}

	@Override
	public void subscribe(Subscriber<? super Boolean> s) {
		source.subscribe(new MonoAnySubscriber<T>(s, predicate));
	}

	static final class MonoAnySubscriber<T> extends SubscriberDeferredScalar<T, Boolean>
	implements Upstream {
		final Predicate<? super T> predicate;

		Subscription s;

		boolean done;

		public MonoAnySubscriber(Subscriber<? super Boolean> actual, Predicate<? super T> predicate) {
			super(actual);
			this.predicate = predicate;
		}

		@Override
		public void cancel() {
			s.cancel();
			super.cancel();
		}

		@Override
		public void onSubscribe(Subscription s) {
			if (BackpressureUtils.validate(this.s, s)) {
				this.s = s;

				subscriber.onSubscribe(this);

				s.request(Long.MAX_VALUE);
			}
		}

		@Override
		public void onNext(T t) {

			if (done) {
				return;
			}

			boolean b;

			try {
				b = predicate.test(t);
			} catch (Throwable e) {
				done = true;
				s.cancel();

				subscriber.onError(e);
				return;
			}
			if (b) {
				done = true;
				s.cancel();

				set(true);
			}
		}

		@Override
		public void onError(Throwable t) {
			if (done) {
				Exceptions.onErrorDropped(t);
				return;
			}
			done = true;

			subscriber.onError(t);
		}

		@Override
		public void onComplete() {
			if (done) {
				return;
			}
			done = true;
			set(false);
		}

		@Override
		public boolean isTerminated() {
			return done;
		}

		@Override
		public Object upstream() {
			return s;
		}

		@Override
		public Object delegateInput() {
			return predicate;
		}
	}
}
