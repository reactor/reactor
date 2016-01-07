package reactor.rx.stream;

import org.reactivestreams.*;

import reactor.core.subscriber.SubscriberDeferredScalar;
import reactor.core.support.BackpressureUtils;

/**
 * Counts the number of values in the source sequence.
 *
 * @param <T> the source value type
 */

/**
 * {@see https://github.com/reactor/reactive-streams-commons}
 * @since 2.5
 */
public final class MonoCount<T> extends reactor.Mono.MonoBarrier<T, Long> {

	public MonoCount(Publisher<? extends T> source) {
		super(source);
	}

	@Override
	public void subscribe(Subscriber<? super Long> s) {
		source.subscribe(new MonoCountSubscriber<>(s));
	}

	static final class MonoCountSubscriber<T> extends SubscriberDeferredScalar<T, Long>
	implements Upstream {

		long counter;

		Subscription s;

		public MonoCountSubscriber(Subscriber<? super Long> actual) {
			super(actual);
		}

		@Override
		public void cancel() {
			super.cancel();
			s.cancel();
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
			counter++;
		}

		@Override
		public void onComplete() {
			set(counter);
		}

		@Override
		public Object upstream() {
			return s;
		}

	}
}
