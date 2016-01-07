package reactor.rx.stream;

import org.reactivestreams.*;

import reactor.core.subscriber.SubscriberDeferredScalar;
import reactor.core.support.BackpressureUtils;


/**
 * {@see https://github.com/reactor/reactive-streams-commons}
 * @since 2.5
 */
public final class MonoIsEmpty<T> extends reactor.Mono.MonoBarrier<T, Boolean> {

	public MonoIsEmpty(Publisher<? extends T> source) {
		super(source);
	}

	@Override
	public void subscribe(Subscriber<? super Boolean> s) {
		source.subscribe(new MonoIsEmptySubscriber<>(s));
	}

	static final class MonoIsEmptySubscriber<T> extends SubscriberDeferredScalar<T, Boolean>
	implements Upstream {
		Subscription s;

		public MonoIsEmptySubscriber(Subscriber<? super Boolean> actual) {
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
			s.cancel();

			set(false);
		}

		@Override
		public void onComplete() {
			set(true);
		}

		@Override
		public Object upstream() {
			return s;
		}
	}
}
