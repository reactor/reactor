package reactor.rx.stream;

import java.util.Objects;
import reactor.fn.*;

import org.reactivestreams.*;

import reactor.core.error.Exceptions;
import reactor.core.subscriber.SubscriberDeferredScalar;
import reactor.core.subscription.EmptySubscription;
import reactor.core.support.BackpressureUtils;

/**
 * Collects the values of the source sequence into a container returned by
 * a supplier and a collector action working on the container and the current source
 * value.
 *
 * @param <T> the source value type
 * @param <R> the container value type
 */

/**
 * {@see https://github.com/reactor/reactive-streams-commons}
 * @since 2.5
 */
public final class MonoCollect<T, R> extends reactor.Mono.MonoBarrier<T, R> {

	final Supplier<R> supplier;

	final BiConsumer<? super R, ? super T> action;

	public MonoCollect(Publisher<? extends T> source, Supplier<R> supplier,
							BiConsumer<? super R, ? super T> action) {
		super(source);
		this.supplier = Objects.requireNonNull(supplier, "supplier");
		this.action = Objects.requireNonNull(action);
	}

	@Override
	public void subscribe(Subscriber<? super R> s) {
		R container;

		try {
			container = supplier.get();
		} catch (Throwable e) {
			EmptySubscription.error(s, e);
			return;
		}

		if (container == null) {
			EmptySubscription.error(s, new NullPointerException("The supplier returned a null container"));
			return;
		}

		source.subscribe(new MonoCollectSubscriber<>(s, action, container));
	}

	static final class MonoCollectSubscriber<T, R>
			extends SubscriberDeferredScalar<T, R>
	implements Upstream {

		final BiConsumer<? super R, ? super T> action;

		final R container;

		Subscription s;

		boolean done;

		public MonoCollectSubscriber(Subscriber<? super R> actual, BiConsumer<? super R, ? super T> action,
										  R container) {
			super(actual);
			this.action = action;
			this.container = container;
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
			if (done) {
				Exceptions.onNextDropped(t);
				return;
			}

			try {
				action.accept(container, t);
			} catch (Throwable e) {
				cancel();

				onError(e);
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
			set(container);
		}

		@Override
		public R get() {
			return container;
		}

		@Override
		public void setValue(R value) {
			// value is constant
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
			return action;
		}

		@Override
		public Object delegateOutput() {
			return container;
		}
	}
}
