package reactor.rx.stream;

import java.util.Objects;
import reactor.fn.Supplier;

import org.reactivestreams.*;

import reactor.core.subscription.EmptySubscription;
import reactor.core.support.ReactiveState;

/**
 * Defers the creation of the actual Publisher the Subscriber will be subscribed to.
 *
 * @param <T> the value type
 */

/**
 * {@see <a href='https://github.com/reactor/reactive-streams-commons'>https://github.com/reactor/reactive-streams-commons</a>}
 * @since 2.5
 */
public final class StreamDefer<T> 
extends reactor.rx.Stream<T>
implements 
												ReactiveState.Factory,
												ReactiveState.Upstream {

	final Supplier<? extends Publisher<? extends T>> supplier;

	public StreamDefer(Supplier<? extends Publisher<? extends T>> supplier) {
		this.supplier = Objects.requireNonNull(supplier, "supplier");
	}

	@Override
	public Object upstream() {
		return supplier;
	}

	@Override
	public void subscribe(Subscriber<? super T> s) {
		Publisher<? extends T> p;

		try {
			p = supplier.get();
		} catch (Throwable e) {
			EmptySubscription.error(s, e);
			return;
		}

		if (p == null) {
			EmptySubscription.error(s, new NullPointerException("The Producer returned by the supplier is null"));
			return;
		}

		p.subscribe(s);
	}
}
