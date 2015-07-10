package reactor.rx.stream;

import org.reactivestreams.Subscriber;
import reactor.Environment;
import reactor.ReactorProcessor;
import reactor.core.error.Exceptions;
import reactor.fn.Consumer;
import reactor.fn.Function;
import reactor.rx.Stream;
import reactor.rx.subscription.PushSubscription;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * A {@code BarrierStream} provides a type of {@code Stream} into which you can bind {@link reactor.fn.Consumer
 * Consumers} and {@link reactor.fn.Function Functions} from arbitrary components. This allows you to create a {@code
 * Stream} that will be triggered when all bound callbacks have been invoked. The {@code List&lt;Object&gt;} passed
 * downstream will be the accumulation of all the values either passed into a {@code Consumer} or the return value from
 * a bound {@code Function}. This provides a way for the user to cleanly create a dependency chain of arbitrary
 * callbacks and aggregate the results into a single value.
 * <pre><code>
 * BarrierStream barrierStream = new BarrierStream();
 *
 * EventBus bus = EventBus.create(Environment.get());
 * bus.on($("hello"), barrierStream.wrap((Event<String> ev) -> {
 *   System.out.println("got in bus: " + ev.getData());
 * }));
 *
 * Streams.just("Hello World!")
 *        .map(barrierStream.wrap((Function<String, String>) String::toUpperCase))
 *        .consume(s -> {
 *          System.out.println("got in stream: " + s);
 *        });
 *
 * barrierStream.consume(vals -> {
 *   System.out.println("got vals: " + vals);
 * });
 *
 * bus.notify("hello", Event.wrap("Hello World!"));
 * </code></pre>
 * <p>
 * NOTE: To get blocking semantics for the calling thread, you only need to call {@link reactor.rx.Stream#next()} to return a
 * {@code Promise}.
 * </p>
 */
public class BarrierStream extends Stream<List<Object>> {

	private final AtomicInteger wrappedCnt = new AtomicInteger(0);
	private final AtomicInteger resultCnt  = new AtomicInteger(0);
	private final List<Object>  values     = new ArrayList<Object>();

	private PushSubscription<List<Object>> downstream;

	public BarrierStream() {
	}

	public BarrierStream(Environment env) {
		dispatchOn(env);
	}

	public BarrierStream(ReactorProcessor dispatcher) {
		dispatchOn(dispatcher);
	}

	public BarrierStream(Environment env, ReactorProcessor dispatcher) {
		dispatchOn(env, dispatcher);
	}

	public <I, O> Function<I, O> wrap(final Function<I, O> fn) {
		if (null != downstream && downstream.isComplete()) {
			throw new IllegalStateException("This BarrierStream is already complete");
		}

		final int valIdx = wrappedCnt.getAndIncrement();
		return new Function<I, O>() {
			@SuppressWarnings("unchecked")
			@Override
			public O apply(I in) {
				O out = fn.apply(in);
				addResult(valIdx, out);
				return out;
			}
		};
	}

	public <I> Consumer<I> wrap(final Consumer<I> consumer) {
		if (null != downstream && downstream.isComplete()) {
			throw new IllegalStateException("This BarrierStream is already complete");
		}

		final int valIdx = wrappedCnt.getAndIncrement();
		return new Consumer<I>() {
			@SuppressWarnings("unchecked")
			@Override
			public void accept(I in) {
				consumer.accept(in);
				addResult(valIdx, in);
			}
		};
	}

	@Override
	public void subscribe(Subscriber<? super List<Object>> s) {
		if (null != downstream) {
			throw new IllegalStateException("This BarrierStream already has a Subscriber");
		}

		downstream = new PushSubscription<List<Object>>(this, s) {
			@Override
			public void request(long n) {
				super.request(n);

				if (resultCnt.get() == wrappedCnt.get()) {
					try {
						onNext(values);
						onComplete();
					} catch (Throwable t) {
						onError(t);
					}
				}
			}
		};
		try {
			s.onSubscribe(downstream);
		}catch (Throwable throwable){
			Exceptions.throwIfFatal(throwable);
			s.onError(throwable);
		}
	}

	private void addResult(int idx, Object obj) {
		synchronized (values) {
			if (values.size() == idx) {
				values.add(obj);
			} else if (values.size() < idx) {
				for (int i = values.size(); i < idx; i++) {
					values.add(null);
				}
				values.add(obj);
			} else {
				values.set(idx, obj);
			}
		}
		if (resultCnt.incrementAndGet() == wrappedCnt.get()
		    && null != downstream
		    && !downstream.isComplete()) {
			downstream.onNext(values);
			downstream.onComplete();
		}
	}

}
