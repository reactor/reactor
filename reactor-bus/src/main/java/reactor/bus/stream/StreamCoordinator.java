/*
 * Copyright (c) 2011-2016 Pivotal Software Inc, All Rights Reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *       http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package reactor.bus.stream;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicIntegerFieldUpdater;

import org.reactivestreams.Subscriber;
import org.reactivestreams.Subscription;
import reactor.core.error.Exceptions;
import reactor.fn.Consumer;
import reactor.fn.Function;
import reactor.rx.Stream;

/**
 * A {@code StreamCoordinator} provides a type of {@code Stream} into which you can bind {@link reactor.fn.Consumer
 * Consumers} and {@link reactor.fn.Function Functions} from arbitrary components. This allows you to create a {@code
 * Stream} that will be triggered when all bound callbacks have been invoked. The {@code List&lt;Object&gt;} passed
 * downstream will be the accumulation of all the values either passed into a {@code Consumer} or the return value from
 * a bound {@code Function}. This provides a way for the user to cleanly create a dependency chain of arbitrary
 * callbacks and aggregate the results into a single value.
 * <pre><code>
 * StreamCoordinator coordinator = new StreamCoordinator();
 * <p>
 * EventBus bus = EventBus.create(Environment.get());
 * bus.on($("hello"), StreamCoordinator.wrap((Event<String> ev) -> {
 *   System.out.println("got in bus: " + ev.getData());
 * }));
 * <p>
 * Streams.just("Hello World!")
 *        .map(coordinator.wrap((Function<String, String>) String::toUpperCase))
 *        .consume(s -> {
 *          System.out.println("got in stream: " + s);
 *        });
 * <p>
 * coordinator.consume(vals -> {
 *   System.out.println("got vals: " + vals);
 * });
 * <p>
 * bus.notify("hello", Event.wrap("Hello World!"));
 * </code></pre>
 * <p> NOTE: To get blocking semantics for the calling thread, you only need to call {@link reactor.rx.Stream#next()} to
 * return a {@code Mono}. </p>
 */
public class StreamCoordinator extends Stream<List<Object>> implements Subscription {

	@SuppressWarnings("unused")
	private volatile       int                                          terminated = 0;
	@SuppressWarnings("rawtypes")
	protected static final AtomicIntegerFieldUpdater<StreamCoordinator> TERMINATED =
			AtomicIntegerFieldUpdater.newUpdater(StreamCoordinator.class, "terminated");

	private final AtomicInteger wrappedCnt = new AtomicInteger(0);
	private final AtomicInteger resultCnt  = new AtomicInteger(0);
	private final List<Object>  values     = new ArrayList<Object>();

	private Subscriber<? super List<Object>> downstream;

	public <I, O> Function<I, O> wrap(final Function<I, O> fn) {
		if (null != downstream && TERMINATED.get(this) == 1) {
			throw new IllegalStateException("This StreamCoordinator is already complete");
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
		if (null != downstream && TERMINATED.get(this) == 1) {
			throw new IllegalStateException("This StreamCoordinator is already complete");
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
	public void request(long n) {
		Subscriber<? super List<Object>> s = downstream;
		if(s != null && TERMINATED.get(this) == 0){
			if (resultCnt.get() == wrappedCnt.get()) {
				try {
					s.onNext(values);
					s.onComplete();
				}
				catch (Throwable t) {
					Exceptions.throwIfFatal(t);
					s.onError(t);
				}
			}
		}
	}

	@Override
	public void cancel() {
		TERMINATED.set(this, 1);
	}

	@Override
	public void subscribe(Subscriber<? super List<Object>> s) {
		if (null != downstream) {
			throw new IllegalStateException("This StreamCoordinator already has a Subscriber");
		}

		downstream = s;

		try {
			s.onSubscribe(this);
		}
		catch (Throwable throwable) {
			Exceptions.throwIfFatal(throwable);
			s.onError(throwable);
		}
	}

	private void addResult(int idx, Object obj) {
		synchronized (values) {
			if (values.size() == idx) {
				values.add(obj);
			}
			else if (values.size() < idx) {
				for (int i = values.size(); i < idx; i++) {
					values.add(null);
				}
				values.add(obj);
			}
			else {
				values.set(idx, obj);
			}
		}
		if (resultCnt.incrementAndGet() == wrappedCnt.get() && null != downstream && TERMINATED.get(this) == 0) {
			downstream.onNext(values);
			downstream.onComplete();
		}
	}

}
