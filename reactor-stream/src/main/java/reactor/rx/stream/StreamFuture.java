/*
 * Copyright (c) 2011-2014 Pivotal Software, Inc.
 *
 *  Licensed under the Apache License, Version 2.0 (the "License");
 *  you may not use this file except in compliance with the License.
 *  You may obtain a copy of the License at
 *
 *         http://www.apache.org/licenses/LICENSE-2.0
 *
 *  Unless required by applicable law or agreed to in writing, software
 *  distributed under the License is distributed on an "AS IS" BASIS,
 *  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  See the License for the specific language governing permissions and
 *  limitations under the License.
 */
package reactor.rx.stream;

import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;

import reactor.Flux;
import reactor.core.subscriber.SubscriberWithContext;
import reactor.fn.Consumer;
import reactor.rx.Stream;
import reactor.rx.Streams;

/**
 * A Stream that emits a result of a {@link java.util.concurrent.Future} and then complete.
 * <p>
 * Since the stream retains the future reference in a final field, any
 * {@link org.reactivestreams.Subscriber}
 * will replay the {@link java.util.concurrent.Future#get()}
 * <p>
 * Create such stream with the provided factory, E.g.:
 * <pre>
 * {@code
 * Streams.just(someFuture).consume(
 * log::info,
 * log::error,
 * (-> log.info("complete"))
 * )
 * }
 * </pre>
 * <pre>
 * Will log:
 * {@code
 * 1
 * 2
 * 3
 * 4
 * complete
 * }
 * </pre>
 *
 * @author Stephane Maldini
 */
public final class StreamFuture<T> {

	private StreamFuture() {
	}

	/**
	 *
	 * @param future
	 * @return
	 */
	public static <T> Stream<T> create(Future<? extends T> future){
		return create(future, 0, null);
	}

	/**
	 *
	 * @param future
	 * @param time
	 * @param unit
	 * @return
	 */
	public static <T> Stream<T> create(final Future<? extends T> future,
			final long time,
			final TimeUnit unit){

		if(time < 0 && unit != null){
			throw new IllegalArgumentException("Given time is negative : "+time+" "+unit);
		}
		else if(future.isCancelled()) {
			return Streams.empty();
		}

		return Streams.wrap(Flux.create(new Consumer<SubscriberWithContext<T, Void>>() {
			@Override
			public void accept(SubscriberWithContext<T, Void> s) {
				try {
					T result = unit == null ? future.get() : future.get(time, unit);
					s.onNext(result);
					s.onComplete();
				}
				catch (Exception e) {
					s.onError(e);
				}
			}
		}));
	}
}
