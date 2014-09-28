/*
 * Copyright (c) 2011-2013 GoPivotal, Inc. All Rights Reserved.
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
package reactor.rx.stream;

import org.reactivestreams.Subscriber;
import reactor.event.dispatch.Dispatcher;
import reactor.rx.Stream;
import reactor.rx.StreamSubscription;

import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;

/**
 * A Stream that emits a result of a {@link java.util.concurrent.Future} and then complete.
 * <p>
 * Since the stream retains the future reference in a final field, any {@link this#subscribe(org.reactivestreams
 * .Subscriber)}
 * will replay the {@link java.util.concurrent.Future#get()}
 * <p>
 * Create such stream with the provided factory, E.g.:
 * {@code
 * Streams.defer(someFuture).consume(
 *log::info,
 *log::error,
 * (-> log.info("complete"))
 * )
 * }
 * <p>
 * Will log:
 * 1
 * 2
 * 3
 * 4
 * complete
 *
 * @author Stephane Maldini
 */
public class FutureStream<T> extends Stream<T> {

	private final Future<? extends T> future;
	private final long                time;
	private final TimeUnit            unit;

	public FutureStream(Future<? extends T> future,
	                    Dispatcher dispatcher) {
		this(future, 0, TimeUnit.SECONDS, dispatcher);
	}

	public FutureStream(Future<? extends T> future,
	                    long time,
	                    TimeUnit unit,
	                    Dispatcher dispatcher) {
		super(dispatcher);

		this.future = future;
		this.time = time;
		this.unit = unit;

		capacity(1);

		keepAlive(true);
	}

	@Override
	protected StreamSubscription<T> createSubscription(Subscriber<? super T> subscriber,
	                                                   boolean reactivePull) {
		return new StreamSubscription<T>(this, subscriber) {

			@Override
			public void request(long elements) {
				super.request(elements);

				if (buffer.isComplete()) return;

				try {
					T result = unit == null ? future.get() : future.get(time, unit);

					buffer.complete();

					onNext(result);
					onComplete();

				} catch (Throwable e) {
					onError(e);
					state = State.ERROR;
					error = e;
				}
			}
		};
	}
}
