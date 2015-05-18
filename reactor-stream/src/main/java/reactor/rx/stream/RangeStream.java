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

import org.reactivestreams.Subscriber;
import reactor.core.support.Exceptions;
import reactor.rx.Stream;
import reactor.rx.subscription.PushSubscription;

/**
 * A Stream that emits N {@link java.lang.Long} from the inclusive start value defined to the inclusive end and then
 * complete.
 * <p>
 * Since the stream retains the boundaries in a final field, any {@link this#subscribe(org.reactivestreams.Subscriber)}
 * will replay all the range. This is a "Cold" stream.
 * <p>
 * Create such stream with the provided factory, E.g.:
 * <pre>
 * {@code
 * Streams.range(1, 10000).consume(
 *    log::info,
 *    log::error,
 *    (-> log.info("complete"))
 * )
 * }
 * </pre>
 * <pre>
 * {@code
 * Will log:
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
public final class RangeStream extends Stream<Long> {

	private final long start;
	private final long end;

	public RangeStream(long start, long end) {
		this.start = start;
		this.end = end;
	}

	@Override
	public void subscribe(Subscriber<? super Long> subscriber) {
		try {
			if (start <= end) {
				subscriber.onSubscribe(new PushSubscription<Long>(this, subscriber) {
					Long cursor = start;

					@Override
					public void request(long elements) {

						long l = 0;
						while (l < elements && cursor <= end) {
							if (isComplete()) return;
							onNext(cursor++);
							l++;
						}
						if (cursor > end) {
							onComplete();
						}
					}

					@Override
					public String toString() {
						return "{" +
								"cursor=" + cursor + "" + (end > 0 ? "[" + 100 * (cursor - 1) / end + "%]" : "") +
								", start=" + start + ", end=" + end + "}";
					}
				});
			} else {
				subscriber.onComplete();
			}
		} catch (Throwable throwable) {
			Exceptions.throwIfFatal(throwable);
			subscriber.onError(throwable);
		}
	}


	@Override
	public String toString() {
		return super.toString() + " [" + start + " to " + end + "]";
	}
}
