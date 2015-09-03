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
import reactor.core.publisher.PublisherFactory;
import reactor.core.subscriber.SubscriberWithContext;
import reactor.fn.Consumer;
import reactor.fn.Function;
import reactor.rx.Stream;
import reactor.rx.Streams;

/**
 * A Stream that emits N {@link java.lang.Long} from the inclusive start value defined to the inclusive end and then
 * complete.
 * <p>
 * Since the stream retains the boundaries in a final field, any {@link org.reactivestreams.Subscriber}
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
public final class RangeStream {


	/**
	 * Create a Range Stream Publisher
	 *
	 * @param min
	 * @param max
	 * @return
	 */
	public static Stream<Long> create(final long min, final long max) {
		return Streams.wrap(PublisherFactory.create(new Consumer<SubscriberWithContext<Long, Range>>() {
			  @Override
			  public void accept(SubscriberWithContext<Long, Range> subscriber) {
				  Range range = subscriber.context();

				  if (range.cursor <= range.end) {
					  subscriber.onNext(range.cursor++);
				  }
				  if (range.cursor > range.end) {
					  subscriber.onComplete();
				  }
			  }
		  }, new Function<Subscriber<? super Long>, Range>() {
			  @Override
			  public Range apply(Subscriber<? super Long> subscriber) {
				  if (max < min) {
					  subscriber.onComplete();
					  throw PublisherFactory.PrematureCompleteException.INSTANCE;
				  }
				  return new Range(min, max);
			  }
		  })
		);
	}

	private final static class Range {
		final long start;
		final long end;

		long cursor;

		public Range(long start, long end) {
			this.start = start;
			this.end = end;
			cursor = start;
		}

		@Override
		public String toString() {
			return "{" +
			  "cursor=" + cursor + "" + (end > 0 ? "[" + 100 * (cursor - 1) / end + "%]" : "") +
			  ", start=" + start + ", end=" + end + "}";
		}
	}
}
