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

package reactor.io.net;

import java.net.InetSocketAddress;

import org.reactivestreams.Publisher;
import org.reactivestreams.Subscriber;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import reactor.fn.timer.Timer;
import reactor.io.buffer.Buffer;
import reactor.rx.Stream;
import reactor.rx.Streams;

/**
 * An abstract {@link ReactiveChannel} implementation that handles the basic interaction and behave as a {@link
 * Stream}.
 *
 * @author Stephane Maldini
 */
public class ChannelStream<IN, OUT> extends Stream<IN> implements
                                                       ReactiveChannel<IN, OUT> {

	protected static final Logger log = LoggerFactory.getLogger(ChannelStream.class);


	private final ReactiveChannel<IN, OUT> actual;
	private final Timer                    timer;
	private final long                     prefetch;

	/**
	 *
	 * @param actual
	 * @param timer
	 * @param prefetch
	 * @param <IN>
	 * @param <OUT>
	 * @return
	 */
	public static <IN, OUT> ChannelStream<IN, OUT> wrap(final ReactiveChannel<IN, OUT> actual, Timer timer, long prefetch){
		return new ChannelStream<>(actual, timer, prefetch);
	}

	/**
	 *
	 * @param actual
	 * @param timer
	 * @param prefetch
	 * @param <IN>
	 * @param <OUT>
	 * @return
	 */
	public static <IN, OUT> ReactiveChannelHandler<IN, OUT, ReactiveChannel<IN, OUT>> wrap(
			final ReactiveChannelHandler<IN, OUT, ChannelStream<IN, OUT>> actual,
			final Timer timer,
			final long prefetch){

		if(actual == null) return null;

		return new ReactiveChannelHandler<IN, OUT, ReactiveChannel<IN, OUT>>() {
			@Override
			public Publisher<Void> apply(ReactiveChannel<IN, OUT> stream) {
				return actual.apply(wrap(stream, timer, prefetch));
			}
		};
	}

	protected ChannelStream(final ReactiveChannel<IN, OUT> actual,
							final Timer timer,
	                        long prefetch) {

		this.timer = timer;
		this.actual = actual;
		this.prefetch = prefetch;
	}

	@Override
	public Stream<IN> input() {
		return this;
	}

	@Override
	@SuppressWarnings("unchecked")
	public Stream<Void> writeWith(final Publisher<? extends OUT> source) {
		final Stream<? extends OUT> sourceStream;

		if (Stream.class.isAssignableFrom(source.getClass())) {
			sourceStream = ((Stream<? extends OUT>) source);
		} else {
			sourceStream = new Stream<OUT>() {
				@Override
				public void subscribe(Subscriber<? super OUT> subscriber) {
					source.subscribe(subscriber);
				}

				@Override
				public long getCapacity() {
					return prefetch;
				}

				@Override
				public Timer getTimer() {
					return timer;
				}
			};
		}

		return new Stream<Void>() {
			@Override
			public void subscribe(Subscriber<? super Void> s) {
				actual.writeWith(sourceStream).subscribe(s);
			}

			@Override
			public Timer getTimer() {
				return timer;
			}
		};
	}

	@Override
	final public Stream<Void> writeBufferWith(Publisher<? extends Buffer> source) {

		return Streams.wrap(this.actual.writeBufferWith(source));
	}

	@Override
	public final Timer getTimer() {
		return timer;
	}

	@Override
	final public long getCapacity() {
		return prefetch;
	}

	@Override
	public void subscribe(Subscriber<? super IN> s) {
		actual.input().subscribe(s);
	}

	@Override
	public InetSocketAddress remoteAddress() {
		return actual.remoteAddress();
	}

	@Override
	public ConsumerSpec on() {
		return actual.on();
	}

	@Override
	public Object delegate() {
		return actual.delegate();
	}
}
