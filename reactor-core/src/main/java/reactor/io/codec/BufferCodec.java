/*
 * Copyright (c) 2011-2015 Pivotal Software Inc, All Rights Reserved.
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

package reactor.io.codec;

import org.reactivestreams.Publisher;
import org.reactivestreams.Subscriber;
import reactor.core.publisher.PublisherFactory;
import reactor.core.subscriber.SubscriberBarrier;
import reactor.fn.Function;
import reactor.io.buffer.Buffer;

import java.util.concurrent.atomic.AtomicLongFieldUpdater;

/**
 * Implementations of a {@literal BufferCodec} are codec manipulating Buffer sources
 *
 * @param <IN>  The type produced by decoding
 * @param <OUT> The type consumed by encoding
 * @author Stephane Maldini
 * @since 2.0.4
 */
public abstract class BufferCodec<IN, OUT> extends Codec<Buffer, IN, OUT> {

	/**
	 * Create a new Codec set with a \0 delimiter to finish any Buffer encoded value or scan for delimited decoded
	 * Buffers.
	 */
	protected BufferCodec() {
		super();
	}

	/**
	 * A delimiter can be used to trail any decoded buffer or to finalize encoding from any incoming value
	 *
	 * @param delimiter delimiter can be left undefined (null) to bypass appending at encode time and scanning at decode
	 *                  time.
	 */
	protected BufferCodec(Byte delimiter) {
		super(delimiter);
	}

	@Override
	public Publisher<IN> decode(final Publisher<? extends Buffer> publisherToDecode) {
		if (true) {
			return super.decode(publisherToDecode);
		}
		return PublisherFactory.intercept(publisherToDecode,
				new Function<Subscriber<? super IN>, SubscriberBarrier<Buffer, IN>>() {
					@Override
					public SubscriberBarrier<Buffer, IN> apply(final Subscriber<? super IN> subscriber) {
						return new AggregatingDecoderBarrier<IN>(BufferCodec.this, subscriber);
					}
				});
	}

	@Override
	public Publisher<Buffer> encode(Publisher<? extends OUT> publisherToEncode) {
		if (true) {
			return super.encode(publisherToEncode);
		}
		return PublisherFactory.intercept(publisherToEncode,
				new Function<Subscriber<? super Buffer>, SubscriberBarrier<OUT, Buffer>>() {
					@Override
					public SubscriberBarrier<OUT, Buffer> apply(final Subscriber<? super Buffer> subscriber) {
						return new AggregatingEncoderBarrier(subscriber);
					}
				});
	}

	private static final class AggregatingDecoderBarrier<IN> extends SubscriberBarrier<Buffer, IN> {

		private volatile long pendingDemand = 0l;

		private final static AtomicLongFieldUpdater<AggregatingDecoderBarrier> PENDING_UPDATER =
				AtomicLongFieldUpdater.newUpdater(AggregatingDecoderBarrier.class, "pendingDemand");

		final Buffer               aggregate;
		final Function<Buffer, IN> codec;
		final Byte                 delimiter;

		public AggregatingDecoderBarrier(BufferCodec<IN, ?> codec, Subscriber<? super IN> subscriber) {
			super(subscriber);
			this.codec = codec.decoder();
			this.delimiter = codec.delimiter;
			if (delimiter != null) {
				aggregate = null;
			} else {
				aggregate = null;
			}
		}

		@Override
		protected void doNext(Buffer buffer) {
			long previous = PENDING_UPDATER.decrementAndGet(this);

			if (aggregate != null) {
				aggregate.append(buffer);
				buffer.position(0);
				//split using the delimiter
				if (delimiter != null) {
					int index = buffer.indexOf(delimiter);
					if (index == -1) {
						return;
					}

					int aggregateIndex = aggregate.limit() - buffer.limit() + index;
					Buffer aggregTmp = aggregate.duplicate();
					aggregTmp.position(aggregate.position()).flip();
					for (Buffer.View view : aggregTmp.split(delimiter)) {
						if(view.getEnd() == aggregTmp.limit()) {
							return;
						}

						subscriber.onNext(codec.apply(view.get()));
					}
					aggregate.clear();
				}
				return;
			}
			subscriber.onNext(codec.apply(buffer));
		}

		@Override
		protected void doRequest(long n) {
			long previous = PENDING_UPDATER.getAndAdd(this, n);
			super.doRequest(n);
		}
	}

	private class AggregatingEncoderBarrier extends SubscriberBarrier<OUT, Buffer> {
		final Buffer aggregate = new Buffer();

		public AggregatingEncoderBarrier(Subscriber<? super Buffer> subscriber) {
			super(subscriber);
		}

		@Override
		protected void doNext(OUT src) {
			//subscriber.onNext(src);
		}
	}
}
