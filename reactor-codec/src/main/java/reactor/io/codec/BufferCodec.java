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

import java.util.Iterator;
import java.util.concurrent.atomic.AtomicLongFieldUpdater;

import org.reactivestreams.Publisher;
import org.reactivestreams.Subscriber;
import reactor.Publishers;
import reactor.core.subscriber.SubscriberBarrier;
import reactor.core.support.BackpressureUtils;
import reactor.fn.Consumer;
import reactor.fn.Function;
import reactor.fn.Supplier;
import reactor.io.buffer.Buffer;

/**
 * Implementations of a {@literal BufferCodec} are codec manipulating Buffer sources
 * @param <IN> The type produced by decoding
 * @param <OUT> The type consumed by encoding
 * @author Stephane Maldini
 * @since 2.0.4
 */
public abstract class BufferCodec<IN, OUT> extends Codec<Buffer, IN, OUT> {

	/**
	 * Create a new Codec set with a \0 delimiter to finish any Buffer encoded value or
	 * scan for delimited decoded Buffers.
	 */
	protected BufferCodec() {
		super();
	}

	/**
	 * A delimiter can be used to trail any decoded buffer or to finalize encoding from
	 * any incoming value
	 * @param delimiter delimiter can be left undefined (null) to bypass appending at
	 * encode time and scanning at decode time.
	 */
	protected BufferCodec(Byte delimiter) {
		super(delimiter);
	}

	/**
	 * A delimiter can be used to trail any decoded buffer or to finalize encoding from
	 * any incoming value
	 * @param delimiter delimiter can be left undefined (null) to bypass appending at
	 * encode time and scanning at decode time.
	 */
	protected BufferCodec(Byte delimiter, Supplier<?> decoderContext) {
		super(delimiter, decoderContext);
	}

	protected int canDecodeNext(Buffer buffer, Object context) {
		return delimiter == null || buffer == null ? -1 : buffer.indexOf(delimiter);
	}

	protected Iterator<Buffer.View> iterateDecode(final Buffer buffer, final Object context) {
		return new Iterator<Buffer.View>() {

			@Override
			public boolean hasNext() {
				return canDecodeNext(buffer, context) != -1;
			}

			@Override
			public Buffer.View next() {
				int limit = buffer.limit();
				Buffer.View view =
						buffer.createView(buffer.position() != 0 ? buffer.position() + 1 : buffer.position(), canDecodeNext(buffer, context));

				if(buffer.remaining() > 0) {
					buffer.position(view.getEnd());
					buffer.limit(limit);
				}
				return view;
			}
		};
	}

	@Override
	public Publisher<IN> decode(final Publisher<Buffer> publisherToDecode) {
		return Publishers.lift(publisherToDecode, new Function<Subscriber<? super IN>, Subscriber<? super Buffer>>() {
			@Override
			public Subscriber<? super Buffer> apply(
					final Subscriber<? super IN> subscriber) {
				return new AggregatingDecoderBarrier<IN>(BufferCodec.this, subscriber);
			}
		});
	}

	private static final class AggregatingDecoderBarrier<IN>
			extends SubscriberBarrier<Buffer, IN> {

		private volatile long pendingDemand = 0l;

		private final static AtomicLongFieldUpdater<AggregatingDecoderBarrier>
				PENDING_UPDATER =
				AtomicLongFieldUpdater.newUpdater(AggregatingDecoderBarrier.class, "pendingDemand");

		final BufferCodec<IN, ?> codec;
		final Object             decoderContext;

		Buffer aggregate;

		public AggregatingDecoderBarrier(BufferCodec<IN, ?> codec,
				Subscriber<? super IN> subscriber) {
			super(subscriber);
			this.codec = codec;
			this.decoderContext = codec.decoderContextProvider.get();
		}

		@Override
		protected void doNext(Buffer buffer) {
			if (aggregate != null) {
				aggregate = new Buffer().append(aggregate)
				                        .append(buffer)
				                        .flip();
			}
			else if (-1L == codec.canDecodeNext(buffer, decoderContext)) {
				aggregate = buffer;
				super.doRequest(1L);
				return;
			}

			if (aggregate == null) {
				decodeAndNext(buffer);
			}
			else {
				Buffer agg = aggregate;

				if(-1L != codec.canDecodeNext(agg, decoderContext)){
					aggregate = null;
					decodeAndNext(agg);
				}
				else {
					super.doRequest(1L);
				}
			}
		}

		@Override
		protected void doComplete() {
			Buffer agg = aggregate;
			if (agg != null) {
				aggregate = null;
				decodeAndNext(agg.position(0));
			}
			super.doComplete();
		}

		private void decodeAndNext(Buffer buffer) {

			IN next;

			Iterator<Buffer.View> views = codec.iterateDecode(buffer, decoderContext);

			if (!views.hasNext()) {
				aggregate = buffer;
				return;
			}

			Buffer.View cursor;
			while (views.hasNext()) {
				cursor = views.next();
				if (BackpressureUtils.getAndSub(PENDING_UPDATER, this, 1L) > 0) {
					next = codec.decodeNext(cursor.get(), decoderContext);
					if (next != null) {
						subscriber.onNext(next);
					}
					else {
						aggregate =
								new Buffer().append(buffer.slice(cursor.getStart(), buffer.limit()))
								            .append(aggregate)
								            .flip();
						return;
					}
				}
				else {
					aggregate =
							new Buffer().append(buffer.slice(cursor.getStart(), buffer.limit()))
							            .append(aggregate)
							            .flip();
					return;
				}
			}
			if (buffer.position() != buffer.limit()) {
				aggregate = buffer.duplicate();
			}
		}

		@Override
		protected void doRequest(long n) {
			if (BackpressureUtils.getAndAdd(PENDING_UPDATER, this, n) == 0) {
				super.doRequest(n);
				Buffer agg = aggregate;
				if(agg != null){
					decodeAndNext(agg);
				}
			}
			//TODO deal with complete and remaining
		}
	}

	protected final class BufferInvokeOrReturnFunction<C>
			extends DefaultInvokeOrReturnFunction<C> {

		public BufferInvokeOrReturnFunction(Consumer<IN> consumer, C context) {
			super(consumer, context);
		}

		@Override
		public IN apply(Buffer buffer) {
			if (consumer != null) {
				int pos;
				while ((pos = buffer.position()) < buffer.limit()) {
					super.apply(buffer);
					if (pos == buffer.position()) {
						break;
					}
				}
				return null;
			}
			return super.apply(buffer);
		}
	}
}
