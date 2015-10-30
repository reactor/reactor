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
import java.util.concurrent.atomic.AtomicIntegerFieldUpdater;
import java.util.concurrent.atomic.AtomicLongFieldUpdater;
import java.util.concurrent.atomic.AtomicReferenceFieldUpdater;

import org.reactivestreams.Publisher;
import org.reactivestreams.Subscriber;
import reactor.Publishers;
import reactor.core.subscriber.SubscriberBarrier;
import reactor.core.support.BackpressureUtils;
import reactor.core.support.internal.PlatformDependent;
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
		return delimiter == null ? (buffer.remaining() > 0 ? buffer.limit() : -1) :
				buffer.indexOf(delimiter);
	}

	protected Iterator<Buffer.View> iterateDecode(final Buffer buffer,
			final Object context) {
		return new DecodedViewIterator(buffer, context);
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

		private final static AtomicReferenceFieldUpdater<AggregatingDecoderBarrier, Buffer>
				AGGREGATE =
				PlatformDependent.newAtomicReferenceFieldUpdater(AggregatingDecoderBarrier.class, "aggregate");

		private final static AtomicIntegerFieldUpdater<AggregatingDecoderBarrier>
				TERMINATED =
				AtomicIntegerFieldUpdater.newUpdater(AggregatingDecoderBarrier.class, "terminated");

		private final static AtomicLongFieldUpdater<AggregatingDecoderBarrier>
				PENDING_UPDATER =
				AtomicLongFieldUpdater.newUpdater(AggregatingDecoderBarrier.class, "pendingDemand");

		final BufferCodec<IN, ?> codec;
		final Object             decoderContext;

		private volatile long pendingDemand = 0l;
		private volatile int  terminated    = 0;
		private volatile Buffer aggregate;

		public AggregatingDecoderBarrier(BufferCodec<IN, ?> codec,
				Subscriber<? super IN> subscriber) {
			super(subscriber);
			this.codec = codec;
			this.decoderContext = codec.decoderContextProvider.get();
		}

		private boolean tryDrain() {
			Buffer agg = aggregate;
			return agg == null || tryEmit(agg);
		}

		@Override
		protected void doRequest(long n) {
			if (BackpressureUtils.getAndAdd(PENDING_UPDATER, this, n) == 0) {
				super.doRequest(n);
				if (!tryDrain()) {
					requestMissing();
				}
			}
			else if (terminated == 1) {
				if (tryDrain() && TERMINATED.compareAndSet(this, 1, 2)) {
					super.doComplete();
				}
			}
		}

		private void requestMissing(){
			if(pendingDemand != Long.MAX_VALUE){
				super.doRequest(1L);
			}
		}

		@Override
		protected void doNext(Buffer buffer) {
			Buffer aggregate = this.aggregate;
			if (aggregate != null) {
				aggregate = combine(buffer);
			}
			else if (-1L == codec.canDecodeNext(buffer, decoderContext)) {
				combine(buffer);
				requestMissing();
				return;
			}

			if (aggregate == null) {
				tryEmit(buffer);
				requestMissing();
			}
			else {
				if (-1L == codec.canDecodeNext(aggregate, decoderContext)) {
					requestMissing();
				}
				else if (AGGREGATE.compareAndSet(this, aggregate, null)) {
					if (!tryEmit(aggregate)) {
						requestMissing();
					}
				}
				else {
					requestMissing();
				}
			}
		}

		private Buffer combine(Buffer buffer) {
			Buffer aggregate = this.aggregate;
			Buffer combined;
			for (; ; ) {
				combined = buffer.newBuffer()
				                 .append(aggregate)
				                 .append(buffer)
				                 .flip();

				if (AGGREGATE.compareAndSet(this, aggregate, combined)) {
					return combined;
				}
				aggregate = this.aggregate;
			}
		}

		@Override
		protected void doError(Throwable throwable) {
			if (TERMINATED.compareAndSet(this, 0, 1)) {
				tryDrain();
				super.doError(throwable);
			}
		}

		@Override
		protected void doComplete() {
			if (TERMINATED.compareAndSet(this, 0, 1)) {
				if (tryDrain()) {
					super.doComplete();
				}
			}
		}

		private boolean tryEmit(Buffer buffer) {

			IN next;

			Iterator<Buffer.View> views = codec.iterateDecode(buffer, decoderContext);

			if (!views.hasNext()) {
				combine(buffer);
				return false;
			}

			Buffer.View cursor;

			while (views.hasNext()) {
				cursor = views.next();
				if (cursor != null) {
					next = codec.decodeNext(cursor.get(), decoderContext);
					if (next != null && BackpressureUtils.getAndSub(PENDING_UPDATER, this, 1L) > 0) {
						subscriber.onNext(next);
					}
					else {
						combine(buffer.slice(cursor.getStart(), buffer.limit()));
						return next != null;
					}
				}
				else {
					combine(buffer);
					return false;
				}
			}
			if (buffer.remaining() > 0) {
				combine(buffer);
				return false;
			}
			return true;
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

	protected class DecodedViewIterator implements Iterator<Buffer.View> {

		protected final Buffer buffer;
		protected final Object context;

		public DecodedViewIterator(Buffer buffer, Object context) {
			this.buffer = buffer;
			this.context = context;
		}

		@Override
		public boolean hasNext() {
			return canDecodeNext(buffer, context) != -1;
		}

		@Override
		public Buffer.View next() {
			int limit = buffer.limit();
			int endchunk = canDecodeNext(buffer, context);

			if (endchunk == -1) {
				return null;
			}

			Buffer.View view = buffer.createView(buffer.position(), endchunk);

			if (buffer.remaining() > 0) {
				buffer.position(Math.min(limit, view.getEnd()));
				buffer.limit(limit);
			}
			return view;
		}
	}
}
