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

package reactor.io.net.impl.netty.tcp;

import java.util.concurrent.atomic.AtomicIntegerFieldUpdater;
import java.util.concurrent.atomic.AtomicLongFieldUpdater;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;
import io.netty.channel.ChannelDuplexHandler;
import io.netty.channel.ChannelFuture;
import io.netty.channel.ChannelFutureListener;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.ChannelPromise;
import io.netty.util.ReferenceCountUtil;
import org.reactivestreams.Publisher;
import org.reactivestreams.Subscriber;
import org.reactivestreams.Subscription;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import reactor.core.error.CancelException;
import reactor.core.error.Exceptions;
import reactor.core.error.ReactorFatalException;
import reactor.core.processor.rb.disruptor.RingBuffer;
import reactor.core.processor.rb.disruptor.Sequence;
import reactor.core.processor.rb.disruptor.Sequencer;
import reactor.core.subscriber.BaseSubscriber;
import reactor.core.support.BackpressureUtils;
import reactor.core.support.Bounded;
import reactor.core.support.SignalType;
import reactor.fn.Consumer;
import reactor.fn.Supplier;
import reactor.io.buffer.Buffer;
import reactor.io.net.ReactiveChannel;
import reactor.io.net.ReactiveChannelHandler;
import reactor.io.net.impl.netty.NettyBuffer;
import reactor.io.net.impl.netty.NettyChannel;

/**
 * Netty {@link io.netty.channel.ChannelInboundHandler} implementation that passes data to a Reactor {@link
 * reactor.io.net.ReactiveChannel}.
 * @author Jon Brisbin
 * @author Stephane Maldini
 */
public class NettyChannelHandlerBridge extends ChannelDuplexHandler {

	protected static final Logger log = LoggerFactory.getLogger(NettyChannelHandlerBridge.class);

	protected final ReactiveChannelHandler<Buffer, Buffer, ReactiveChannel<Buffer, Buffer>> handler;
	protected final NettyChannel                                                            reactorNettyChannel;

	protected ChannelInputSubscriber channelSubscriber;

	private volatile       int                                                  channelRef  = 0;
	protected static final AtomicIntegerFieldUpdater<NettyChannelHandlerBridge> CHANNEL_REF =
			AtomicIntegerFieldUpdater.newUpdater(NettyChannelHandlerBridge.class, "channelRef");

	public NettyChannelHandlerBridge(ReactiveChannelHandler<Buffer, Buffer, ReactiveChannel<Buffer, Buffer>> handler,
			NettyChannel reactorNettyChannel) {
		this.handler = handler;
		this.reactorNettyChannel = reactorNettyChannel;
	}

	public ChannelInputSubscriber subscription() {
		return channelSubscriber;
	}

	@Override
	public void userEventTriggered(final ChannelHandlerContext ctx, Object evt) throws Exception {
		if (evt != null && evt.getClass()
		                      .equals(ChannelInputSubscriber.class)) {

			@SuppressWarnings("unchecked") ChannelInputSubscriber subscriberEvent = (ChannelInputSubscriber) evt;

			if (null == channelSubscriber) {
				CHANNEL_REF.incrementAndGet(NettyChannelHandlerBridge.this);
				channelSubscriber = subscriberEvent;
				subscriberEvent.onSubscribe(new Subscription() {
					@Override
					public void request(long n) {
						if (n == Long.MAX_VALUE) {
							ctx.channel()
							   .config()
							   .setAutoRead(true);
						}
						ctx.read();
					}

					@Override
					public void cancel() {
						channelSubscriber = null;
						//log.debug("Cancel read");
						ctx.channel()
						   .config()
						   .setAutoRead(false);
						CHANNEL_REF.decrementAndGet(NettyChannelHandlerBridge.this);
					}
				});

			}
			else {
				channelSubscriber.onSubscribe(SignalType.NOOP_SUBSCRIPTION);
				channelSubscriber.onError(new IllegalStateException("Only one connection input subscriber allowed."));
			}
		}
		super.userEventTriggered(ctx, evt);
	}

	@Override
	public void channelActive(final ChannelHandlerContext ctx) throws Exception {
		super.channelActive(ctx);
		handler.apply(reactorNettyChannel)
		       .subscribe(new BaseSubscriber<Void>() {
			       @Override
			       public void onSubscribe(Subscription s) {
				       super.onSubscribe(s);
				       s.request(Long.MAX_VALUE);
			       }

			       @Override
			       public void onError(Throwable t) {
				       super.onError(t);
				       log.error("Error processing connection. Closing the channel.", t);

				       ctx.writeAndFlush(Unpooled.EMPTY_BUFFER)
				          .addListener(ChannelFutureListener.CLOSE);
			       }

			       @Override
			       public void onComplete() {
				       ctx.writeAndFlush(Unpooled.EMPTY_BUFFER)
				          .addListener(ChannelFutureListener.CLOSE);
			       }
		       });
	}

	@Override
	public void channelReadComplete(ChannelHandlerContext ctx) throws Exception {
		if (channelSubscriber == null) {
			return;
		}

		try {
			super.channelReadComplete(ctx);
			if (channelSubscriber.shouldReadMore()) {
				ctx.read();
			}

		}
		catch (Throwable err) {
			Exceptions.throwIfFatal(err);
			if (channelSubscriber != null) {
				channelSubscriber.onError(err);
			}
			else {
				throw err;
			}
		}
	}

	@Override
	public void channelInactive(ChannelHandlerContext ctx) throws Exception {
		try {
			if (this.channelSubscriber != null) {
				channelSubscriber.onComplete();
				channelSubscriber = null;
			}
			super.channelInactive(ctx);
		}
		catch (Throwable err) {
			Exceptions.throwIfFatal(err);
			if (channelSubscriber != null) {
				channelSubscriber.onError(err);
			}
			else {
				throw err;
			}
		}
	}

	@Override
	public void channelRead(ChannelHandlerContext ctx, Object msg) throws Exception {
		doRead(ctx, msg);
	}

	@SuppressWarnings("unchecked")
	protected final void doRead(ChannelHandlerContext ctx, Object msg) {
		try {
			if (null == channelSubscriber || msg == Unpooled.EMPTY_BUFFER) {
				ReferenceCountUtil.release(msg);
				return;
			}

			NettyBuffer buffer = NettyBuffer.create(msg);
			try {
				channelSubscriber.onNext(buffer);
			}
			finally {
				if (buffer.getByteBuf() != null) {
					if (buffer.getByteBuf()
					          .refCnt() != 0) {
						ReferenceCountUtil.release(buffer.getByteBuf());
					}
				}
			}
		}
		catch (Throwable err) {
			Exceptions.throwIfFatal(err);
			if (channelSubscriber != null) {
				channelSubscriber.onError(err);
			}
			else {
				throw err;
			}
		}
	}

	@Override
	public void write(final ChannelHandlerContext ctx, Object msg, final ChannelPromise promise) throws Exception {
		if (msg instanceof Publisher) {
			CHANNEL_REF.incrementAndGet(this);

			@SuppressWarnings("unchecked") Publisher<?> data = (Publisher<?>) msg;
			final long capacity = msg instanceof Bounded ? ((Bounded) data).getCapacity() : Long.MAX_VALUE;

			if (capacity == Long.MAX_VALUE) {
				data.subscribe(new FlushOnTerminateSubscriber(ctx, promise));
			}
			else {
				data.subscribe(new FlushOnCapacitySubscriber(ctx, promise, capacity));
			}
		}
		else {
			super.write(ctx, msg, promise);
		}
	}

	@Override
	public void exceptionCaught(ChannelHandlerContext ctx, Throwable err) throws Exception {
		Exceptions.throwIfFatal(err);
		if (channelSubscriber != null) {
			channelSubscriber.onError(err);
		}
		else {
			log.error("Unexpected issue", err);
		}
	}

	protected ChannelFuture doOnWrite(Object data, ChannelHandlerContext ctx) {
		if (Buffer.class.isAssignableFrom(data.getClass())) {
			if (NettyBuffer.class.equals(data.getClass())) {
				return ctx.write(((NettyBuffer) data).get());
			}
			return ctx.channel()
			          .write(convertBufferToByteBuff(ctx, (Buffer) data));
		}
		else if (Unpooled.EMPTY_BUFFER != data) {
			return ctx.channel()
			          .write(data);
		}
		return null;
	}

	protected static ByteBuf convertBufferToByteBuff(ChannelHandlerContext ctx, Buffer data) {
		ByteBuf buff = ctx.alloc()
		                  .buffer(data.remaining());
		return buff.writeBytes(data.byteBuffer());
	}

	protected void doOnTerminate(ChannelHandlerContext ctx, ChannelFuture last, final ChannelPromise promise) {
		CHANNEL_REF.decrementAndGet(this);

		if (ctx.channel()
		       .isOpen()) {
			ChannelFutureListener listener = new ChannelFutureListener() {
				@Override
				public void operationComplete(ChannelFuture future) throws Exception {
					if (future.isSuccess()) {
						promise.trySuccess();
					}
					else {
						promise.tryFailure(future.cause());
					}
				}
			};

			if (last != null) {
				ctx.flush();
				last.addListener(listener);
			}
			else {
				ctx.writeAndFlush(Unpooled.EMPTY_BUFFER)
				   .addListener(listener);
			}
		}
		else {
			promise.trySuccess();
		}
	}

	@SuppressWarnings("unused")
	protected void doOnSubscribe(ChannelHandlerContext ctx,
			final Subscription s,
			long request,
			final Consumer<Void> cb) {
		ctx.channel()
		   .closeFuture()
		   .addListener(new ChannelFutureListener() {
			   @Override
			   public void operationComplete(ChannelFuture future) throws Exception {
				   if (log.isDebugEnabled()) {
					   log.debug("Cancel connection");
				   }
				   s.cancel();
				   cb.accept(null);
			   }
		   });
		s.request(request);
	}

	/**
	 * An event to attach a {@link Subscriber} to the {@link NettyChannel} created by {@link NettyChannelHandlerBridge}
	 */
	public static final class ChannelInputSubscriber implements Subscription, Subscriber<Buffer> {

		private final Subscriber<? super Buffer> inputSubscriber;

		private volatile Subscription subscription;

		@SuppressWarnings("unused")
		private volatile int                                               terminated = 0;
		private final    AtomicIntegerFieldUpdater<ChannelInputSubscriber> TERMINATED =
				AtomicIntegerFieldUpdater.newUpdater(ChannelInputSubscriber.class, "terminated");

		@SuppressWarnings("unused")
		private volatile long requested;
		private final AtomicLongFieldUpdater<ChannelInputSubscriber> REQUESTED =
				AtomicLongFieldUpdater.newUpdater(ChannelInputSubscriber.class, "requested");

		@SuppressWarnings("unused")
		private volatile int running;
		private final AtomicIntegerFieldUpdater<ChannelInputSubscriber> RUNNING =
				AtomicIntegerFieldUpdater.newUpdater(ChannelInputSubscriber.class, "running");

		Sequence pollCursor;
		volatile Throwable error;
		volatile RingBuffer<RingBuffer.Slot<Buffer>> readBackpressureBuffer;

		final int bufferSize;

		public ChannelInputSubscriber(Subscriber<? super Buffer> inputSubscriber, long bufferSize) {
			if (null == inputSubscriber) {
				throw new IllegalArgumentException("Connection input subscriber must not be null.");
			}
			this.inputSubscriber = inputSubscriber;
			this.bufferSize = (int) Math.min(Math.max(bufferSize, 32), 128);
		}

		@Override
		public void request(long n) {
			if (terminated == 1) {
				return;
			}
			if (BackpressureUtils.checkRequest(n, inputSubscriber)) {
				if (BackpressureUtils.getAndAdd(REQUESTED, this, n) == 0 && n == Long.MAX_VALUE) {
					Subscription subscription = this.subscription;
					if (subscription != null) {
						subscription.request(n);
					}
				}
				else {
					drain();
				}
			}
		}

		@Override
		public void cancel() {
			Subscription subscription = this.subscription;
			if (subscription != null) {
				this.subscription = null;
				if (TERMINATED.compareAndSet(this, 0, 1)) {
					subscription.cancel();
				}
			}
		}

		@Override
		public void onSubscribe(Subscription s) {
			if (BackpressureUtils.checkSubscription(subscription, s)) {
				subscription = s;
				inputSubscriber.onSubscribe(this);
			}
		}

		@Override
		public void onNext(Buffer bytes) {
			if (RUNNING.get(this) == 0 && RUNNING.compareAndSet(this, 0, 1)) {
				long r = BackpressureUtils.getAndSub(REQUESTED, this, 1L);
				if(r != 0) {
					try {
						inputSubscriber.onNext(bytes);
					}
					catch (Throwable e) {
						Exceptions.throwIfFatal(e);
						cancel();
						onError(e);
						return;
					}
				}
				else{
					RingBuffer<RingBuffer.Slot<Buffer>> queue = getReadBackpressureBuffer();
					long n = queue.next();
					queue.get(n).value = bytes;
					queue.publish(n);
				}
				if(RUNNING.decrementAndGet(this) == 0){
					return;
				}
			}
			else {
				RingBuffer<RingBuffer.Slot<Buffer>> queue = getReadBackpressureBuffer();
				long n = queue.next();
				queue.get(n).value = bytes;
				queue.publish(n);
				if(RUNNING.getAndIncrement(this) == 0){
					return;
				}
			}

			drainBackpressureQueue();
		}

		@Override
		public void onError(Throwable t) {
			if (!TERMINATED.compareAndSet(this, 0, 1)) {
				throw ReactorFatalException.create(t);
			}
			error = t;
			drain();
		}

		@Override
		public void onComplete() {
			if (!TERMINATED.compareAndSet(this, 0, 1)) {
				drain();
				throw CancelException.get();
			}
			drain();
		}

		boolean shouldReadMore() {
			return requested > 0 ||
					(readBackpressureBuffer != null && readBackpressureBuffer.pending() == 0);
		}

		void drain(){
			if(RUNNING.getAndIncrement(this) == 0){
				drainBackpressureQueue();
			}
		}

		void drainBackpressureQueue() {
			int missed = 1;
			final Sequence pollCursor = this.pollCursor;
			final Subscriber<? super Buffer> child = this.inputSubscriber;
			for (; ; ) {
				long demand = requested;
				RingBuffer<RingBuffer.Slot<Buffer>> queue;
				if (demand != 0) {
					long remaining = demand;
					queue = readBackpressureBuffer;
					if (queue != null) {

						long polled = -1;
						RingBuffer.Slot<Buffer> holder;
						while (polled <= queue.getCursor() && (demand == Long.MAX_VALUE || remaining-- > 0)) {

							if(subscription == null){
								break;
							}

							polled = pollCursor.get() + 1L;
							holder = queue.get(polled);
							if (holder.value != null) {
								child.onNext(holder.value);
								holder.value = null;
								pollCursor.set(polled);
							}
						}
					}
					Subscription subscription = this.subscription;
					if (demand != Long.MAX_VALUE && remaining > 0 && subscription != null) {
						subscription.request(remaining);
					}
				}
				queue = readBackpressureBuffer;
				if((queue == null || queue.pending() == 0) && terminated == 1){
					if(error != null){
						inputSubscriber.onError(error);
					}
					else {
						inputSubscriber.onComplete();
					}
					return;
				}
				missed = RUNNING.addAndGet(this, -missed);
				if (missed == 0){
					break;
				}
			}

		}

		@SuppressWarnings("unchecked")
		RingBuffer<RingBuffer.Slot<Buffer>> getReadBackpressureBuffer() {
			RingBuffer<RingBuffer.Slot<Buffer>> q = readBackpressureBuffer;
			if (q == null) {
				q = RingBuffer.createSingleProducer(bufferSize);
				q.addGatingSequence(pollCursor = Sequencer.newSequence(-1L));
				readBackpressureBuffer = q;
			}
			return q;
		}

		@Override
		public String toString() {
			return "ChannelInputSubscriber{" +
					"terminated=" + terminated +
					", requested=" + requested +
					'}';
		}
	}

	private class FlushOnTerminateSubscriber extends BaseSubscriber<Object> implements Consumer<Void> {

		private final ChannelHandlerContext ctx;
		private final ChannelPromise        promise;
		ChannelFuture lastWrite;
		Subscription  subscription;

		public FlushOnTerminateSubscriber(ChannelHandlerContext ctx, ChannelPromise promise) {
			this.ctx = ctx;
			this.promise = promise;
		}

		@Override
		public void accept(Void aVoid) {
			subscription = null;
		}

		@Override
		public void onSubscribe(final Subscription s) {
			if (BackpressureUtils.checkSubscription(subscription, s)) {
				this.subscription = s;
				doOnSubscribe(ctx, s, Long.MAX_VALUE, this);
			}
		}

		@Override
		public void onNext(final Object w) {
			super.onNext(w);
			if (subscription == null) {
				throw CancelException.get();
			}
			try {
				ChannelFuture cf = doOnWrite(w, ctx);
				lastWrite = cf;
				if (cf != null && log.isDebugEnabled()) {
					cf.addListener(new ChannelFutureListener() {
						@Override
						public void operationComplete(ChannelFuture future) throws Exception {
							if (!future.isSuccess()) {
								log.error("write error :" + w, future.cause());
								if (Buffer.class.isAssignableFrom(w.getClass())) {
									((Buffer) w).rewind();
								}
							}
						}
					});
				}
			}
			catch (Throwable t) {
				onError(Exceptions.addValueAsLastCause(t, w));
			}
		}

		@Override
		public void onError(Throwable t) {
			super.onError(t);
			if (subscription == null) {
				throw new IllegalStateException("already flushed", t);
			}
			subscription = null;
			log.error("Write error", t);
			doOnTerminate(ctx, lastWrite, promise);
		}

		@Override
		public void onComplete() {
			if (subscription == null) {
				throw new IllegalStateException("already flushed");
			}
			subscription = null;
			doOnTerminate(ctx, lastWrite, promise);
		}
	}

	private class FlushOnCapacitySubscriber extends BaseSubscriber<Object> implements Runnable, Consumer<Void> {

		private final ChannelHandlerContext ctx;
		private final ChannelPromise        promise;
		private final long                  capacity;

		private Subscription subscription;
		private long written = 0L;

		private final ChannelFutureListener writeListener = new ChannelFutureListener() {

			@Override
			public void operationComplete(ChannelFuture future) throws Exception {
				if (!future.isSuccess() && future.cause() != null) {
					log.error("error during write");
					promise.tryFailure(future.cause());
					return;
				}
				if (capacity == 1L || --written == 0L) {
					if (subscription != null) {
						subscription.request(capacity);
					}
				}
			}
		};

		public FlushOnCapacitySubscriber(ChannelHandlerContext ctx, ChannelPromise promise, long capacity) {
			this.ctx = ctx;
			this.promise = promise;
			this.capacity = capacity;
		}

		@Override
		public void onSubscribe(final Subscription s) {
			if (BackpressureUtils.checkSubscription(subscription, s)) {
				subscription = s;
				doOnSubscribe(ctx, s, capacity, this);
			}
		}

		@Override
		public void onNext(Object w) {
			super.onNext(w);
			if (subscription == null) {
				throw CancelException.get();
			}
			try {
				ChannelFuture cf = doOnWrite(w, ctx);
				if (cf != null) {
					cf.addListener(writeListener);
				}
				if (capacity == 1L) {
					ctx.flush();
				}
				else {
					ctx.channel()
					   .eventLoop()
					   .execute(this);
				}
			}
			catch (Throwable t) {
				onError(Exceptions.addValueAsLastCause(t, w));
				throw CancelException.get();
			}
		}

		@Override
		public void onError(Throwable t) {
			super.onError(t);
			if (subscription == null) {
				throw new IllegalStateException("already flushed", t);
			}
			log.error("Write error", t);
			subscription = null;
			doOnTerminate(ctx, null, promise);
		}

		@Override
		public void onComplete() {
			if (subscription == null) {
				throw new IllegalStateException("already flushed");
			}
			subscription = null;
			if (log.isDebugEnabled()) {
				log.debug("Flush Connection");
			}
			doOnTerminate(ctx, null, promise);
		}

		@Override
		public void run() {
			if (++written == capacity) {
				ctx.flush();
			}
		}

		@Override
		public void accept(Void aVoid) {
			subscription = null;
		}

	}

	public ReactiveChannelHandler<Buffer, Buffer, ReactiveChannel<Buffer, Buffer>> getHandler() {
		return handler;
	}

	public NettyChannel getReactorNettyChannel() {
		return reactorNettyChannel;
	}

}
