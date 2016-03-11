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
 *  WITHIN WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  See the License for the specific language governing permissions and
 *  limitations under the License.
 */

package reactor.io.netty.impl.netty;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;
import io.netty.channel.*;
import io.netty.util.ReferenceCountUtil;
import org.reactivestreams.Publisher;
import org.reactivestreams.Subscriber;
import org.reactivestreams.Subscription;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import reactor.Environment;
import reactor.core.processor.CancelException;
import reactor.core.support.Exceptions;
import reactor.core.support.NonBlocking;
import reactor.fn.Consumer;
import reactor.io.buffer.Buffer;
import reactor.io.netty.ChannelStream;
import reactor.io.netty.ReactorChannelHandler;
import reactor.io.netty.Spec;
import reactor.rx.action.support.DefaultSubscriber;
import reactor.rx.subscription.PushSubscription;

import java.util.concurrent.atomic.AtomicIntegerFieldUpdater;

/**
 * Netty {@link io.netty.channel.ChannelInboundHandler} implementation that passes data to a Reactor {@link
 * reactor.io.netty.ChannelStream}.
 *
 * @author Jon Brisbin
 * @author Stephane Maldini
 */
public class NettyChannelHandlerBridge<IN, OUT> extends ChannelDuplexHandler {

	protected static final Logger log = LoggerFactory.getLogger(NettyChannelHandlerBridge.class);

	protected final ReactorChannelHandler<IN, OUT, ChannelStream<IN, OUT>> handler;
	protected final NettyChannelStream<IN, OUT>                            channelStream;

	protected PushSubscription<IN> channelSubscription;
	private   ByteBuf              remainder;

	private volatile int channelRef = 0;
	protected static final AtomicIntegerFieldUpdater<NettyChannelHandlerBridge> CHANNEL_REF =
			AtomicIntegerFieldUpdater.newUpdater(NettyChannelHandlerBridge.class, "channelRef");

	public NettyChannelHandlerBridge(
			ReactorChannelHandler<IN, OUT, ChannelStream<IN, OUT>> handler, NettyChannelStream<IN, OUT> channelStream
	) {
		this.handler = handler;
		this.channelStream = channelStream;
	}

	public PushSubscription<IN> subscription() {
		return channelSubscription;
	}

	@Override
	public void userEventTriggered(final ChannelHandlerContext ctx, Object evt) throws Exception {
		if (evt != null && evt.getClass().equals(ChannelInputSubscriberEvent.class)) {

			if (null == channelSubscription) {

				@SuppressWarnings("unchecked")
				ChannelInputSubscriberEvent<IN> subscriberEvent = (ChannelInputSubscriberEvent<IN>) evt;

				CHANNEL_REF.incrementAndGet(NettyChannelHandlerBridge.this);

				this.channelSubscription = new PushSubscription<IN>(null, subscriberEvent.inputSubscriber) {
					@Override
					protected void onRequest(long n) {
						if (n == Long.MAX_VALUE) {
							ctx.channel().config().setAutoRead(true);
						}
						ctx.read();
					}

					@Override
					public void cancel() {
						super.cancel();
						channelSubscription = null;
						//log.debug("Cancel read");
						ctx.channel().config().setAutoRead(false);
						CHANNEL_REF.decrementAndGet(NettyChannelHandlerBridge.this);
					}
				};
				subscriberEvent.inputSubscriber.onSubscribe(channelSubscription);

			} else {
				channelSubscription.onError(new IllegalStateException("Only one connection input subscriber allowed."));
			}
		}
		super.userEventTriggered(ctx, evt);
	}

	@Override
	public void channelActive(final ChannelHandlerContext ctx) throws Exception {
		super.channelActive(ctx);
		handler.apply(channelStream)
		       .subscribe(new DefaultSubscriber<Void>() {
			       @Override
			       public void onSubscribe(Subscription s) {
				       s.request(Long.MAX_VALUE);
			       }

			       @Override
			       public void onError(Throwable t) {
				       log.error("Error processing connection. Closing the channel.", t);

				       ctx.writeAndFlush(Unpooled.EMPTY_BUFFER).addListener(ChannelFutureListener.CLOSE);
			       }

			       @Override
			       public void onComplete() {
				       ctx.writeAndFlush(Unpooled.EMPTY_BUFFER).addListener(ChannelFutureListener.CLOSE);
			       }
		       });
	}

	@Override
	public void channelReadComplete(ChannelHandlerContext ctx) throws Exception {
		if (channelSubscription == null) {
			return;
		}

		try {
			super.channelReadComplete(ctx);
			if (channelSubscription.pendingRequestSignals() != Long.MAX_VALUE){
				channelSubscription.updatePendingRequests(-1);
				if(channelSubscription.pendingRequestSignals() > 0l) {
					ctx.read();
				}
			}
		} catch (Throwable throwable) {
			if (channelSubscription != null) {
				channelSubscription.onError(throwable);
			} else if (Environment.alive()) {
				Environment.get().routeError(throwable);
			}
		}
	}

	@Override
	public void channelInactive(ChannelHandlerContext ctx) throws Exception {
		try {
			if (this.channelSubscription != null) {
				channelSubscription.onComplete();
				channelSubscription = null;
			}
			super.channelInactive(ctx);
		} catch (Throwable err) {
			if (channelSubscription != null) {
				channelSubscription.onError(err);
			} else if (Environment.alive()) {
				Environment.get().routeError(err);
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
			if (null == channelSubscription || msg == Unpooled.EMPTY_BUFFER) {
				ReferenceCountUtil.release(msg);
				return;
			}

			if (channelStream.getDecoder() == Spec.NOOP_DECODER || !ByteBuf.class.isAssignableFrom(msg.getClass())) {
				channelSubscription.onNext((IN) msg);
				return;
			} else if (channelStream.getDecoder() == null) {
				try {
					channelSubscription.onNext((IN) new Buffer(((ByteBuf) msg).nioBuffer()));
				} finally {
					ReferenceCountUtil.release(msg);
				}
				return;
			}

			ByteBuf data = (ByteBuf) msg;
			if (remainder == null) {
				try {
					passToConnection(data);
				} finally {
					if (data.isReadable()) {
						remainder = data;
					} else {
						data.release();
					}
				}
				return;
			}

			if (!bufferHasSufficientCapacity(remainder, data)) {
				ByteBuf combined = createCombinedBuffer(remainder, data, ctx);
				remainder.release();
				remainder = combined;
			} else {
				remainder.writeBytes(data);
			}
			data.release();

			try {
				passToConnection(remainder);
			} finally {
				if (remainder.isReadable()) {
					remainder.discardSomeReadBytes();
				} else {
					remainder.release();
					remainder = null;
				}
			}
		} catch (Throwable t) {
			if (channelSubscription != null) {
				channelSubscription.onError(t);
			} else if (Environment.alive()) {
				Environment.get().routeError(t);
			}
		}

	}

	@Override
	public void write(final ChannelHandlerContext ctx, Object msg, final ChannelPromise promise) throws Exception {
		if (msg instanceof Publisher) {
			CHANNEL_REF.incrementAndGet(this);

			@SuppressWarnings("unchecked")
			Publisher<?> data = (Publisher<?>) msg;
			final long capacity = msg instanceof NonBlocking ? ((NonBlocking) data).getCapacity() : Long.MAX_VALUE;

			if (capacity == Long.MAX_VALUE) {
				data.subscribe(new FlushOnTerminateSubscriber(ctx, promise));
			} else {
				data.subscribe(new FlushOnCapacitySubscriber(ctx, promise, capacity));
			}
		} else {
			super.write(ctx, msg, promise);
		}
	}


	@Override
	public void exceptionCaught(ChannelHandlerContext ctx, Throwable cause) throws Exception {
		if (channelSubscription != null) {
			channelSubscription.onError(cause);
		} else if (Environment.alive()) {
			Environment.get().routeError(cause);
		} else {
			log.error("Unexpected issue", cause);
		}
	}

	protected ChannelFuture doOnWrite(Object data, ChannelHandlerContext ctx) {
		if (data.getClass().equals(Buffer.class)) {
			return ctx.channel().write(convertBufferToByteBuff(ctx, (Buffer) data));
		} else if (Unpooled.EMPTY_BUFFER != data) {
			return ctx.channel().write(data);
		}
		return null;
	}

	protected static ByteBuf convertBufferToByteBuff(ChannelHandlerContext ctx, Buffer data) {
		ByteBuf buff = ctx.alloc().buffer(data.remaining());
		return buff.writeBytes(data.byteBuffer());
	}

	protected void doOnTerminate(ChannelHandlerContext ctx, ChannelFuture last, final ChannelPromise promise) {
		CHANNEL_REF.decrementAndGet(this);

		if (ctx.channel().isOpen()) {
			ChannelFutureListener listener = new ChannelFutureListener() {
				@Override
				public void operationComplete(ChannelFuture future) throws Exception {
					if (future.isSuccess()) {
						promise.trySuccess();
					} else {
						promise.tryFailure(future.cause());
					}
				}
			};

			if (last != null) {
				ctx.flush();
				last.addListener(listener);
			} else {
				ctx.writeAndFlush(Unpooled.EMPTY_BUFFER).addListener(listener);
			}
		} else {
			promise.trySuccess();
		}
	}

	private static boolean bufferHasSufficientCapacity(ByteBuf receiver, ByteBuf provider) {
		return receiver.writerIndex() <= receiver.maxCapacity() - provider.readableBytes();
	}

	private static ByteBuf createCombinedBuffer(ByteBuf partOne, ByteBuf partTwo, ChannelHandlerContext ctx) {
		ByteBuf combined = ctx.alloc().buffer(partOne.readableBytes() + partTwo.readableBytes());
		combined.writeBytes(partOne);
		combined.writeBytes(partTwo);
		return combined;
	}

	private void passToConnection(ByteBuf data) {
		Buffer b = new Buffer(data.nioBuffer());
		int start = b.position();
		if (null != channelStream.getDecoder() && null != b.byteBuffer()) {
			IN read = channelStream.getDecoder().apply(b);
			if (read != null) {
				channelSubscription.onNext(read);
			}
		}

		//data.remaining() > 0;
		data.skipBytes(b.position() - start);
	}

	protected void doOnSubscribe(ChannelHandlerContext ctx, final Subscription s) {
	}

	/**
	 * An event to attach a {@link Subscriber} to the {@link NettyChannelStream}
	 * created by {@link NettyChannelHandlerBridge}
	 *
	 * @param <IN>
	 */
	public static final class ChannelInputSubscriberEvent<IN> {
		private final Subscriber<IN> inputSubscriber;

		public ChannelInputSubscriberEvent(Subscriber<IN> inputSubscriber) {
			if (null == inputSubscriber) {
				throw new IllegalArgumentException("Connection input subscriber must not be null.");
			}
			this.inputSubscriber = inputSubscriber;
		}
	}

	private class FlushOnTerminateSubscriber extends DefaultSubscriber<Object> implements ChannelFutureListener  {

		private final ChannelHandlerContext ctx;
		private final ChannelPromise        promise;
		ChannelFuture lastWrite;
		Subscription  subscription;

		public FlushOnTerminateSubscriber(ChannelHandlerContext ctx, ChannelPromise promise) {
			this.ctx = ctx;
			this.promise = promise;
		}

		public void operationComplete(ChannelFuture future) throws Exception {
			if (log.isDebugEnabled()) {
				log.debug("Cancel connection");
			}
			if(subscription != null) {
				subscription.cancel();
			}
			subscription = null;
		}

		@Override
		public void onSubscribe(final Subscription s) {
			this.subscription = s;

			ctx.channel()
			   .closeFuture()
			   .addListener(this);

			doOnSubscribe(ctx, s);

			s.request(Long.MAX_VALUE);
		}

		@Override
		public void onNext(final Object w) {
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
							}
						}
					});
				}
			} catch (Throwable t) {
				onError(Exceptions.addValueAsLastCause(t, w));
			}
		}

		@Override
		public void onError(Throwable t) {
			if (subscription == null) throw new IllegalStateException("already flushed", t);
			log.error("Write error", t);
			subscription = null;
			ctx.channel()
			   .closeFuture()
			   .removeListener(this);

			doOnTerminate(ctx, lastWrite, promise);
		}

		@Override
		public void onComplete() {
			if (subscription == null) throw new IllegalStateException("already flushed");
			subscription = null;
			ctx.channel()
			   .closeFuture()
			   .removeListener(this);

			doOnTerminate(ctx, lastWrite, promise);
		}
	}

	private class FlushOnCapacitySubscriber extends DefaultSubscriber<Object>
			implements Runnable, ChannelFutureListener  {

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
			subscription = s;
			ctx.channel()
			   .closeFuture()
			   .addListener(this);

			doOnSubscribe(ctx, s);

			s.request(capacity);
		}

		@Override
		public void onNext(Object w) {
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
				} else {
					ctx.channel().eventLoop().execute(this);
				}
			} catch (Throwable t) {
				onError(Exceptions.addValueAsLastCause(t, w));
			}
		}

		@Override
		public void onError(Throwable t) {
			if (subscription == null) throw new IllegalStateException("already flushed", t);
			log.error("Write error", t);
			subscription = null;
			ctx.channel()
			   .closeFuture()
			   .removeListener(this);

			doOnTerminate(ctx, null, promise);
		}

		@Override
		public void onComplete() {
			if (subscription == null) throw new IllegalStateException("already flushed");
			subscription = null;
			if (log.isDebugEnabled()) {
				log.debug("Flush Connection");
			}
			ctx.channel()
			   .closeFuture()
			   .removeListener(this);

			doOnTerminate(ctx, null, promise);
		}

		@Override
		public void run() {
			if (++written == capacity) {
				ctx.flush();
			}
		}


		public void operationComplete(ChannelFuture future) throws Exception {
			if (log.isDebugEnabled()) {
				log.debug("Cancel connection");
			}
			if(subscription != null) {
				subscription.cancel();
			}
			subscription = null;
		}

	}

	public ReactorChannelHandler<IN, OUT, ChannelStream<IN, OUT>> getHandler() {
		return handler;
	}

	public NettyChannelStream<IN, OUT> getChannelStream() {
		return channelStream;
	}

}