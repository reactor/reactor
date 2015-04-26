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

package reactor.io.net.impl.netty;

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
import reactor.io.buffer.Buffer;
import reactor.io.net.ChannelStream;
import reactor.io.net.ReactorChannelHandler;
import reactor.io.net.Spec;
import reactor.rx.action.support.DefaultSubscriber;
import reactor.rx.subscription.PushSubscription;

/**
 * Netty {@link io.netty.channel.ChannelInboundHandler} implementation that passes data to a Reactor {@link
 * reactor.io.net.ChannelStream}.
 *
 * @author Jon Brisbin
 * @author Stephane Maldini
 */
public class NettyChannelHandlerBridge<IN, OUT> extends ChannelDuplexHandler {

	protected static final Logger log = LoggerFactory.getLogger(NettyChannelHandlerBridge.class);

	protected final ReactorChannelHandler<IN, OUT, ChannelStream<IN, OUT>> handler;
	private final   NettyChannelStream<IN, OUT>                            channelStream;

	protected PushSubscription<IN> channelSubscription;
	private   ByteBuf              remainder;

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

				this.channelSubscription = new PushSubscription<IN>(null, subscriberEvent.inputSubscriber) {
					@Override
					protected void onRequest(long n) {
						if (n == Long.MAX_VALUE) {
							ctx.channel().config().setAutoRead(true);
						}
						ctx.read();
					}

				/*	@Override
					public void cancel() {
						super.cancel();
						if (ctx.channel().isOpen()) {
							ctx.close();
						}
					}*/
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
						if (channelSubscription == null) {
							ctx.channel().close();
						}
					}

					@Override
					public void onComplete() {
						if(channelSubscription == null) {
							ctx.channel().close();
						}
					}
				});
	}

	@Override
	public void channelReadComplete(ChannelHandlerContext ctx) throws Exception {
		if (this.channelSubscription == null) {
			return;
		}

		try {
			super.channelReadComplete(ctx);
			if (channelSubscription.pendingRequestSignals() != Long.MAX_VALUE
					&& channelSubscription.pendingRequestSignals() > 1l) {
				ctx.read();
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
	@SuppressWarnings("unchecked")
	public void channelRead(ChannelHandlerContext ctx, Object msg) throws Exception {
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
					((ByteBuf) msg).release();
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
		} catch (CancelException ce) {
			ReferenceCountUtil.release(msg);
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
			@SuppressWarnings("unchecked")
			final Publisher<?> data = (Publisher<?>) msg;

			data.subscribe(new DefaultSubscriber<Object>() {

				ChannelFuture lastWrite;

				@Override
				public void onSubscribe(final Subscription s) {
					s.request(Long.MAX_VALUE); // TODO: Netty Backpressure (currently logical BackPressure at ChannelStream
					// level)
					ctx.channel().closeFuture().addListener(new ChannelFutureListener() {
						@Override
						public void operationComplete(ChannelFuture future) throws Exception {
							s.cancel();
						}
					});
				}

				@Override
				public void onNext(Object w) {
					try {
						lastWrite = doOnWrite(w, ctx);
					} catch (Throwable t) {
						onError(Exceptions.addValueAsLastCause(t, w));
					}
				}

				@Override
				public void onError(Throwable t) {
					log.error("Write error", t);
					doOnTerminate(ctx, lastWrite, promise);
				}

				@Override
				public void onComplete() {
					doOnTerminate(ctx, lastWrite, promise);
				}

			});
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
		} else{
			log.error("Unexpected issue", cause);
		}
	}

	protected ChannelFuture doOnWrite(Object data, ChannelHandlerContext ctx){
		if (data.getClass().equals(Buffer.class)) {
			return ctx.channel().write(Unpooled.wrappedBuffer(((Buffer)data).byteBuffer()));
		}else if(Unpooled.EMPTY_BUFFER != data){
			return ctx.channel().write(data);
		}
		return null;
	}

	protected void doOnTerminate(ChannelHandlerContext ctx, ChannelFuture last, final ChannelPromise promise){
		if(ctx.channel().isOpen()) {
			ctx.flush();
			if (last != null) {
				last.addListener(new ChannelFutureListener() {
					@Override
					public void operationComplete(ChannelFuture future) throws Exception {
						if (future.isSuccess()) {
							promise.trySuccess();
						} else {
							promise.tryFailure(future.cause());
						}
					}
				});
			}
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

}
