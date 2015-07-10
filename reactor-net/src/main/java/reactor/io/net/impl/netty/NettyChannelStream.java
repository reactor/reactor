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

package reactor.io.net.impl.netty;

import io.netty.channel.*;
import io.netty.handler.timeout.IdleState;
import io.netty.handler.timeout.IdleStateEvent;
import io.netty.handler.timeout.IdleStateHandler;
import org.reactivestreams.Publisher;
import org.reactivestreams.Subscriber;
import reactor.Environment;
import reactor.ReactorProcessor;
import reactor.core.error.CancelException;
import reactor.fn.Consumer;
import reactor.io.buffer.Buffer;
import reactor.io.codec.Codec;
import reactor.io.net.ChannelStream;
import reactor.io.net.ReactorChannel;
import reactor.rx.Streams;
import reactor.rx.broadcast.Broadcaster;
import reactor.rx.subscription.PushSubscription;

import java.net.InetSocketAddress;
import java.util.concurrent.TimeUnit;

/**
 * {@link ReactorChannel} implementation that delegates to Netty.
 *
 * @author Jon Brisbin
 * @author Stephane Maldini
 */
public class NettyChannelStream<IN, OUT> extends ChannelStream<IN, OUT> {

	private final Channel ioChannel;

	public NettyChannelStream(Environment env,
	                          Codec<Buffer, IN, OUT> codec,
	                          long prefetch,
	                          ReactorProcessor eventsDispatcher,
	                          Channel ioChannel) {
		super(env, codec, prefetch, eventsDispatcher);
		this.ioChannel = ioChannel;
	}

	@Override
	public void subscribe(Subscriber<? super IN> subscriber) {
		ioChannel.pipeline()
				.fireUserEventTriggered(new NettyChannelHandlerBridge.ChannelInputSubscriberEvent<>(subscriber));
	}

	@Override
	public void doSubscribeWriter(Publisher<? extends OUT> writer, final Subscriber<? super Void> postWriter) {

		final Publisher<?> encodedWriter;
		if (getEncoder() != null) {
			encodedWriter = Streams.wrap(writer).map(getEncoder());
		} else {
			encodedWriter = writer;
		}



		if(ioChannel.eventLoop().inEventLoop()) {
			ioChannel.write(encodedWriter).addListener(new ChannelFutureListener() {
				@Override
				public void operationComplete(ChannelFuture future) throws Exception {
					if (future.isSuccess()) {
						postWriter.onSubscribe(Broadcaster.HOT_SUBSCRIPTION);
						postWriter.onComplete();
					} else {
						postWriter.onError(future.cause());
					}
				}
			});
		}else {
			ioChannel.eventLoop().execute(new Runnable() {
				@Override
				public void run() {
					ioChannel.write(encodedWriter).addListener(new ChannelFutureListener() {
						@Override
						public void operationComplete(ChannelFuture future) throws Exception {
							if (future.isSuccess()) {
								postWriter.onSubscribe(Broadcaster.HOT_SUBSCRIPTION);
								postWriter.onComplete();
							} else {
								postWriter.onError(future.cause());
							}
						}
					});
				}
			});
		}

	}

	@Override
	public InetSocketAddress remoteAddress() {
		return (InetSocketAddress) ioChannel.remoteAddress();
	}

	@Override
	public ConsumerSpec on() {
		return new NettyConsumerSpec();
	}

	@Override
	public Channel delegate() {
		return ioChannel;
	}

	@Override
	@SuppressWarnings("unchecked")
	public void doDecoded(IN in) {
		NettyChannelHandlerBridge ch = ioChannel.pipeline().get(NettyChannelHandlerBridge.class);
		PushSubscription<IN> subscription = ch == null ? null : ch.subscription();
		if (subscription != null) {
			try {
				subscription.onNext(in);
			} catch (CancelException ce){

			}
		}
	}

	@Override
	public String toString() {
		return this.getClass().getName() + " {" +
				"channel=" + ioChannel +
				'}';
	}

	private class NettyConsumerSpec implements ConsumerSpec {
		@Override
		public ConsumerSpec close(final Consumer<Void> onClose) {
			ioChannel.pipeline().addLast(new ChannelDuplexHandler() {
				@Override
				public void channelInactive(ChannelHandlerContext ctx) throws Exception {
					onClose.accept(null);
					super.channelInactive(ctx);
				}
			});
			return this;
		}

		@Override
		public ConsumerSpec readIdle(long idleTimeout, final Consumer<Void> onReadIdle) {
			ioChannel.pipeline().addFirst(new IdleStateHandler(idleTimeout, 0, 0, TimeUnit.MILLISECONDS) {
				@Override
				protected void channelIdle(ChannelHandlerContext ctx, IdleStateEvent evt) throws Exception {
					if (evt.state() == IdleState.READER_IDLE) {
						onReadIdle.accept(null);
					}
					super.channelIdle(ctx, evt);
				}
			});
			return this;
		}

		@Override
		public ConsumerSpec writeIdle(long idleTimeout, final Consumer<Void> onWriteIdle) {
			ioChannel.pipeline().addLast(new IdleStateHandler(0, idleTimeout, 0, TimeUnit.MILLISECONDS) {
				@Override
				protected void channelIdle(ChannelHandlerContext ctx, IdleStateEvent evt) throws Exception {
					if (evt.state() == IdleState.WRITER_IDLE) {
						onWriteIdle.accept(null);
					}
					super.channelIdle(ctx, evt);
				}
			});
			return this;
		}
	}

}
