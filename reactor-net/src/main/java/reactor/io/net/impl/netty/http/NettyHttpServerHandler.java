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

package reactor.io.net.impl.netty.http;

import io.netty.buffer.ByteBufHolder;
import io.netty.channel.ChannelFuture;
import io.netty.channel.ChannelFutureListener;
import io.netty.channel.ChannelHandlerContext;
import io.netty.handler.codec.http.DefaultHttpContent;
import io.netty.handler.codec.http.DefaultLastHttpContent;
import io.netty.handler.codec.http.HttpContent;
import io.netty.handler.codec.http.LastHttpContent;
import org.reactivestreams.Publisher;
import org.reactivestreams.Subscriber;
import org.reactivestreams.Subscription;
import reactor.fn.Consumer;
import reactor.io.buffer.Buffer;
import reactor.io.net.ChannelStream;
import reactor.io.net.ReactorChannelHandler;
import reactor.io.net.impl.netty.NettyChannelHandlerBridge;
import reactor.io.net.impl.netty.NettyChannelStream;
import reactor.rx.action.support.DefaultSubscriber;

/**
 * Conversion between Netty types  and Reactor types ({@link NettyHttpChannel} and {@link reactor.io.buffer.Buffer}).
 *
 * @author Stephane Maldini
 */
public class NettyHttpServerHandler<IN, OUT> extends NettyChannelHandlerBridge<IN, OUT> {

	private final NettyChannelStream<IN, OUT> tcpStream;
	protected     NettyHttpChannel<IN, OUT>   request;

	public NettyHttpServerHandler(
			ReactorChannelHandler<IN, OUT, ChannelStream<IN, OUT>> handler,
			NettyChannelStream<IN, OUT> tcpStream) {
		super(handler, tcpStream);
		this.tcpStream = tcpStream;
	}

	@Override
	public void channelActive(ChannelHandlerContext ctx) throws Exception {
		ctx.fireChannelActive();
		ctx.read();
	}

	@Override
	public void channelRead(final ChannelHandlerContext ctx, final Object msg) throws Exception {
		Class<?> messageClass = msg.getClass();
		if (request == null && io.netty.handler.codec.http.HttpRequest.class.isAssignableFrom(messageClass)) {
			request = new NettyHttpChannel<>(tcpStream, (io.netty.handler.codec.http.HttpRequest) msg);

			final Publisher<Void> closePublisher = handler.apply(request);
			final Subscriber<Void> closeSub = new DefaultSubscriber<Void>() {

				Subscription subscription;

				@Override
				public void onSubscribe(Subscription s) {
					subscription = s;
					s.request(Long.MAX_VALUE);
				}

				@Override
				public void onError(Throwable t) {
					log.error("Error processing connection. Closing the channel.", t);
					if (CHANNEL_REF.get(NettyHttpServerHandler.this) == 0) {
						ctx.channel().close();
					}
				}

				@Override
				public void onComplete() {
					subscription.cancel();
					if (ctx.channel().isOpen()) {
						if (log.isDebugEnabled()) {
							log.debug("Close Http Response ");
						}
						writeLast(ctx);
						//ctx.channel().close();
						/*
						ctx.channel().writeAndFlush(LastHttpContent.EMPTY_LAST_CONTENT).addListener(new ChannelFutureListener() {
							@Override
							public void operationComplete(ChannelFuture future) throws Exception {
								if(ctx.channel().isOpen()){
									ctx.channel().close();
								}
							}
						});
						 */
					}
				}
			};

			closePublisher.subscribe(closeSub);

		}
		if (HttpContent.class.isAssignableFrom(messageClass)) {
			super.channelRead(ctx, ((ByteBufHolder) msg).content());
		}
		postRead(ctx, msg);
	}

	protected void postRead(ChannelHandlerContext ctx, Object msg){
		if (channelSubscription != null && DefaultLastHttpContent.class.equals(msg.getClass())) {
			channelSubscription.onComplete();
			channelSubscription = null;
		}
	}
	protected void writeLast(ChannelHandlerContext ctx){
		ctx
		  .writeAndFlush(request.checkHeader() ? request.getNettyResponse() : LastHttpContent.EMPTY_LAST_CONTENT)
		  .addListener(ChannelFutureListener.CLOSE);
	}

	@Override
	protected ChannelFuture doOnWrite(final Object data, final ChannelHandlerContext ctx) {
		if (data.getClass().equals(Buffer.class)) {
			return ctx.write(new DefaultHttpContent(convertBufferToByteBuff(ctx, (Buffer) data)));
		} else {
			return ctx.write(data);
		}
	}

	@Override
	protected void doOnSubscribe(ChannelHandlerContext ctx, Subscription s, long n, Consumer<Void> cb) {
		if (request.checkHeader()) {
			ctx.write(request.getNettyResponse());
		}

		super.doOnSubscribe(ctx, s, n, cb);
	}

	NettyHttpServerHandler<IN, OUT> withWebsocketSupport(String url, String protocols){
		//prevent further header to be sent for handshaking
		if(!request.checkHeader()){
			log.error("Cannot enable websocket if headers have already been sent");
			return this;
		}
		return new NettyHttpWSServerHandler<>(url, protocols, this);
	}
}
