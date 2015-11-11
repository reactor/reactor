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

package reactor.io.net.impl.netty.http;

import io.netty.buffer.ByteBufHolder;
import io.netty.channel.ChannelHandlerContext;
import io.netty.handler.codec.http.DefaultHttpRequest;
import io.netty.handler.codec.http.FullHttpResponse;
import io.netty.handler.codec.http.HttpContent;
import io.netty.handler.codec.http.HttpMethod;
import io.netty.handler.codec.http.HttpResponse;
import io.netty.handler.codec.http.HttpResponseStatus;
import io.netty.handler.codec.http.HttpVersion;
import io.netty.handler.codec.http.LastHttpContent;
import org.reactivestreams.Subscriber;
import org.reactivestreams.Subscription;
import reactor.Publishers;
import reactor.core.subscriber.BaseSubscriber;
import reactor.core.support.BackpressureUtils;
import reactor.io.buffer.Buffer;
import reactor.io.net.ReactiveChannel;
import reactor.io.net.ReactiveChannelHandler;
import reactor.io.net.http.HttpChannel;
import reactor.io.net.http.HttpException;
import reactor.io.net.impl.netty.NettyChannel;
import reactor.io.net.impl.netty.tcp.NettyChannelHandlerBridge;

/**
 * @author Stephane Maldini
 */
public class NettyHttpClientHandler extends NettyChannelHandlerBridge {

	protected final NettyChannel                    tcpStream;
	protected       NettyHttpChannel                httpChannel;
	protected       Subscriber<? super HttpChannel<Buffer, Buffer>> replySubscriber;

	/**
	 * The body of an HTTP response should be discarded.
	 */
	private boolean discardBody = false;

	public NettyHttpClientHandler(
			ReactiveChannelHandler<Buffer, Buffer, ReactiveChannel<Buffer, Buffer>> handler,
			NettyChannel tcpStream) {
		super(handler, tcpStream);
		this.tcpStream = tcpStream;
	}

	@Override
	public void channelActive(final ChannelHandlerContext ctx) throws Exception {
		ctx.fireChannelActive();

		if(httpChannel == null) {
			httpChannel = new NettyHttpChannel(tcpStream, new DefaultHttpRequest(HttpVersion.HTTP_1_1, HttpMethod.GET, "/")) {
				@Override
				protected void doSubscribeHeaders(Subscriber<? super Void> s) {
					tcpStream.emitWriter(Publishers.just(getNettyRequest()), s);
				}
			};
			httpChannel.keepAlive(true);
			httpChannel.headers().transferEncodingChunked();
		}


		handler.apply(httpChannel)
		       .subscribe(new BaseSubscriber<Void>() {
			       @Override
			       public void onSubscribe(final Subscription s) {
				       ctx.read();
				       BackpressureUtils.checkSubscription(null, s);
				       s.request(Long.MAX_VALUE);
			       }

			       @Override
			       public void onError(Throwable t) {
				       super.onError(t);
				       log.error("Error processing connection. Closing the channel.", t);
				       if (ctx.channel()
				              .isOpen()) {
					       ctx.channel()
					          .close();
				       }
			       }

			       @Override
			       public void onComplete() {
				       writeLast(ctx);
			       }
		       });
	}

	@Override
	public void channelRead(ChannelHandlerContext ctx, Object msg) throws Exception {
		Class<?> messageClass = msg.getClass();
		if (HttpResponse.class.isAssignableFrom(messageClass)) {
			HttpResponse response = (HttpResponse) msg;
			if (httpChannel != null) {
				httpChannel.setNettyResponse(response);
			}

			checkResponseCode(ctx, response);

			ctx.fireChannelRead(msg);
			if (FullHttpResponse.class.isAssignableFrom(messageClass)) {
				postRead(ctx, msg);
			}
			if(!discardBody && replySubscriber != null){
				Publishers.just(httpChannel).subscribe(replySubscriber);
			}
		} else if (HttpContent.class.isAssignableFrom(messageClass)) {
			super.channelRead(ctx, ((ByteBufHolder) msg).content());
			postRead(ctx, msg);
		} else if(!discardBody){
			super.channelRead(ctx, msg);
		}
	}

	private void checkResponseCode(ChannelHandlerContext ctx, HttpResponse response) throws Exception {
		boolean discardBody = false;

		int code = response.getStatus().code();
		if (code == HttpResponseStatus.NOT_FOUND.code()
				|| code == HttpResponseStatus.BAD_REQUEST.code()
				|| code == HttpResponseStatus.INTERNAL_SERVER_ERROR.code()) {
			Exception ex = new HttpException(httpChannel);
			exceptionCaught(ctx, ex);
			if(replySubscriber != null){
				Publishers.<HttpChannel<Buffer, Buffer>>error(ex).subscribe(replySubscriber);
			}
			discardBody = true;
		}

		setDiscardBody(discardBody);
	}

	protected void postRead(ChannelHandlerContext ctx, Object msg){
		if (LastHttpContent.class.isAssignableFrom(msg.getClass())) {
			ctx.channel().close();
		}
	}

	@Override
	@SuppressWarnings("unchecked")
	public void userEventTriggered(ChannelHandlerContext ctx, Object evt)
			throws Exception {

		if (evt != null && evt.getClass().equals(ChannelInputSubscriberEvent.class)) {
			replySubscriber = ((ChannelInputSubscriberEvent)evt).clientReplySubscriber;
		}
		else {
			super.userEventTriggered(ctx, evt);
		}

	}

	protected void writeLast(final ChannelHandlerContext ctx){
		ctx.channel().writeAndFlush(LastHttpContent.EMPTY_LAST_CONTENT);
	}

	private void setDiscardBody(boolean discardBody) {
		this.discardBody = discardBody;
	}

	/**
	 * An event to attach a {@link Subscriber} to the {@link NettyChannel}
	 * created by {@link NettyHttpClient}
	 */
	public static final class ChannelInputSubscriberEvent {

		private final Subscriber<? super HttpChannel<Buffer,Buffer>> clientReplySubscriber;

		public ChannelInputSubscriberEvent(Subscriber<? super HttpChannel<Buffer, Buffer>> inputSubscriber) {
			if (null == inputSubscriber) {
				throw new IllegalArgumentException("HTTP input subscriber must not be null.");
			}
			this.clientReplySubscriber = inputSubscriber;
		}
	}
}
