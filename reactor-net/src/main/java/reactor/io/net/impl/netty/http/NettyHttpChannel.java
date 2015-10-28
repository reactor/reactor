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

import io.netty.buffer.Unpooled;
import io.netty.channel.ChannelFuture;
import io.netty.channel.ChannelFutureListener;
import io.netty.channel.socket.SocketChannel;
import io.netty.handler.codec.http.*;
import org.reactivestreams.Publisher;
import org.reactivestreams.Subscriber;
import reactor.core.support.Assert;
import reactor.io.buffer.Buffer;
import reactor.io.net.http.BaseHttpChannel;
import reactor.io.net.http.HttpChannel;
import reactor.io.net.http.model.*;
import reactor.io.net.http.model.HttpHeaders;
import reactor.io.net.impl.netty.NettyChannel;

import java.net.InetSocketAddress;

import static io.netty.handler.codec.http.HttpHeaders.is100ContinueExpected;

/**
 * @author Sebastien Deleuze
 * @author Stephane Maldini
 */
public abstract class NettyHttpChannel extends BaseHttpChannel<Buffer, Buffer>
		implements Publisher<Buffer> {

	private static final FullHttpResponse CONTINUE =
			new DefaultFullHttpResponse(HttpVersion.HTTP_1_1, HttpResponseStatus.CONTINUE, Unpooled.EMPTY_BUFFER);

	private final NettyChannel             tcpStream;
	private final HttpRequest              nettyRequest;
	private final NettyHttpHeaders         headers;
	private       HttpResponse             nettyResponse;
	private       NettyHttpResponseHeaders responseHeaders;

	public NettyHttpChannel(NettyChannel tcpStream,
	                        HttpRequest request
	) {
		super(tcpStream.getCapacity());
		this.tcpStream = tcpStream;
		this.nettyRequest = request;
		this.nettyResponse = new DefaultHttpResponse(request.getProtocolVersion(), HttpResponseStatus.OK);
		this.headers = new NettyHttpHeaders(request);
		this.responseHeaders = new NettyHttpResponseHeaders(this.nettyResponse);

		responseHeader(ResponseHeaders.TRANSFER_ENCODING, "chunked");
	}

	@Override
	public Publisher<Void> writeBufferWith(Publisher<? extends Buffer> dataStream) {
		return tcpStream.writeWith(dataStream);
	}

	@Override
	public Publisher<Buffer> input() {
		return this;
	}

	@Override
	public void subscribe(final Subscriber<? super Buffer> subscriber) {
		// Handle the 'Expect: 100-continue' header if necessary.
		// TODO: Respond with 413 Request Entity Too Large
		//   and discard the traffic or close the connection.
		//       No need to notify the upstream handlers - just log.
		//       If decoding a response, just throw an error.
		if (is100ContinueExpected(nettyRequest)) {
			tcpStream.delegate().writeAndFlush(CONTINUE).addListener(new ChannelFutureListener() {
				@Override
				public void operationComplete(ChannelFuture future) throws Exception {
					if (!future.isSuccess()) {
						subscriber.onError(future.cause());
					} else {
						tcpStream.subscribe(subscriber);
					}
				}
			});
		} else {
			tcpStream.subscribe(subscriber);
		}
	}

	// REQUEST contract

	@Override
	public Protocol protocol() {
		HttpVersion version = this.nettyRequest.getProtocolVersion();
		if (version.equals(HttpVersion.HTTP_1_0)) {
			return Protocol.HTTP_1_0;
		} else if (version.equals(HttpVersion.HTTP_1_1)) {
			return Protocol.HTTP_1_1;
		}
		throw new IllegalStateException(version.protocolName() + " not supported");
	}


	@Override
	protected Publisher<Void> writeWithAfterHeaders(Publisher<? extends Buffer> writer) {
		return tcpStream.writeWith(writer);
	}

	@Override
	public boolean markHeadersAsFlushed() {
		return super.markHeadersAsFlushed();
	}

	@Override
	protected void doHeader(String name, String value) {
		this.headers.set(name, value);
	}

	@Override
	protected void doAddHeader(String name, String value) {
		this.headers.add(name, value);
	}

	@Override
	public String uri() {
		return this.nettyRequest.getUri();
	}

	@Override
	public Method method() {
		return new Method(this.nettyRequest.getMethod().name());
	}

	@Override
	public NettyHttpHeaders headers() {
		return this.headers;
	}

	public HttpRequest getNettyRequest() {
		return nettyRequest;
	}

	// RESPONSE contract

	@Override
	public Status responseStatus() {
		return Status.valueOf(this.nettyResponse.getStatus().code());
	}

	@Override
	public void doResponseStatus(Status status) {
		this.nettyResponse.setStatus(HttpResponseStatus.valueOf(status.getCode()));
	}

	@Override
	public Transfer transfer() {
		if ("chunked".equals(this.headers.get(ResponseHeaders.TRANSFER_ENCODING))) {
			Assert.isTrue(Protocol.HTTP_1_1.equals(protocol()));
			return Transfer.CHUNKED;
		} else if (this.headers.get(ResponseHeaders.TRANSFER_ENCODING) == null) {
			return Transfer.NON_CHUNKED;
		}
		throw new IllegalStateException("Can't determine a valide transfer based on headers and protocol");
	}

	@Override
	public HttpChannel<Buffer, Buffer> transfer(Transfer transfer) {
		switch (transfer) {
			case EVENT_STREAM:
				this.responseHeader(ResponseHeaders.CONTENT_TYPE, "text/event-stream");
			case CHUNKED:
				Assert.isTrue(Protocol.HTTP_1_1.equals(protocol()));
				this.responseHeader(ResponseHeaders.TRANSFER_ENCODING, "chunked");
				break;
			case NON_CHUNKED:
				this.responseHeaders().remove(ResponseHeaders.TRANSFER_ENCODING);
		}
		return this;
	}

	@Override
	public NettyHttpResponseHeaders responseHeaders() {
		return this.responseHeaders;
	}

	@Override
	protected void doResponseHeader(String name, String value) {
		this.responseHeaders.set(name, value);
	}

	@Override
	protected void doAddResponseHeader(String name, String value) {
		this.responseHeaders.add(name, value);
	}

	public HttpResponse getNettyResponse() {
		return nettyResponse;
	}

	@Override
	public boolean isWebsocket() {
		String isWebsocket = headers.get(HttpHeaders.UPGRADE);
		return isWebsocket != null && isWebsocket.toLowerCase().equals("websocket");
	}

	@Override
	@SuppressWarnings("unchecked")
	public SocketChannel delegate() {
		return (SocketChannel) tcpStream.delegate();
	}

	@Override
	public InetSocketAddress remoteAddress() {
		return tcpStream.remoteAddress();
	}

	@Override
	public ConsumerSpec on() {
		return tcpStream.on();
	}

	void setNettyResponse(HttpResponse nettyResponse) {
		this.nettyResponse = nettyResponse;
	}

	@Override
	public boolean isKeepAlive() {
		return headers.isKeepAlive();
	}

	@Override
	public HttpChannel<Buffer, Buffer> keepAlive(boolean keepAlive) {
		responseHeaders.keepAlive(keepAlive);
		return this;
	}
}
