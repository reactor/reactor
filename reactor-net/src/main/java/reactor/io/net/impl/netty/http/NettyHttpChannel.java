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
import io.netty.channel.Channel;
import io.netty.handler.codec.http.*;
import org.reactivestreams.Subscriber;
import org.reactivestreams.Subscription;
import reactor.Environment;
import reactor.core.dispatch.SynchronousDispatcher;
import reactor.core.support.Assert;
import reactor.io.buffer.Buffer;
import reactor.io.codec.Codec;
import reactor.io.net.ChannelStream;
import reactor.io.net.PeerStream;
import reactor.io.net.http.HttpChannel;
import reactor.io.net.http.model.HttpHeaders;
import reactor.io.net.http.model.*;
import reactor.io.net.impl.netty.NettyChannelStream;

import java.net.InetSocketAddress;
import java.nio.ByteBuffer;

/**
 * @author Sebastien Deleuze
 * @author Stephane Maldini
 */
public class NettyHttpChannel<IN, OUT> extends HttpChannel<IN, OUT> {

	private final NettyChannelStream<IN, OUT> tcpStream;
	private final HttpRequest                 nettyRequest;
	private final NettyHttpHeaders            headers;
	private       HttpResponse                nettyResponse;
	private       NettyHttpResponseHeaders    responseHeaders;

	public NettyHttpChannel(NettyChannelStream<IN, OUT> tcpStream,
	                        PeerStream<IN, OUT, ChannelStream<IN, OUT>> server,
	                        HttpRequest request, Codec<Buffer, IN, OUT> codec
	) {
		super(tcpStream.getEnvironment(), codec, tcpStream.getCapacity(), server,
				SynchronousDispatcher.INSTANCE, SynchronousDispatcher.INSTANCE);
		this.tcpStream = tcpStream;
		this.nettyRequest = request;
		this.nettyResponse = new DefaultHttpResponse(HttpVersion.HTTP_1_1, HttpResponseStatus.OK);
		this.headers = new NettyHttpHeaders(request);
		this.responseHeaders = new NettyHttpResponseHeaders(this.nettyResponse);

		responseHeader(ResponseHeaders.TRANSFER_ENCODING, "chunked");
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
	public HttpHeaders headers() {
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
	public HttpChannel<IN, OUT> transfer(Transfer transfer) {
		switch (transfer) {
			case EVENT_STREAM:
				throw new IllegalStateException("Transfer " + Transfer.EVENT_STREAM + " is not supported yet");
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
	public ResponseHeaders responseHeaders() {
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
	public Channel delegate() {
		return tcpStream.delegate();
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
	protected void write(ByteBuffer data, Subscriber<?> onComplete, boolean flush) {
		write(new DefaultHttpContent(Unpooled.wrappedBuffer(data)), onComplete, flush);
	}

	@Override
	protected void write(final Object data, final Subscriber<?> onComplete, boolean flush) {
		if (HEADERS_SENT.compareAndSet(this, 0, 1)) {
			tcpStream.write(nettyResponse, new Subscriber<Object>() {
				@Override
				public void onSubscribe(Subscription s) {
					//IGNORE
				}

				@Override
				public void onNext(Object o) {
					//IGNORE
				}

				@Override
				public void onError(Throwable t) {
					if(Environment.alive()){
						Environment.get().routeError(t);
					}
				}

				@Override
				public void onComplete() {
					tcpStream.write(data, onComplete, false);
				}
			}, true);
		}else{
			tcpStream.write(data, onComplete, flush);
		}
	}

	@Override
	protected void flush() {
		tcpStream.flush();
	}

	NettyChannelStream<IN, OUT> tcpStream() {
		return tcpStream;
	}
}
