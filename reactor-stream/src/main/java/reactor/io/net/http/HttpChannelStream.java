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

package reactor.io.net.http;

import java.util.Map;

import org.reactivestreams.Publisher;
import reactor.fn.timer.Timer;
import reactor.io.net.ChannelStream;
import reactor.io.net.ReactiveChannel;
import reactor.io.net.ReactiveChannelHandler;
import reactor.io.net.ReactorChannelHandler;
import reactor.io.net.http.model.HttpHeaders;
import reactor.io.net.http.model.Method;
import reactor.io.net.http.model.Protocol;
import reactor.io.net.http.model.ResponseHeaders;
import reactor.io.net.http.model.Status;
import reactor.io.net.http.model.Transfer;
import reactor.rx.Stream;
import reactor.rx.Streams;

/**
 * A Request/Response {@link ChannelStream} extension that provides for several helpers to
 * control HTTP behavior and observe its metadata.
 * @author Stephane maldini
 * @since 2.1
 */
public class HttpChannelStream<IN, OUT> extends ChannelStream<IN, OUT> {

	private final HttpChannel<IN, OUT> actual;

	/**
	 *
	 * @param actual
	 * @param timer
	 * @param prefetch
	 * @param <IN>
	 * @param <OUT>
	 * @return
	 */
	public static <IN, OUT> HttpChannelStream<IN, OUT> wrap(final HttpChannel<IN, OUT> actual, Timer timer, long prefetch){
		return new HttpChannelStream<>(actual, timer, prefetch);
	}

	/**
	 *
	 * @param actual
	 * @param timer
	 * @param prefetch
	 * @param <IN>
	 * @param <OUT>
	 * @return
	 */
	public static <IN, OUT> ReactiveChannelHandler<IN, OUT, HttpChannel<IN, OUT>> wrap(
			final ReactorHttpHandler<IN, OUT> actual, final Timer timer, final long prefetch){

		if(actual == null) return null;

		return new ReactiveChannelHandler<IN, OUT, HttpChannel<IN, OUT>>() {
			@Override
			public Publisher<Void> apply(HttpChannel<IN, OUT> stream) {
					return actual.apply(wrap(stream, timer, prefetch));
			}
		};
	}

	protected HttpChannelStream(HttpChannel<IN, OUT> actual, Timer timer, long prefetch) {
		super(actual, timer, prefetch);
		this.actual = actual;
	}

	// REQUEST contract

	/**
	 * @see HttpChannel#params()
	 */
	public final Map<String, String> params() {
		return actual.params();
	}

	/**
	 * Read URI param from the given key
	 * @param key matching key
	 * @return the resolved parameter for the given key name
	 */
	public final String param(String key) {
		return actual.param(key);
	}

	/**
	 * @return Resolved HTTP request headers
	 */
	public HttpHeaders headers(){
		return actual.headers();
	}

	/**
	 * Register an HTTP request header
	 * @param name Header name
	 * @param value Header content
	 * @return this
	 */
	public final HttpChannelStream<IN, OUT> header(String name, String value) {
		actual.header(name, value);
		return this;
	}

	/**
	 * Is the request keepAlive
	 * @return is keep alive
	 */
	public boolean isKeepAlive(){
		return actual.isKeepAlive();
	}

	/**
	 * set the request keepAlive if true otherwise remove the existing connection keep
	 * alive header
	 * @return is keep alive
	 */
	public HttpChannelStream<IN, OUT> keepAlive(boolean keepAlive){
		actual.keepAlive(keepAlive);
		return this;
	}


	/**
	 * Accumulate a Request Header using the given name and value, appending ";" for each
	 * new value
	 * @return this
	 */
	public HttpChannelStream<IN, OUT> addHeader(String name, String value) {
		actual.addHeader(name, value);
		return this;
	}

	/**
	 * @return the resolved request protocol (HTTP 1.1 etc)
	 */
	public Protocol protocol(){
		return actual.protocol();
	}

	/**
	 * @return the resolved target address
	 */
	public String uri(){
		return actual.uri();
	}

	/**
	 * @return the resolved request method (HTTP 1.1 etc)
	 */
	public Method method(){
		return actual.method();
	}


	// RESPONSE contract

	/**
	 * @return the resolved HTTP Response Status
	 */
	public Status responseStatus(){
		return actual.responseStatus();
	}

	/**
	 * Set the response status to an outgoing response
	 * @param status the status to define
	 * @return this
	 */
	public HttpChannelStream<IN, OUT> responseStatus(Status status) {
		actual.responseStatus(status);
		return this;
	}

	/**
	 * @return the resolved response HTTP headers
	 */
	public ResponseHeaders responseHeaders(){
		return actual.responseHeaders();
	}

	/**
	 * Define the response HTTP header for the given key
	 * @param name the HTTP response header key to override
	 * @param value the HTTP response header content
	 * @return this
	 */
	public final HttpChannelStream<IN, OUT> responseHeader(String name, String value) {
		actual.responseHeader(name, value);
		return this;
	}

	/**
	 * Accumulate a response HTTP header for the given key name, appending ";" for each
	 * new value
	 * @param name the HTTP response header name
	 * @param value the HTTP response header value
	 * @return this
	 */
	public HttpChannelStream<IN, OUT> addResponseHeader(String name, String value) {
		actual.addResponseHeader(name, value);
		return this;
	}

	/**
	 * Flush the headers if not sent. Might be useful for the case
	 * @return Stream to signal error or successful write to the client
	 */
	public Stream<Void> writeHeaders() {
		return Streams.wrap(actual.writeHeaders());
	}

	/**
	 * @return the Transfer setting SSE for this http connection (e.g. event-stream)
	 */
	public HttpChannelStream<IN, OUT> sse() {
		return transfer(Transfer.EVENT_STREAM);
	}

	/**
	 * @return the Transfer setting for this http connection (e.g. event-stream)
	 */
	public Transfer transfer(){
		return actual.transfer();
	}

	/**
	 * Define the Transfer mode for this http connection
	 * @param transfer the new transfer mode
	 * @return this
	 */
	public HttpChannelStream<IN, OUT> transfer(Transfer transfer){
		actual.transfer(transfer);
		return this;
	}

	public boolean isWebsocket(){
		return actual.isWebsocket();
	}

	@Override
	public final Stream<Void> writeWith(final Publisher<? extends OUT> source) {
		return Streams.wrap(actual.writeWith(source));
	}
}
