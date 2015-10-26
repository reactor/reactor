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

import reactor.fn.Function;
import reactor.io.net.ReactiveChannelHandler;
import reactor.io.net.ReactivePeer;
import reactor.io.net.http.model.Method;
import reactor.rx.Promise;
import reactor.rx.Promises;
import reactor.rx.Stream;
import reactor.rx.Streams;

/**
 * The base class for a Reactor-based Http client.
 * @param <IN> The type that will be received by this client
 * @param <OUT> The type that will be sent by this client
 *
 * @author Stephane Maldini
 * @since 2.1
 */
public final class ReactorHttpClient<IN, OUT> {

	private final HttpClient<IN, OUT> client;

	public static <IN, OUT> ReactorHttpClient<IN,OUT> create(HttpClient<IN, OUT> client) {
		return new ReactorHttpClient<>(client);
	}

	protected ReactorHttpClient(HttpClient<IN, OUT> client) {
		this.client = client;
	}

	/**
	 * Shutdown this {@literal ReactorPeer} and complete the returned {@link Promise <Void>}
	 * when shut down.
	 * @return a {@link Promise<Void>} that will be complete when the {@link
	 * ReactivePeer} is shutdown
	 */
	public Promise<Void> shutdown() {
		return Promises.from(client.shutdown());
	}


	/**
	 * HTTP GET the passed URL. When connection has been made, the passed handler is
	 * invoked and can be used to build precisely the request and write data to it.
	 * @param url the target remote URL
	 * @param handler the {@link ReactiveChannelHandler} to invoke on open channel
	 * @return a {@link Stream} of the {@link HttpChannel} ready to consume for
	 * response
	 */
	public final Stream<HttpChannelStream<IN, OUT>> get(String url,
			final ReactorHttpHandler<IN, OUT> handler) {
		return request(Method.GET, url, handler);
	}

	/**
	 * HTTP GET the passed URL.
	 * @param url the target remote URL
	 * @return a {@link Stream} of the {@link HttpChannel} ready to consume for
	 * response
	 */
	public final Stream<HttpChannelStream<IN, OUT>> get(String url) {

		return request(Method.GET, url, null);
	}

	/**
	 * HTTP POST the passed URL. When connection has been made, the passed handler is
	 * invoked and can be used to build precisely the request and write data to it.
	 * @param url the target remote URL
	 * @param handler the {@link ReactiveChannelHandler} to invoke on open channel
	 * @return a {@link Stream} of the {@link HttpChannel} ready to consume for
	 * response
	 */
	public final Stream<HttpChannelStream<IN, OUT>> post(String url,
			final ReactorHttpHandler<IN, OUT> handler) {
		return request(Method.POST, url, handler);
	}

	/**
	 * HTTP PUT the passed URL. When connection has been made, the passed handler is
	 * invoked and can be used to build precisely the request and write data to it.
	 * @param url the target remote URL
	 * @param handler the {@link ReactiveChannelHandler} to invoke on open channel
	 * @return a {@link Stream} of the {@link HttpChannel} ready to consume for
	 * response
	 */
	public final Stream<HttpChannelStream<IN, OUT>> put(String url,
			final ReactorHttpHandler<IN, OUT> handler) {
		return request(Method.PUT, url, handler);
	}

	/**
	 * HTTP DELETE the passed URL. When connection has been made, the passed handler is
	 * invoked and can be used to build precisely the request and write data to it.
	 * @param url the target remote URL
	 * @param handler the {@link ReactiveChannelHandler} to invoke on open channel
	 * @return a {@link Stream} of the {@link HttpChannel} ready to consume for
	 * response
	 */
	public final Stream<HttpChannelStream<IN, OUT>> delete(String url,
			final ReactorHttpHandler<IN, OUT> handler) {
		return request(Method.DELETE, url, handler);
	}

	/**
	 * HTTP DELETE the passed URL. When connection has been made, the passed handler is
	 * invoked and can be used to build precisely the request and write data to it.
	 * @param url the target remote URL
	 * @return a {@link Stream} of the {@link HttpChannel} ready to consume for
	 * response
	 */
	public final Stream<HttpChannelStream<IN, OUT>> delete(String url) {
		return request(Method.DELETE, url, null);
	}

	/**
	 * WebSocket to the passed URL.
	 * @param url the target remote URL
	 * @return a {@link Stream} of the {@link HttpChannel} ready to consume for
	 * response
	 */
	public final Stream<HttpChannelStream<IN, OUT>> ws(String url) {
		return request(Method.WS, url, null);
	}

	/**
	 * WebSocket to the passed URL. When connection has been made, the passed handler is
	 * invoked and can be used to build
	 *
	 * precisely the request and write data to it.
	 * @param url the target remote URL
	 * @param handler the {@link ReactiveChannelHandler} to invoke on open channel
	 * @return a {@link Stream} of the {@link HttpChannel} ready to consume for
	 * response
	 */
	public final Stream<HttpChannelStream<IN, OUT>> ws(String url,
			final ReactorHttpHandler<IN, OUT> handler) {
		return request(Method.WS, url, handler);
	}

	/**
	 * Use the passed HTTP method to send to the given URL. When connection has been made,
	 * the passed handler is invoked and can be used to build precisely the request and
	 * write data to it.
	 * @param method the HTTP method to send
	 * @param url the target remote URL
	 * @param handler the {@link ReactiveChannelHandler} to invoke on open channel
	 * @return a {@link Stream} of the {@link HttpChannel} ready to consume for
	 * response
	 */
	public Stream<HttpChannelStream<IN, OUT>> request(Method method,
			String url,
			final ReactorHttpHandler<IN, OUT> handler){

		return Streams.wrap(
				client.request(method, url,
						HttpChannelStream.wrap(handler, client.getDefaultTimer(), client.getDefaultPrefetchSize())
				)
		).map(new Function<HttpChannel<IN, OUT>, HttpChannelStream<IN, OUT>>() {
			@Override
			public HttpChannelStream<IN, OUT> apply(HttpChannel<IN, OUT> channel) {
				return HttpChannelStream.wrap(channel, client.getDefaultTimer(), client.getDefaultPrefetchSize());
			}
		});
	}

}
