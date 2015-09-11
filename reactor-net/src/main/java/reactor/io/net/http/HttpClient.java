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

import reactor.fn.timer.Timer;
import reactor.io.buffer.Buffer;
import reactor.io.codec.Codec;
import reactor.io.net.ReactorChannelHandler;
import reactor.io.net.ReactorClient;
import reactor.io.net.config.ClientSocketOptions;
import reactor.io.net.http.model.Method;
import reactor.rx.Stream;

/**
 * The base class for a Reactor-based Http client.
 *
 * @param <IN>  The type that will be received by this client
 * @param <OUT> The type that will be sent by this client
 * @author Jon Brisbin
 * @author Stephane Maldini
 */
public abstract class HttpClient<IN, OUT>
  extends ReactorClient<IN, OUT, HttpChannel<IN, OUT>> {

	protected HttpClient(Timer timer,
	                     Codec<Buffer, IN, OUT> codec,
	                     ClientSocketOptions options) {
		super(timer, codec, options != null ? options.prefetch() : 1);
	}

	/**
	 * HTTP GET the passed URL. When connection has been made, the passed handler is invoked and can be used to build
	 * precisely the request and write data to it.
	 *
	 * @param url     the target remote URL
	 * @param handler the {@link ReactorChannelHandler} to invoke on open channel
	 * @return a {@link Stream} of the {@link HttpChannel} ready to consume for response
	 */
	public final Stream<? extends HttpChannel<IN, OUT>> get(String url,
	                                                         final ReactorChannelHandler<IN, OUT, HttpChannel<IN, OUT>>
	                                                           handler) {
		return request(Method.GET, url, handler);
	}


	/**
	 * HTTP GET the passed URL.
	 *
	 * @param url the target remote URL
	 * @return a {@link Stream} of the {@link HttpChannel} ready to consume for response
	 */
	public final Stream<? extends HttpChannel<IN, OUT>> get(String url) {

		return request(Method.GET, url, null);
	}

	/**
	 * HTTP POST the passed URL. When connection has been made, the passed handler is invoked and can be used to build
	 * precisely the request and write data to it.
	 *
	 * @param url     the target remote URL
	 * @param handler the {@link ReactorChannelHandler} to invoke on open channel
	 * @return a {@link Stream} of the {@link HttpChannel} ready to consume for response
	 */
	public final Stream<? extends HttpChannel<IN, OUT>> post(String url,
	                                                          final ReactorChannelHandler<IN, OUT, HttpChannel<IN,
	                                                            OUT>>
	                                                            handler) {
		return request(Method.POST, url, handler);
	}


	/**
	 * HTTP PUT the passed URL. When connection has been made, the passed handler is invoked and can be used to build
	 * precisely the request and write data to it.
	 *
	 * @param url     the target remote URL
	 * @param handler the {@link ReactorChannelHandler} to invoke on open channel
	 * @return a {@link Stream} of the {@link HttpChannel} ready to consume for response
	 */
	public final Stream<? extends HttpChannel<IN, OUT>> put(String url,
	                                                         final ReactorChannelHandler<IN, OUT, HttpChannel<IN, OUT>>
	                                                           handler) {
		return request(Method.PUT, url, handler);
	}

	/**
	 * HTTP DELETE the passed URL. When connection has been made, the passed handler is invoked and can be used to
	 * build
	 * precisely the request and write data to it.
	 *
	 * @param url     the target remote URL
	 * @param handler the {@link ReactorChannelHandler} to invoke on open channel
	 * @return a {@link Stream} of the {@link HttpChannel} ready to consume for response
	 */
	public final Stream<? extends HttpChannel<IN, OUT>> delete(String url,
	                                                            final ReactorChannelHandler<IN, OUT, HttpChannel<IN,
	                                                              OUT>> handler) {
		return request(Method.DELETE, url, handler);
	}

	/**
	 * HTTP DELETE the passed URL. When connection has been made, the passed handler is invoked and can be used to
	 * build
	 * precisely the request and write data to it.
	 *
	 * @param url the target remote URL
	 * @return a {@link Stream} of the {@link HttpChannel} ready to consume for response
	 */
	public final Stream<? extends HttpChannel<IN, OUT>> delete(String url) {
		return request(Method.DELETE, url, null);
	}

	/**
	 * WebSocket to the passed URL.
	 *
	 * @param url the target remote URL
	 * @return a {@link Stream} of the {@link HttpChannel} ready to consume for response
	 */
	public final Stream<? extends HttpChannel<IN, OUT>> ws(String url) {
		return request(Method.WS, url, null);
	}

	/**
	 * WebSocket to the passed URL. When connection has been made, the passed handler is invoked and can be used to
	 * build
	 *
	 * precisely the request and write data to it.
	 *
	 * @param url     the target remote URL
	 * @param handler the {@link ReactorChannelHandler} to invoke on open channel
	 * @return a {@link Stream} of the {@link HttpChannel} ready to consume for response
	 */
	public final Stream<? extends HttpChannel<IN, OUT>> ws(String url,
	                                                        final ReactorChannelHandler<IN, OUT, HttpChannel<IN, OUT>>
	                                                          handler) {
		return request(Method.WS, url, handler);
	}

	/**
	 * Use the passed HTTP method to send to the given URL.
	 * When connection has been made, the passed handler is invoked and can be used to build
	 * precisely the request and write data to it.
	 *
	 * @param method  the HTTP method to send
	 * @param url     the target remote URL
	 * @param handler the {@link ReactorChannelHandler} to invoke on open channel
	 * @return a {@link Stream} of the {@link HttpChannel} ready to consume for response
	 */
	public abstract Stream<? extends HttpChannel<IN, OUT>> request(Method method, String url,
	                                                                final ReactorChannelHandler<IN, OUT,
	                                                                  HttpChannel<IN,
	                                                                  OUT>> handler);

}
