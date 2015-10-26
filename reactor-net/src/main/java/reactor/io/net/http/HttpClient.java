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

import java.net.InetSocketAddress;

import org.reactivestreams.Publisher;
import reactor.Publishers;
import reactor.fn.Function;
import reactor.fn.timer.Timer;
import reactor.fn.tuple.Tuple2;
import reactor.io.net.ReactiveChannelHandler;
import reactor.io.net.ReactiveClient;
import reactor.io.net.Reconnect;
import reactor.io.net.config.ClientSocketOptions;
import reactor.io.net.http.model.Method;

/**
 * The base class for a Reactor-based Http client.
 * @param <IN> The type that will be received by this client
 * @param <OUT> The type that will be sent by this client
 * @author Jon Brisbin
 * @author Stephane Maldini
 */
public abstract class HttpClient<IN, OUT>
		extends ReactiveClient<IN, OUT, HttpChannel<IN, OUT>> {

	protected HttpClient(Timer timer, ClientSocketOptions options) {
		super(timer, options != null ? options.prefetch() : 1);
	}

	/**
	 * HTTP GET the passed URL. When connection has been made, the passed handler is
	 * invoked and can be used to build precisely the request and write data to it.
	 * @param url the target remote URL
	 * @param handler the {@link ReactiveChannelHandler} to invoke on open channel
	 * @return a {@link Publisher} of the {@link HttpChannel} ready to consume for
	 * response
	 */
	public final Publisher<? extends HttpChannel<IN, OUT>> get(String url,
			final ReactiveChannelHandler<IN, OUT, HttpChannel<IN, OUT>> handler) {
		return request(Method.GET, url, handler);
	}

	/**
	 * HTTP GET the passed URL.
	 * @param url the target remote URL
	 * @return a {@link Publisher} of the {@link HttpChannel} ready to consume for
	 * response
	 */
	public final Publisher<? extends HttpChannel<IN, OUT>> get(String url) {

		return request(Method.GET, url, null);
	}

	/**
	 * HTTP POST the passed URL. When connection has been made, the passed handler is
	 * invoked and can be used to build precisely the request and write data to it.
	 * @param url the target remote URL
	 * @param handler the {@link ReactiveChannelHandler} to invoke on open channel
	 * @return a {@link Publisher} of the {@link HttpChannel} ready to consume for
	 * response
	 */
	public final Publisher<? extends HttpChannel<IN, OUT>> post(String url,
			final ReactiveChannelHandler<IN, OUT, HttpChannel<IN, OUT>> handler) {
		return request(Method.POST, url, handler);
	}

	/**
	 * HTTP PUT the passed URL. When connection has been made, the passed handler is
	 * invoked and can be used to build precisely the request and write data to it.
	 * @param url the target remote URL
	 * @param handler the {@link ReactiveChannelHandler} to invoke on open channel
	 * @return a {@link Publisher} of the {@link HttpChannel} ready to consume for
	 * response
	 */
	public final Publisher<? extends HttpChannel<IN, OUT>> put(String url,
			final ReactiveChannelHandler<IN, OUT, HttpChannel<IN, OUT>> handler) {
		return request(Method.PUT, url, handler);
	}

	/**
	 * HTTP DELETE the passed URL. When connection has been made, the passed handler is
	 * invoked and can be used to build precisely the request and write data to it.
	 * @param url the target remote URL
	 * @param handler the {@link ReactiveChannelHandler} to invoke on open channel
	 * @return a {@link Publisher} of the {@link HttpChannel} ready to consume for
	 * response
	 */
	public final Publisher<? extends HttpChannel<IN, OUT>> delete(String url,
			final ReactiveChannelHandler<IN, OUT, HttpChannel<IN, OUT>> handler) {
		return request(Method.DELETE, url, handler);
	}

	/**
	 * HTTP DELETE the passed URL. When connection has been made, the passed handler is
	 * invoked and can be used to build precisely the request and write data to it.
	 * @param url the target remote URL
	 * @return a {@link Publisher} of the {@link HttpChannel} ready to consume for
	 * response
	 */
	public final Publisher<? extends HttpChannel<IN, OUT>> delete(String url) {
		return request(Method.DELETE, url, null);
	}

	/**
	 * WebSocket to the passed URL.
	 * @param url the target remote URL
	 * @return a {@link Publisher} of the {@link HttpChannel} ready to consume for
	 * response
	 */
	public final Publisher<? extends HttpChannel<IN, OUT>> ws(String url) {
		return request(Method.WS, url, null);
	}

	/**
	 * WebSocket to the passed URL. When connection has been made, the passed handler is
	 * invoked and can be used to build
	 *
	 * precisely the request and write data to it.
	 * @param url the target remote URL
	 * @param handler the {@link ReactiveChannelHandler} to invoke on open channel
	 * @return a {@link Publisher} of the {@link HttpChannel} ready to consume for
	 * response
	 */
	public final Publisher<? extends HttpChannel<IN, OUT>> ws(String url,
			final ReactiveChannelHandler<IN, OUT, HttpChannel<IN, OUT>> handler) {
		return request(Method.WS, url, handler);
	}

	/**
	 * Use the passed HTTP method to send to the given URL. When connection has been made,
	 * the passed handler is invoked and can be used to build precisely the request and
	 * write data to it.
	 * @param method the HTTP method to send
	 * @param url the target remote URL
	 * @param handler the {@link ReactiveChannelHandler} to invoke on open channel
	 * @return a {@link Publisher} of the {@link HttpChannel} ready to consume for
	 * response
	 */
	public abstract Publisher<? extends HttpChannel<IN, OUT>> request(Method method,
			String url,
			final ReactiveChannelHandler<IN, OUT, HttpChannel<IN, OUT>> handler);

	/**
	 *
	 * @param preprocessor
	 * @param <NEWIN>
	 * @param <NEWOUT>
	 * @param <NEWCONN>
	 * @return
	 */
	public <NEWIN, NEWOUT, NEWCONN extends HttpChannel<NEWIN, NEWOUT>> HttpClient<NEWIN, NEWOUT> httpProcessor(
			final HttpProcessor<IN, OUT, ? super HttpChannel<IN, OUT>, NEWIN, NEWOUT, NEWCONN> preprocessor
	){
		return new PreprocessedHttpClient<>(preprocessor);
	}

	private final class PreprocessedHttpClient<NEWIN, NEWOUT, NEWCONN extends HttpChannel<NEWIN, NEWOUT>>
			extends HttpClient<NEWIN, NEWOUT> {

		private final HttpProcessor<IN, OUT, ? super HttpChannel<IN, OUT>, NEWIN, NEWOUT, NEWCONN>
				preprocessor;

		public PreprocessedHttpClient(
				HttpProcessor<IN, OUT, ? super HttpChannel<IN, OUT>, NEWIN, NEWOUT, NEWCONN> preprocessor) {
			super(HttpClient.this.getDefaultTimer(), null);
			this.preprocessor = preprocessor;
		}

		@Override
		public Publisher<? extends HttpChannel<NEWIN, NEWOUT>> request(Method method,
				String url,
				final ReactiveChannelHandler<NEWIN, NEWOUT, HttpChannel<NEWIN, NEWOUT>> handler) {
			return Publishers.map(HttpClient.this.request(method, url, handler != null ?
					new ReactiveChannelHandler<IN, OUT, HttpChannel<IN, OUT>>() {
				@Override
				public Publisher<Void> apply(HttpChannel<IN, OUT> conn) {
					return handler.apply(preprocessor.transform(conn));
				}
			} : null), new Function<HttpChannel<IN, OUT>, HttpChannel<NEWIN, NEWOUT>>() {
				@Override
				public HttpChannel<NEWIN, NEWOUT> apply(HttpChannel<IN, OUT> channel) {
					return preprocessor.transform(channel);
				}
			});
		}

		@Override
		protected Publisher<Tuple2<InetSocketAddress, Integer>> doStart(
				final ReactiveChannelHandler<NEWIN, NEWOUT, HttpChannel<NEWIN, NEWOUT>> handler,
				Reconnect reconnect) {
			return HttpClient.this.start(new ReactiveChannelHandler<IN, OUT, HttpChannel<IN, OUT>>() {
				@Override
				public Publisher<Void> apply(HttpChannel<IN, OUT> conn) {
					return handler.apply(preprocessor.transform(conn));
				}
			}, reconnect);
		}

		@Override
		protected Publisher<Void> doStart(
				final ReactiveChannelHandler<NEWIN, NEWOUT, HttpChannel<NEWIN, NEWOUT>> handler) {
			return HttpClient.this.start(new ReactiveChannelHandler<IN, OUT, HttpChannel<IN, OUT>>() {
				@Override
				public Publisher<Void> apply(HttpChannel<IN, OUT> conn) {
					return handler.apply(preprocessor.transform(conn));
				}
			});
		}

		@Override
		protected Publisher<Void> doShutdown() {
			return HttpClient.this.shutdown();
		}
	}
}
