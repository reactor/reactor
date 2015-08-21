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

import org.reactivestreams.Publisher;
import reactor.Environment;
import reactor.bus.registry.Registration;
import reactor.bus.registry.Registries;
import reactor.bus.registry.Registry;
import reactor.bus.selector.Selector;
import reactor.ReactorProcessor;
import reactor.io.buffer.Buffer;
import reactor.io.codec.Codec;
import reactor.io.net.NetSelectors;
import reactor.io.net.ReactorChannelHandler;
import reactor.io.net.ReactorPeer;
import reactor.io.net.http.model.HttpHeaders;
import reactor.rx.Promise;

import java.net.InetSocketAddress;
import java.util.Iterator;
import java.util.List;

/**
 * Base functionality needed by all servers that communicate with clients over HTTP.
 *
 * @param <IN>  The type that will be received by this server
 * @param <OUT> The type that will be sent by this server
 * @author Stephane Maldini
 */
public abstract class HttpServer<IN, OUT>
  extends ReactorPeer<IN, OUT, HttpChannel<IN, OUT>> {

	protected final Registry<HttpChannel, ReactorChannelHandler<IN, OUT, HttpChannel<IN, OUT>>> routedWriters;

	private boolean hasWebsocketEndpoints = false;

	protected HttpServer(Environment env, ReactorProcessor dispatcher, Codec<Buffer, IN, OUT> codec) {
		super(env, dispatcher, codec);
		this.routedWriters = Registries.create();
	}

	/**
	 * Start the server without any global handler, only the specific routed methods (get, post...) will apply.
	 *
	 * @return a Promise fulfilled when server is started
	 */
	public Promise<Void> start() {
		return start(null);
	}

	/**
	 * Get the address to which this server is bound. If port 0 was used on configuration, try resolving the port.
	 *
	 * @return the bind address
	 */
	public abstract InetSocketAddress getListenAddress();

	/**
	 * Register an handler for the given Selector condition, incoming connections will query the internal registry
	 * to invoke the matching handlers. Implementation may choose to reply 404 if no route matches.
	 *
	 * @param condition       a {@link Selector} to match the incoming connection with registered handler
	 * @param serviceFunction an handler to invoke for the given condition
	 * @return {@code this}
	 */
	@SuppressWarnings("unchecked")
	public HttpServer<IN, OUT> route(
	  final Selector<HttpChannel> condition,
	  final ReactorChannelHandler<IN, OUT, HttpChannel<IN, OUT>> serviceFunction) {

		routedWriters.register(condition, serviceFunction);
		return this;
	}

	/**
	 * Listen for HTTP GET on the passed path to be used as a routing condition. Incoming connections will query the
	 * internal registry
	 * to invoke the matching handlers.
	 * <p>
	 * e.g. "/test/{param}". Params are resolved using {@link HttpChannel#param(String)}
	 *
	 * @param path    The {@link HttpSelector} to resolve against this path, pattern matching and capture are supported
	 * @param handler an handler to invoke for the given condition
	 * @return {@code this}
	 */
	public final HttpServer<IN, OUT> get(String path,
	                                     final ReactorChannelHandler<IN, OUT, HttpChannel<IN, OUT>> handler) {
		route(NetSelectors.get(path), handler);
		return this;
	}

	/**
	 * Listen for HTTP POST on the passed path to be used as a routing condition. Incoming connections will query the
	 * internal registry
	 * to invoke the matching handlers.
	 * <p>
	 * e.g. "/test/{param}". Params are resolved using {@link HttpChannel#param(String)}
	 *
	 * @param path    The {@link HttpSelector} to resolve against this path, pattern matching and capture are supported
	 * @param handler an handler to invoke for the given condition
	 * @return {@code this}
	 */
	public final HttpServer<IN, OUT> post(String path,
	                                      final ReactorChannelHandler<IN, OUT, HttpChannel<IN, OUT>> handler) {
		route(NetSelectors.post(path), handler);
		return this;
	}


	/**
	 * Listen for HTTP PUT on the passed path to be used as a routing condition. Incoming connections will query the
	 * internal registry
	 * to invoke the matching handlers.
	 * <p>
	 * e.g. "/test/{param}". Params are resolved using {@link HttpChannel#param(String)}
	 *
	 * @param path    The {@link HttpSelector} to resolve against this path, pattern matching and capture are supported
	 * @param handler an handler to invoke for the given condition
	 * @return {@code this}
	 */
	public final HttpServer<IN, OUT> put(String path,
	                                     final ReactorChannelHandler<IN, OUT, HttpChannel<IN, OUT>> handler) {
		route(NetSelectors.put(path), handler);
		return this;
	}


	/**
	 * Listen for WebSocket on the passed path to be used as a routing condition. Incoming connections will query the
	 * internal registry
	 * to invoke the matching handlers.
	 * <p>
	 * e.g. "/test/{param}". Params are resolved using {@link HttpChannel#param(String)}
	 *
	 * @param path    The {@link HttpSelector} to resolve against this path, pattern matching and capture are supported
	 * @param handler an handler to invoke for the given condition
	 * @return {@code this}
	 */
	public final HttpServer<IN, OUT> ws(String path,
	                                    final ReactorChannelHandler<IN, OUT, HttpChannel<IN, OUT>> handler) {
		route(NetSelectors.get(path), handler);
		hasWebsocketEndpoints = true;
		return this;
	}

	/**
	 * Listen for HTTP DELETE on the passed path to be used as a routing condition. Incoming connections will query
	 * the internal registry
	 * to invoke the matching handlers.
	 * <p>
	 * e.g. "/test/{param}". Params are resolved using {@link HttpChannel#param(String)}
	 *
	 * @param path    The {@link HttpSelector} to resolve against this path, pattern matching and capture are supported
	 * @param handler an handler to invoke for the given condition
	 * @return {@code this}
	 */
	public final HttpServer<IN, OUT> delete(String path,
	                                        final ReactorChannelHandler<IN, OUT, HttpChannel<IN, OUT>> handler) {
		route(NetSelectors.delete(path), handler);
		return this;
	}

	protected abstract void onWebsocket(HttpChannel<IN, OUT> next);

	protected final boolean hasWebsocketEndpoints() {
		return hasWebsocketEndpoints;
	}

	protected Iterable<? extends Publisher<Void>> routeChannel(final HttpChannel<IN, OUT> ch) {
		final List<Registration<HttpChannel, ? extends ReactorChannelHandler<IN, OUT, HttpChannel<IN, OUT>>>>
		  selected = routedWriters.select(ch);

		if (hasWebsocketEndpoints) {
			String connection = ch.headers().get(HttpHeaders.CONNECTION);
			if (connection != null && connection.equals(HttpHeaders.UPGRADE)) {
				onWebsocket(ch);
			}
		}

		return new Iterable<Publisher<Void>>() {
			@Override
			public Iterator<Publisher<Void>> iterator() {
				final Iterator<Registration<HttpChannel, ? extends ReactorChannelHandler<IN, OUT, HttpChannel<IN,
				  OUT>>>>
				  iterator = selected.iterator();

				return new Iterator<Publisher<Void>>() {
					@Override
					public boolean hasNext() {
						return iterator.hasNext();
					}

					@Override
					public void remove() {
						iterator.remove();
					}

					//Lazy apply
					@Override
					@SuppressWarnings("unchecked")
					public Publisher<Void> next() {
						Registration<HttpChannel, ? extends ReactorChannelHandler<IN, OUT, HttpChannel<IN, OUT>>> next
						  = iterator.next();
						if (next != null) {
							ch.paramsResolver(next.getSelector().getHeaderResolver());
							return next.getObject().apply(ch);
						} else {
							return null;
						}
					}
				};
			}
		};
	}
}
