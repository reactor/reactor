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

package reactor.io.net.http;

import java.net.InetSocketAddress;

import reactor.fn.Predicate;
import reactor.io.net.ReactiveChannelHandler;
import reactor.io.net.ReactivePeer;
import reactor.io.net.ReactorPeer;
import reactor.io.net.http.routing.ChannelMappings;
import reactor.rx.Promise;
import reactor.rx.Promises;

/**
 * Base functionality needed by all servers that communicate with clients over HTTP.
 *
 * @param <IN>  The type that will be received by this server
 * @param <OUT> The type that will be sent by this server
 *
 * @author Stephane Maldini
 * @since 2.1
 */
public final class ReactorHttpServer<IN, OUT> extends ReactorPeer<IN, OUT, HttpServer<IN,OUT>>{

	public static <IN, OUT> ReactorHttpServer<IN,OUT> create(HttpServer<IN, OUT> server) {
		return new ReactorHttpServer<>(server);
	}

	protected ReactorHttpServer(HttpServer<IN, OUT> server) {
		super(server);
	}

	/**
	 * Start this {@literal ReactorPeer}.
	 * @return a {@link Promise<Void>} that will be complete when the {@link
	 * ReactivePeer} is started
	 */
	public Promise<Void> start(ReactiveChannelHandler<IN, OUT, HttpChannelStream<IN, OUT>> handler) {
		return Promises.from(peer.start(
				HttpChannelStream.wrapHttp(handler, peer.getDefaultTimer(), peer.getDefaultPrefetchSize())
		));
	}

	/**
	 * Start the server without any global handler, only the specific routed methods (get, post...) will apply.
	 *
	 * @return a Promise fulfilled when server is started
	 */
	public Promise<Void> start() {
		return Promises.from(peer.start(null));
	}

	/**
	 * Start this {@literal ReactorPeer}.
	 * @return a {@link Promise<Void>} that will be complete when the {@link
	 * ReactivePeer} is started
	 */
	public Promise<Void> start(ReactorHttpHandler<IN, OUT> handler) {
		return Promises.from(peer.start(
				HttpChannelStream.wrapHttp(handler, peer.getDefaultTimer(), peer.getDefaultPrefetchSize())
		));
	}

	/**
	 * Shutdown this {@literal ReactorPeer} and complete the returned {@link Promise<Void>}
	 * when shut down.
	 * @return a {@link Promise<Void>} that will be complete when the {@link
	 * ReactivePeer} is shutdown
	 */
	public Promise<Void> shutdown() {
		return Promises.from(peer.shutdown());
	}

	/**
	 * Get the address to which this server is bound. If port 0 was used on configuration, try resolving the port.
	 *
	 * @return the bind address
	 */
	public InetSocketAddress getListenAddress(){
		return peer.getListenAddress();
	}


	/**
	 * Register an handler for the given Selector condition, incoming connections will query the internal registry
	 * to invoke the matching handlers. Implementation may choose to reply 404 if no route matches.
	 *
	 * @param condition       a {@link Predicate} to match the incoming connection with registered handler
	 * @param serviceFunction an handler to invoke for the given condition
	 * @return {@code this}
	 */
	@SuppressWarnings("unchecked")
	public ReactorHttpServer<IN, OUT> route(
	  final Predicate<HttpChannel> condition,
	 final ReactorHttpHandler<IN, OUT> serviceFunction) {
		peer.route(condition, HttpChannelStream.wrapHttp(serviceFunction, peer.getDefaultTimer(), peer.getDefaultPrefetchSize()));
		return this;
	}

	/**
	 * Listen for HTTP GET on the passed path to be used as a routing condition. Incoming connections will query the
	 * internal registry
	 * to invoke the matching handlers.
	 * <p>
	 * e.g. "/test/{param}". Params are resolved using {@link HttpChannel#param(String)}
	 *
	 * @param path    The {@link ChannelMappings.HttpPredicate} to resolve against this path, pattern matching and capture are supported
	 * @param handler an handler to invoke for the given condition
	 * @return {@code this}
	 */
	public final ReactorHttpServer<IN, OUT> get(String path,
	                                    final ReactorHttpHandler<IN, OUT> handler) {
		peer.get(path, HttpChannelStream.wrapHttp(handler, peer.getDefaultTimer(), peer.getDefaultPrefetchSize()));
		return this;
	}

	/**
	 * Listen for HTTP POST on the passed path to be used as a routing condition. Incoming connections will query the
	 * internal registry
	 * to invoke the matching handlers.
	 * <p>
	 * e.g. "/test/{param}". Params are resolved using {@link HttpChannel#param(String)}
	 *
	 * @param path    The {@link ChannelMappings.HttpPredicate} to resolve against this path, pattern matching and capture are supported
	 * @param handler an handler to invoke for the given condition
	 * @return {@code this}
	 */
	public final ReactorHttpServer<IN, OUT> post(String path,
	                                     final ReactorHttpHandler<IN, OUT> handler) {
		peer.post(path, HttpChannelStream.wrapHttp(handler, peer.getDefaultTimer(), peer.getDefaultPrefetchSize()));
		return this;
	}


	/**
	 * Listen for HTTP PUT on the passed path to be used as a routing condition. Incoming connections will query the
	 * internal registry
	 * to invoke the matching handlers.
	 * <p>
	 * e.g. "/test/{param}". Params are resolved using {@link HttpChannel#param(String)}
	 *
	 * @param path    The {@link ChannelMappings.HttpPredicate} to resolve against this path, pattern matching and capture are supported
	 * @param handler an handler to invoke for the given condition
	 * @return {@code this}
	 */
	public final ReactorHttpServer<IN, OUT> put(String path,
	                                    final ReactorHttpHandler<IN, OUT> handler) {
		peer.put(path, HttpChannelStream.wrapHttp(handler, peer.getDefaultTimer(), peer.getDefaultPrefetchSize()));
		return this;
	}


	/**
	 * Listen for WebSocket on the passed path to be used as a routing condition. Incoming connections will query the
	 * internal registry
	 * to invoke the matching handlers.
	 * <p>
	 * e.g. "/test/{param}". Params are resolved using {@link HttpChannel#param(String)}
	 *
	 * @param path    The {@link ChannelMappings.HttpPredicate} to resolve against this path, pattern matching and capture are supported
	 * @param handler an handler to invoke for the given condition
	 * @return {@code this}
	 */
	public final ReactorHttpServer<IN, OUT> ws(String path,
	                                    final ReactorHttpHandler<IN, OUT> handler) {
		peer.ws(path, HttpChannelStream.wrapHttp(handler, peer.getDefaultTimer(), peer.getDefaultPrefetchSize()));
		return this;
	}

	/**
	 * Listen for HTTP DELETE on the passed path to be used as a routing condition. Incoming connections will query
	 * the internal registry
	 * to invoke the matching handlers.
	 * <p>
	 * e.g. "/test/{param}". Params are resolved using {@link HttpChannel#param(String)}
	 *
	 * @param path    The {@link ChannelMappings.HttpPredicate} to resolve against this path, pattern matching and capture are supported
	 * @param handler an handler to invoke for the given condition
	 * @return {@code this}
	 */
	public final ReactorHttpServer<IN, OUT> delete(String path,
	                                       final ReactorHttpHandler<IN, OUT> handler) {
		peer.delete(path, HttpChannelStream.wrapHttp(handler, peer.getDefaultTimer(), peer.getDefaultPrefetchSize()));
		return this;
	}

}
