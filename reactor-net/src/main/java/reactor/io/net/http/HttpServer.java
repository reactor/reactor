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
import reactor.bus.selector.Selectors;
import reactor.core.Dispatcher;
import reactor.fn.Function;
import reactor.io.buffer.Buffer;
import reactor.io.codec.Codec;
import reactor.io.net.NetSelectors;
import reactor.io.net.PeerStream;
import reactor.io.net.Server;
import reactor.rx.Promise;

import java.util.Iterator;

/**
 * Base functionality needed by all servers that communicate with clients over TCP.
 *
 * @param <IN>  The type that will be received by this server
 * @param <OUT> The type that will be sent by this server
 * @author Jon Brisbin
 * @author Stephane Maldini
 */
public abstract class HttpServer<IN, OUT>
		extends PeerStream<IN, OUT, ServerRequest<IN, OUT>>
		implements Server<IN, OUT, ServerRequest<IN, OUT>> {

	protected final Registry<Function<ServerRequest<IN, OUT>, ? extends Publisher<? extends OUT>>> routedWriters;

	protected HttpServer(Environment env, Dispatcher dispatcher, Codec<Buffer, IN, OUT> codec) {
		super(env, dispatcher, codec);
		this.routedWriters = Registries.create();
	}

	/**
	 * @param condition
	 * @param serviceFunction
	 * @return
	 */
	@SuppressWarnings("unchecked")
	public HttpServer<IN, OUT> route(
			final Selector condition,
			final Function<ServerRequest<IN, OUT>, ? extends Publisher<? extends OUT>> serviceFunction) {

		routedWriters.register(condition, serviceFunction);
		return this;
	}


	@Override
	public Server<IN, OUT, ServerRequest<IN, OUT>> pipeline(
			final Function<ServerRequest<IN, OUT>, ? extends Publisher<? extends OUT>> serviceFunction) {
		route(Selectors.matchAll(), serviceFunction);
		return this;
	}

	/**
	 * Start this server.
	 *
	 * @return {@literal promise} fulfilling when started
	 */
	@Override
	public abstract Promise<Boolean> start();

	@Override
	public abstract Promise<Boolean> shutdown();

	/**
	 * @param path
	 * @param handler
	 * @return
	 */
	public final HttpServer<IN, OUT> get(String path,
	                                     final Function<ServerRequest<IN, OUT>, ? extends Publisher<? extends OUT>>
			                                     handler) {
		route(NetSelectors.get(path), handler);
		return this;
	}

	/**
	 * @param path
	 * @param handler
	 * @return
	 */
	public final HttpServer<IN, OUT> post(String path,
	                                      final Function<ServerRequest<IN, OUT>, ? extends Publisher<? extends OUT>>
			                                      handler) {
		route(NetSelectors.post(path), handler);
		return this;
	}


	/**
	 * @param path
	 * @param handler
	 * @return
	 */
	public final HttpServer<IN, OUT> put(String path,
	                                     final Function<ServerRequest<IN, OUT>, ? extends Publisher<? extends OUT>>
			                                     handler) {
		route(NetSelectors.put(path), handler);
		return this;
	}

	/**
	 * @param path
	 * @param handler
	 * @return
	 */
	public final HttpServer<IN, OUT> delete(String path,
	                                        final Function<ServerRequest<IN, OUT>, ? extends Publisher<? extends OUT>>
			                                        handler) {
		route(NetSelectors.delete(path), handler);
		return this;
	}

	@Override
	protected Iterable<Publisher<? extends OUT>> routeChannel(final ServerRequest<IN, OUT> ch) {
		return new Iterable<Publisher<? extends OUT>>() {
			@Override
			public Iterator<Publisher<? extends OUT>> iterator() {
				final Iterator<Registration<? extends Function<ServerRequest<IN, OUT>, ? extends Publisher<? extends OUT>>>>
						iterator
						= routedWriters.select(ch).iterator();

				return new Iterator<Publisher<? extends OUT>>() {
					@Override
					public boolean hasNext() {
						return iterator.hasNext();
					}

					//Lazy apply
					@Override
					@SuppressWarnings("unchecked")
					public Publisher<? extends OUT> next() {
						Registration<? extends Function<ServerRequest<IN, OUT>, ? extends Publisher<? extends OUT>>> next
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
