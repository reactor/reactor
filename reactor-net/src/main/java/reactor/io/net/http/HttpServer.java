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
import reactor.core.Dispatcher;
import reactor.io.buffer.Buffer;
import reactor.io.codec.Codec;
import reactor.io.net.NetSelectors;
import reactor.io.net.ReactorChannelHandler;
import reactor.io.net.ReactorPeer;
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

	protected final Registry<ReactorChannelHandler<IN, OUT, HttpChannel<IN, OUT>>> routedWriters;

	protected HttpServer(Environment env, Dispatcher dispatcher, Codec<Buffer, IN, OUT> codec) {
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
	 * Get the address to which this server is bound.
	 *
	 * @return
	 */
	public abstract InetSocketAddress getListenAddress();

	/**
	 * @param condition
	 * @param serviceFunction
	 * @return
	 */
	@SuppressWarnings("unchecked")
	public HttpServer<IN, OUT> route(
			final Selector condition,
			final ReactorChannelHandler<IN, OUT, HttpChannel<IN, OUT>> serviceFunction) {

		routedWriters.register(condition, serviceFunction);
		return this;
	}

	/**
	 * @param path
	 * @param handler
	 * @return
	 */
	public final HttpServer<IN, OUT> get(String path,
	                                     final ReactorChannelHandler<IN, OUT, HttpChannel<IN, OUT>> handler) {
		route(NetSelectors.get(path), handler);
		return this;
	}

	/**
	 * @param path
	 * @param handler
	 * @return
	 */
	public final HttpServer<IN, OUT> post(String path,
	                                      final ReactorChannelHandler<IN, OUT, HttpChannel<IN, OUT>> handler) {
		route(NetSelectors.post(path), handler);
		return this;
	}


	/**
	 * @param path
	 * @param handler
	 * @return
	 */
	public final HttpServer<IN, OUT> put(String path,
	                                     final ReactorChannelHandler<IN, OUT, HttpChannel<IN, OUT>> handler) {
		route(NetSelectors.put(path), handler);
		return this;
	}

	/**
	 * @param path
	 * @param handler
	 * @return
	 */
	public final HttpServer<IN, OUT> delete(String path,
	                                        final ReactorChannelHandler<IN, OUT, HttpChannel<IN, OUT>> handler) {
		route(NetSelectors.delete(path), handler);
		return this;
	}

	protected Iterable<? extends Publisher<Void>> routeChannel(final HttpChannel<IN, OUT> ch) {
		final List<Registration<? extends ReactorChannelHandler<IN, OUT, HttpChannel<IN, OUT>>>>
				selected = routedWriters.select(ch);

		return new Iterable<Publisher<Void>>() {
			@Override
			public Iterator<Publisher<Void>> iterator() {
				final Iterator<Registration<? extends ReactorChannelHandler<IN, OUT, HttpChannel<IN, OUT>>>>
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
						Registration<? extends ReactorChannelHandler<IN, OUT, HttpChannel<IN, OUT>>> next = iterator.next();
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
