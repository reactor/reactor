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

package reactor.nexus.pylon;

import java.net.InetSocketAddress;
import java.util.concurrent.CountDownLatch;

import org.reactivestreams.Publisher;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import reactor.Publishers;
import reactor.core.support.ReactiveStateUtils;
import reactor.fn.timer.Timer;
import reactor.io.buffer.Buffer;
import reactor.io.codec.json.JsonCodec;
import reactor.io.net.ReactiveChannel;
import reactor.io.net.ReactiveChannelHandler;
import reactor.io.net.ReactiveNet;
import reactor.io.net.ReactivePeer;
import reactor.io.net.http.HttpChannel;
import reactor.io.net.http.HttpServer;

/**
 * @author Stephane Maldini
 * @since 2.1
 */
public final class Pylon extends ReactivePeer<Buffer, Buffer, ReactiveChannel<Buffer, Buffer>> {

	private static final Logger log = LoggerFactory.getLogger(Pylon.class);

	private static final String CONSOLE_STATIC_PATH     = "/public/";
	private static final String EXIT_URL                = "/exit";
	private static final String CONSOLE_URL             = "/pylon";
	private static final String HTML_DEPENDENCY_CONSOLE = "pylon.html";

	private final HttpServer<Buffer, Buffer> server;

	public static void main(String... args) throws Exception {
		log.info("Deploying Quick Expand with a Nexus and a Pylon... ");

		//Nexus nexus = ReactiveNet.nexus();
		Pylon pylon = create(ReactiveNet.httpServer());

		final CountDownLatch stopped = new CountDownLatch(1);

		pylon.server.get(EXIT_URL, new ReactiveChannelHandler<Buffer, Buffer, HttpChannel<Buffer, Buffer>>() {
			@Override
			public Publisher<Void> apply(HttpChannel<Buffer, Buffer> channel) {
				stopped.countDown();
				return Publishers.empty();
			}
		});

		final JsonCodec<ReactiveStateUtils.Graph, ReactiveStateUtils.Graph> codec =
				new JsonCodec<>(ReactiveStateUtils.Graph.class);

		pylon.server
				.get("/nexus/stream", new ReactiveChannelHandler<Buffer, Buffer, HttpChannel<Buffer, Buffer>>() {

					@Override
					public Publisher<Void> apply(HttpChannel<Buffer, Buffer> channel) {
						return channel.writeBufferWith(codec.encode(Publishers.just(ReactiveStateUtils.scan(Publishers.merge(
								Publishers.just("a"),
								Publishers.just("b"))))));
					}
				});

		pylon.startAndAwait();

		InetSocketAddress addr = pylon.getServer()
		                              .getListenAddress();
		log.info("Quick Expand Deployed, browse http://" + addr.getHostName() + ":" + addr.getPort() + CONSOLE_URL);

		stopped.await();
	}

	/**
	 *
	 * @param server
	 * @return
	 */
	public static Pylon create(HttpServer<Buffer, Buffer> server) {

		Pylon pylon = new Pylon(server.getDefaultTimer(), server);

		log.info("Warping Pylon...");

		server.file(CONSOLE_URL,
				Pylon.class.getResource(CONSOLE_STATIC_PATH + HTML_DEPENDENCY_CONSOLE)
				           .getPath())
		      .directory(CONSOLE_URL,
				      Pylon.class.getResource(CONSOLE_STATIC_PATH)
				                 .getPath());

		return pylon;
	}

	private Pylon(Timer defaultTimer, HttpServer<Buffer, Buffer> server) {
		super(defaultTimer);
		this.server = server;
	}

	/**
	 * @see this#start(ReactiveChannelHandler)
	 */
	public final void startAndAwait() throws InterruptedException {
		Publishers.toReadQueue(start(null))
		          .take();
	}

	/**
	 * @see this#start(ReactiveChannelHandler)
	 */
	public final void start() throws InterruptedException {
		start(null);
	}

	@Override
	protected Publisher<Void> doStart(ReactiveChannelHandler<Buffer, Buffer, ReactiveChannel<Buffer, Buffer>> handler) {
		return server.start();
	}

	@Override
	protected Publisher<Void> doShutdown() {
		return server.shutdown();
	}

	public HttpServer<Buffer, Buffer> getServer() {
		return server;
	}
}
