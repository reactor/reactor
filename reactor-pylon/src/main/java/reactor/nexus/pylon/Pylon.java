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

import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.net.InetSocketAddress;
import java.net.URISyntaxException;
import java.nio.file.Files;
import java.util.Enumeration;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.jar.JarEntry;
import java.util.jar.JarFile;

import org.reactivestreams.Publisher;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import reactor.Processors;
import reactor.Publishers;
import reactor.Subscribers;
import reactor.Timers;
import reactor.core.error.CancelException;
import reactor.core.processor.BaseProcessor;
import reactor.core.processor.ProcessorGroup;
import reactor.core.subscription.ReactiveSession;
import reactor.core.support.ReactiveStateUtils;
import reactor.fn.Consumer;
import reactor.fn.timer.Timer;
import reactor.io.buffer.Buffer;
import reactor.io.codec.json.JsonCodec;
import reactor.io.net.ReactiveChannel;
import reactor.io.net.ReactiveChannelHandler;
import reactor.io.net.ReactiveNet;
import reactor.io.net.ReactivePeer;
import reactor.io.net.http.HttpChannel;
import reactor.io.net.http.HttpServer;
import reactor.io.net.http.routing.ChannelMappings;
import reactor.io.net.impl.netty.http.NettyHttpServer;

/**
 * @author Stephane Maldini
 * @since 2.1
 */
public final class Pylon extends ReactivePeer<Buffer, Buffer, ReactiveChannel<Buffer, Buffer>> {

	private static final Logger log = LoggerFactory.getLogger(Pylon.class);

	private static final String CONSOLE_STATIC_PATH        = "/public";
	private static final String CONSOLE_STATIC_ASSETS_PATH = "/assets";
	private static final String CONSOLE_URL                = "/pylon";
	private static final String CONSOLE_ASSETS_PREFIX      = "/assets";
	private static final String CONSOLE_FAVICON            = "/favicon.ico";
	private static final String HTML_DEPENDENCY_CONSOLE    = "/index.html";

	private final HttpServer<Buffer, Buffer> server;
	private final String                     staticPath;

	private final BaseProcessor<ReactiveStateUtils.Graph, ReactiveStateUtils.Graph> graphStream =
			Processors.emitter(false);

	private final ReactiveStateUtils.Graph                                          lastState   =
			ReactiveStateUtils.newGraph();

	public static void main(String... args) throws Exception {
		log.info("Deploying Quick Expand with a Nexus and a Pylon... ");

		//Nexus nexus = ReactiveNet.nexus();

		Pylon pylon = create(ReactiveNet.httpServer());

		//EXAMPLE
		final CountDownLatch stopped = new CountDownLatch(1);
		final BaseProcessor p4 = Processors.emitter();

		pylon.server.get("/exit", channel -> {
			stopped.countDown();
			return Publishers.empty();
		})
		            .get("/nexus/stream", new ReactiveChannelHandler<Buffer, Buffer, HttpChannel<Buffer, Buffer>>() {
			            final JsonCodec<ReactiveStateUtils.Graph, ReactiveStateUtils.Graph> codec =
					            new JsonCodec<>(ReactiveStateUtils.Graph.class);

			            @Override
			            public Publisher<Void> apply(final HttpChannel<Buffer, Buffer> channel) {
				            channel.responseHeader("Access-Control-Allow-Origin", "*");

				            Publisher<Void> p;
				            if (channel.isWebsocket()) {
					            p = Publishers.concat(NettyHttpServer.upgradeToWebsocket(channel),
							            channel.writeBufferWith(codec.encode(Publishers.capacity(pylon.graphStream,
									            1L))));
				            }
				            else {
					            p = channel.writeBufferWith(codec.encode(Publishers.capacity(pylon.graphStream, 1L)));
				            }

				            return p;

//				            BaseProcessor p = Processors.replay();
//				            BaseProcessor p2 = Processors.emitter();
//				            //channel.input().subscribe(p2);
//				            BaseProcessor p3 = Processors.emitter();
//				            p.subscribe(p3);
//				            ProcessorGroup group = Processors.singleGroup();
//				            p3.dispatchOn(group)
//				              .subscribe(Subscribers.unbounded());
//				            p3.subscribe(Subscribers.unbounded());
//				            p3.subscribe(Subscribers.unbounded());
//				            BaseProcessor p4 = Processors.emitter();
//				            Publishers.zip(Publishers.log(p4), Publishers.timestamp(Publishers.just(1)))
//				                      .subscribe(p2);
//				            p2.dispatchOn(group)
//				              .subscribe(Subscribers.unbounded());
//				            channel.input()
//				                   .subscribe(p4);
//				            ReactiveSession s = p.startSession();
//
//				            Publisher<Void> p5 = channel.writeBufferWith(codec.encode(p));
//				            s.submit(ReactiveStateUtils.scan(p5));
//				            s.finish();
//
//				            return p5;
			            }
		            });

		final ReactiveSession<ReactiveStateUtils.Graph> s = pylon.graphStream.startSession();

		BaseProcessor p = Processors.replay();
		BaseProcessor p2 = Processors.emitter();
		//channel.input().subscribe(p2);
		BaseProcessor p3 = Processors.emitter();
		p.subscribe(p3);
		ProcessorGroup group = Processors.singleGroup();
		p3.dispatchOn(group)
		  .subscribe(Subscribers.unbounded());
		p3.subscribe(Subscribers.unbounded());
		p3.subscribe(Subscribers.unbounded());
		p.startSession();
		Publishers.zip(Publishers.log(p4), Publishers.timestamp(Publishers.just(1)))
		          .subscribe(p2);

		p2.dispatchOn(group)
		  .subscribe(Subscribers.unbounded());

		Timers.create()
		      .schedule(new Consumer<Long>() {
			      @Override
			      public void accept(Long aLong) {
				      if (!s.isCancelled()) {
					      pylon.lastState.mergeWith(ReactiveStateUtils.scan(s));
					      s.submit(pylon.lastState);
				      }
				      else {
					      throw CancelException.get();
				      }
			      }
		      }, 200, TimeUnit.MILLISECONDS);

		//EXAMPLE END

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
	public static Pylon create(HttpServer<Buffer, Buffer> server) throws Exception {

		String staticPath = findOrExtractAssets();

		Pylon pylon = new Pylon(server.getDefaultTimer(), server, staticPath);

		log.info("Warping Pylon...");

		server.file(ChannelMappings.prefix("/pylon"), pylon.pathToStatic(HTML_DEPENDENCY_CONSOLE))
		      .file(CONSOLE_FAVICON, pylon.pathToStatic(CONSOLE_FAVICON))
		      .directory(CONSOLE_ASSETS_PREFIX, pylon.pathToStatic(CONSOLE_STATIC_ASSETS_PATH));

		return pylon;
	}

	private static String findOrExtractAssets() throws Exception {
		if (Pylon.class.getResource(CONSOLE_STATIC_PATH + HTML_DEPENDENCY_CONSOLE)
		               .getPath()
		               .contains("jar!/")) {
			final File dest = Files.createTempDirectory("reactor-pylon")
			                       .toFile();

			dest.deleteOnExit();

			Runtime.getRuntime()
			       .addShutdownHook(new Thread() {

				       @Override
				       public void run() {
					       if (dest.delete()) {
						       log.info("Probes called back from temporary zone");
					       }
				       }
			       });

			log.info("Sending Assets probes to : " + dest);
			deployStaticFiles(dest.toString());
			return dest.toString() + CONSOLE_STATIC_PATH;
		}
		else {
			return Pylon.class.getResource(CONSOLE_STATIC_PATH)
			                  .getPath();
		}
	}

	private String pathToStatic(String target) {
		return staticPath + target;
	}

	private Pylon(Timer defaultTimer, HttpServer<Buffer, Buffer> server, String staticPath) {
		super(defaultTimer);
		this.staticPath = staticPath;
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

	/**
	 *
	 * @param destDir
	 */
	public static void deployStaticFiles(String destDir) throws IOException, URISyntaxException {

		JarFile jar = new JarFile(new File(Pylon.class.getProtectionDomain()
		                                              .getCodeSource()
		                                              .getLocation()
		                                              .toURI()
		                                              .getPath()));
		Enumeration enumEntries = jar.entries();
		final String prefix = CONSOLE_STATIC_PATH.substring(1);
		while (enumEntries.hasMoreElements()) {
			JarEntry file = (JarEntry) enumEntries.nextElement();
			if (!file.getName()
			         .startsWith(prefix)) {
				continue;
			}
			File f = new File(destDir + File.separator + file.getName());
			if (file.isDirectory()) {
				f.mkdir();
				continue;
			}
			InputStream is = jar.getInputStream(file); // get the input stream
			FileOutputStream fos = new FileOutputStream(f);
			while (is.available() > 0) {
				fos.write(is.read());
			}
			fos.close();
			is.close();
		}
	}
}
