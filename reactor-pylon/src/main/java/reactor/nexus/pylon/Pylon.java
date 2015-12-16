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
import java.net.URI;
import java.net.URISyntaxException;
import java.nio.file.Files;
import java.security.CodeSource;
import java.security.ProtectionDomain;
import java.util.Enumeration;
import java.util.concurrent.CountDownLatch;
import java.util.jar.JarEntry;
import java.util.jar.JarFile;

import org.reactivestreams.Publisher;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import reactor.Publishers;
import reactor.fn.Function;
import reactor.core.timer.Timer;
import reactor.io.IO;
import reactor.io.buffer.Buffer;
import reactor.io.net.ReactiveChannel;
import reactor.io.net.ReactiveChannelHandler;
import reactor.io.net.ReactiveNet;
import reactor.io.net.ReactivePeer;
import reactor.io.net.http.HttpChannel;
import reactor.io.net.http.HttpServer;
import reactor.io.net.http.model.ResponseHeaders;
import reactor.io.net.http.routing.ChannelMappings;

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
	private static final String CACHE_MANIFEST             = "/index.appcache";

	private final HttpServer<Buffer, Buffer> server;
	private final String                     staticPath;

	public static void main(String... args) throws Exception {
		String port = System.getenv("PORT");
		String address = args.length > 0 ? args[0] : "0.0.0.0";
		Pylon pylon = create(ReactiveNet.httpServer(address, port != null ? Integer.parseInt(port) : 12013),
				extractAssets() );

		final CountDownLatch stopped = new CountDownLatch(1);

		pylon.startAndAwait();

		log.info("CTRL-C to return...");

		stopped.await();
	}

	/**
	 *
	 * @return
	 */
	public static Pylon create() throws Exception {
		return create(ReactiveNet.httpServer(12013));
	}

	/**
	 *
	 * @param server
	 * @return
	 */
	public static Pylon create(HttpServer<Buffer, Buffer> server) throws Exception {
		return create(server, findOrExtractAssets());
	}

	/**
	 *
	 * @param server
	 * @param staticPath
	 * @return
	 * @throws Exception
	 */
	public static Pylon create(HttpServer<Buffer, Buffer> server, String staticPath) throws Exception {

		Pylon pylon = new Pylon(server.getDefaultTimer(), server, staticPath);

		log.info("Warping Pylon...");

		final Publisher<Buffer> cacheManifest = IO.readFile(pylon.pathToStatic(CACHE_MANIFEST));

		server.file(CONSOLE_FAVICON, pylon.pathToStatic(CONSOLE_FAVICON))
		      .get(CACHE_MANIFEST, new CacheManifestHandler(cacheManifest))
		      .file(ChannelMappings.prefix(CONSOLE_URL), pylon.pathToStatic(HTML_DEPENDENCY_CONSOLE), null)
		      .directory(CONSOLE_ASSETS_PREFIX,
				      pylon.pathToStatic(CONSOLE_STATIC_ASSETS_PATH), new AssetsInterceptor());

		return pylon;
	}

	private static String findOrExtractAssets() throws Exception {
		if (Pylon.class.getResource(CONSOLE_STATIC_PATH + HTML_DEPENDENCY_CONSOLE)
		               .getPath()
		               .contains("jar!/")) {
			return extractAssets();
		}
		else {
			return Pylon.class.getResource(CONSOLE_STATIC_PATH)
			                  .getPath();
		}
	}

	private static String extractAssets() throws Exception{
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

		log.info("Sending scouting probes to : " + dest);
		return deployStaticFiles(dest.toString()) + CONSOLE_STATIC_PATH;
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
		InetSocketAddress addr = server.getListenAddress();
		log.info("Pylon Warped. Troops can receive signal under http://" + addr.getHostName() + ":" + addr.getPort() +
				CONSOLE_URL);
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
	public static String deployStaticFiles(String destDir) throws IOException, URISyntaxException {



		ProtectionDomain protectionDomain = Pylon.class.getProtectionDomain();
		CodeSource codeSource = protectionDomain.getCodeSource();
		URI location = (codeSource == null ? null : codeSource.getLocation().toURI());
		String path = (location == null ? null : location.getSchemeSpecificPart());
		if (path == null) {
			throw new IllegalStateException("Unable to determine code source archive");
		}
		File root = new File(path);
		if (!root.exists()) {
			throw new IllegalStateException(
					"Unable to determine code source archive from " + root);
		}

		if(root.isDirectory()){
			return root.getAbsolutePath();
		}

		JarFile jar = new JarFile(root);
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

		return destDir;
	}

	private static class CacheManifestHandler
			implements ReactiveChannelHandler<Buffer, Buffer, HttpChannel<Buffer, Buffer>> {

		private final Publisher<Buffer> cacheManifest;

		public CacheManifestHandler(Publisher<Buffer> cacheManifest) {
			this.cacheManifest = cacheManifest;
		}

		@Override
		public Publisher<Void> apply(HttpChannel<Buffer, Buffer> channel) {

			return channel.responseHeader("content-type", "text/cache-manifest")
			              .writeBufferWith(cacheManifest);
		}
	}

	private static class AssetsInterceptor
			implements Function<HttpChannel<Buffer, Buffer>, HttpChannel<Buffer, Buffer>> {

		@Override
		public HttpChannel<Buffer, Buffer> apply(HttpChannel<Buffer, Buffer> channel) {

			if(channel.uri().endsWith(".css")){
				channel.responseHeader(ResponseHeaders.CONTENT_TYPE, "text/css; charset=utf-8");
			}
			else if(channel.uri().endsWith(".js")){
				channel.responseHeader(ResponseHeaders.CONTENT_TYPE, "text/javascript; charset=utf-8");
			}

			return channel;
		}
	}
}
