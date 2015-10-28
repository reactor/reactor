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
package reactor.io.net;

import reactor.core.publisher.convert.DependencyUtils;
import reactor.core.support.Assert;
import reactor.fn.Function;
import reactor.io.buffer.Buffer;
import reactor.io.net.http.HttpClient;
import reactor.io.net.http.HttpServer;
import reactor.io.net.http.ReactorHttpClient;
import reactor.io.net.http.ReactorHttpServer;
import reactor.io.net.tcp.ReactorTcpClient;
import reactor.io.net.tcp.ReactorTcpServer;
import reactor.io.net.tcp.TcpClient;
import reactor.io.net.tcp.TcpServer;
import reactor.io.net.udp.DatagramServer;
import reactor.io.net.udp.ReactorDatagramServer;

/**
 * A Streams add-on to work with network facilities from reactor-net, e.g.:
 * <p>
 * <pre>
 * {@code
 * //echo server
 * NetStreams.tcpServer(1234).start( connection -> ch.writeWith(connection) );
 *
 * NetStreams.tcpClient(1234).start( connection ->
 *    connection
 *      //Listen for any incoming data on that connection, they will be Buffer an IOStream can easily decode
 *      .nest()
 *      .flatMap(self -> IOStreams.decode(new StringCodec('\n'), self))
 *      .consume(log::info);
 *
 *    //Push anything from the publisher returned, here a simple Reactor Stream. By default a Buffer is expected
 *    //Will close after write
 *    return connection.writeWith(Streams.just(Buffer.wrap("hello\n")));
 * });
 *
 * //We can also preconfigure global codecs and other custom client/server parameter with the Function signature:
 * NetStreams.tcpServer(spec -> spec.codec(kryoCodec).listen(1235)).start( intput -> {
 *      input.consume(log::info);
 *      return input.writeWith(Streams.period(1l));
 * });
 *
 * //Assigning the same codec to a client and a server greatly improve readability and provide for extended type safety.
 * NetStreams.tcpClient(spec -> spec.connect("localhost", 1235).codec(kryoCodec)).start( input -> {
 *   input.consume(log::info);
 *   return input.writeWith(Streams.just("hello"));
 * });
 *
 * }
 * </pre>
 *
 * @author Stephane Maldini
 */
public class NetStreams {

	static {
		if (!DependencyUtils.hasReactorNet()) {
			throw new IllegalStateException("io.projectreactor:reactor-net:" + DependencyUtils.reactorVersion() +
					" dependency is missing from the classpath.");
		}

	}

	private NetStreams() {
	}

	// TCP

	/**
	 * Bind a new TCP server to "loopback" on port {@literal 12012}. By default the default server implementation is
	 * scanned
	 * from the classpath on Class init. Support for Netty first and ZeroMQ then is provided as long as the relevant
	 * library dependencies are on the classpath.
	 * <p>
	 * To reply data on the active connection, {@link ReactiveChannel#writeWith} can subscribe to any passed {@link org
	 * .reactivestreams.Publisher}.
	 * <p>
	 * Note that {@link reactor.rx.Stream#getCapacity} will be used to switch on/off a channel in auto-read / flush on
	 * write mode.
	 * If the capacity is Long.MAX_Value, write on flush and auto read will apply. Otherwise, data will be flushed
	 * every
	 * capacity batch size and read will pause when capacity number of elements have been dispatched.
	 * <p>
	 * Emitted channels will run on the same thread they have beem receiving IO events.
	 * Apart from dispatching the write, it is possible to use {@link reactor.rx.Stream#process} to process requests
	 * asynchronously.
	 * <p>
	 * By default the type of emitted data or received data is {@link Buffer}
	 *
	 * @return a new Stream of ChannelStream, typically a peer of connections.
	 */
	public static ReactorTcpServer<Buffer, Buffer> tcpServer() {
		return  ReactorTcpServer.create(ReactiveNet.tcpServer());
	}

	/**
	 * Bind a new TCP server to "loopback" on the given port. By default the default server implementation is scanned
	 * from the classpath on Class init. Support for Netty first and ZeroMQ then is provided as long as the relevant
	 * library dependencies are on the classpath.
	 * <p>
	 * A {@link reactor.io.net.tcp.TcpServer} is a specific kind of {@link org.reactivestreams.Publisher} that will
	 * emit:
	 * - onNext {@link ChannelStream} to consume data from
	 * - onComplete when server is shutdown
	 * - onError when any error (more specifically IO error) occurs
	 * From the emitted {@link ReactiveChannel}, one can decide to add in-channel consumers to read any incoming
	 * data.
	 * <p>
	 * To reply data on the active connection, {@link ReactiveChannel#writeWith} can subscribe to any passed {@link org
	 * .reactivestreams.Publisher}.
	 * <p>
	 * Note that {@link reactor.rx.Stream#getCapacity} will be used to switch on/off a channel in auto-read / flush on
	 * write mode.
	 * If the capacity is Long.MAX_Value, write on flush and auto read will apply. Otherwise, data will be flushed
	 * every
	 * capacity batch size and read will pause when capacity number of elements have been dispatched.
	 * <p>
	 * Emitted channels will run on the same thread they have beem receiving IO events.
	 * Apart from dispatching the write, it is possible to use {@link reactor.rx.Stream#process} to process requests
	 * asynchronously.
	 * <p>
	 * By default the type of emitted data or received data is {@link Buffer}
	 *
	 * @param port the port to listen on loopback
	 * @return a new Stream of ChannelStream, typically a peer of connections.
	 */
	public static ReactorTcpServer<Buffer, Buffer> tcpServer(int port) {
		return  ReactorTcpServer.create(ReactiveNet.tcpServer(port));
	}

	/**
	 * Bind a new TCP server to the given bind address on port {@literal 12012}. By default the default server
	 * implementation is scanned
	 * from the classpath on Class init. Support for Netty first and ZeroMQ then is provided as long as the relevant
	 * library dependencies are on the classpath.
	 * <p>
	 * A {@link reactor.io.net.tcp.TcpServer} is a specific kind of {@link org.reactivestreams.Publisher} that will
	 * emit:
	 * - onNext {@link ChannelStream} to consume data from
	 * - onComplete when server is shutdown
	 * - onError when any error (more specifically IO error) occurs
	 * From the emitted {@link ReactiveChannel}, one can decide to add in-channel consumers to read any incoming
	 * data.
	 * <p>
	 * To reply data on the active connection, {@link ReactiveChannel#writeWith} can subscribe to any passed {@link org
	 * .reactivestreams.Publisher}.
	 * <p>
	 * Note that {@link reactor.rx.Stream#getCapacity} will be used to switch on/off a channel in auto-read / flush on
	 * write mode.
	 * If the capacity is Long.MAX_Value, write on flush and auto read will apply. Otherwise, data will be flushed
	 * every
	 * capacity batch size and read will pause when capacity number of elements have been dispatched.
	 * <p>
	 * Emitted channels will run on the same thread they have beem receiving IO events.
	 * Apart from dispatching the write, it is possible to use {@link reactor.rx.Stream#process} to process requests
	 * asynchronously.
	 * <p>
	 * By default the type of emitted data or received data is {@link Buffer}
	 *
	 * @param bindAddress bind address (e.g. "127.0.0.1") to create the server on the default port 12012
	 * @return a new Stream of ChannelStream, typically a peer of connections.
	 */
	public static ReactorTcpServer<Buffer, Buffer> tcpServer(String bindAddress) {
		return  ReactorTcpServer.create(ReactiveNet.tcpServer(bindAddress));
	}

	/**
	 * Bind a new TCP server to the given bind address and port. By default the default server implementation is
	 * scanned
	 * from the classpath on Class init. Support for Netty first and ZeroMQ then is provided as long as the relevant
	 * library dependencies are on the classpath.
	 * <p>
	 * A {@link reactor.io.net.tcp.TcpServer} is a specific kind of {@link org.reactivestreams.Publisher} that will
	 * emit:
	 * - onNext {@link ChannelStream} to consume data from
	 * - onComplete when server is shutdown
	 * - onError when any error (more specifically IO error) occurs
	 * From the emitted {@link ReactiveChannel}, one can decide to add in-channel consumers to read any incoming
	 * data.
	 * <p>
	 * To reply data on the active connection, {@link ReactiveChannel#writeWith} can subscribe to any passed {@link org
	 * .reactivestreams.Publisher}.
	 * <p>
	 * Note that {@link reactor.rx.Stream#getCapacity} will be used to switch on/off a channel in auto-read / flush on
	 * write mode.
	 * If the capacity is Long.MAX_Value, write on flush and auto read will apply. Otherwise, data will be flushed
	 * every
	 * capacity batch size and read will pause when capacity number of elements have been dispatched.
	 * <p>
	 * Emitted channels will run on the same thread they have beem receiving IO events.
	 * Apart from dispatching the write, it is possible to use {@link reactor.rx.Stream#process} to process requests
	 * asynchronously.
	 * <p>
	 * By default the type of emitted data or received data is {@link Buffer}
	 *
	 * @param port        the port to listen on the passed bind address
	 * @param bindAddress bind address (e.g. "127.0.0.1") to create the server on the passed port
	 * @return a new Stream of ChannelStream, typically a peer of connections.
	 */
	public static ReactorTcpServer<Buffer, Buffer> tcpServer(final String bindAddress, final int port) {
		return  ReactorTcpServer.create(ReactiveNet.tcpServer(bindAddress, port));
	}

	/**
	 * Bind a new TCP server to the specified bind address and port. By default the default server implementation is
	 * scanned
	 * from the classpath on Class init. Support for Netty first and ZeroMQ then is provided as long as the relevant
	 * library dependencies are on the classpath.
	 * <p>
	 * A {@link reactor.io.net.tcp.TcpServer} is a specific kind of {@link org.reactivestreams.Publisher} that will
	 * emit:
	 * - onNext {@link ChannelStream} to consume data from
	 * - onComplete when server is shutdown
	 * - onError when any error (more specifically IO error) occurs
	 * From the emitted {@link ReactiveChannel}, one can decide to add in-channel consumers to read any incoming
	 * data.
	 * <p>
	 * To reply data on the active connection, {@link ReactiveChannel#writeWith} can subscribe to any passed {@link org
	 * .reactivestreams.Publisher}.
	 * <p>
	 * Note that {@link reactor.rx.Stream#getCapacity} will be used to switch on/off a channel in auto-read / flush on
	 * write mode.
	 * If the capacity is Long.MAX_Value, write on flush and auto read will apply. Otherwise, data will be flushed
	 * every
	 * capacity batch size and read will pause when capacity number of elements have been dispatched.
	 * <p>
	 * Emitted channels will run on the same thread they have beem receiving IO events.
	 * Apart from dispatching the write, it is possible to use {@link reactor.rx.Stream#process} to process requests
	 * asynchronously.
	 * <p>
	 * By default the type of emitted data or received data is {@link Buffer}
	 *
	 * @param configuringFunction a function will apply and return a {@link reactor.io.net.Spec} to customize the peer
	 * @param <IN>                the given input type received by this peer. Any configured codec decoder must match
	 *                            this type.
	 * @param <OUT>               the given output type received by this peer. Any configured codec encoder must match
	 *                            this type.
	 * @return a new Stream of ChannelStream, typically a peer of connections.
	 */
	public static <IN, OUT> ReactorTcpServer<IN, OUT> tcpServer(
	  Function<? super Spec.TcpServerSpec<IN, OUT>, ? extends Spec.TcpServerSpec<IN, OUT>> configuringFunction
	) {
		return ReactorTcpServer.create(ReactiveNet.tcpServer(configuringFunction));
	}


	/**
	 * Bind a new TCP server to the specified bind address and port.
	 * <p>
	 * A {@link reactor.io.net.tcp.TcpServer} is a specific kind of {@link org.reactivestreams.Publisher} that will
	 * emit:
	 * - onNext {@link ChannelStream} to consume data from
	 * - onComplete when server is shutdown
	 * - onError when any error (more specifically IO error) occurs
	 * From the emitted {@link ReactiveChannel}, one can decide to add in-channel consumers to read any incoming
	 * data.
	 * <p>
	 * To reply data on the active connection, {@link ReactiveChannel#writeWith} can subscribe to any passed {@link org
	 * .reactivestreams.Publisher}.
	 * <p>
	 * Note that {@link reactor.rx.Stream#getCapacity} will be used to switch on/off a channel in auto-read / flush on
	 * write mode.
	 * If the capacity is Long.MAX_Value, write on flush and auto read will apply. Otherwise, data will be flushed
	 * every
	 * capacity batch size and read will pause when capacity number of elements have been dispatched.
	 * <p>
	 * Emitted channels will run on the same thread they have beem receiving IO events.
	 * Apart from dispatching the write, it is possible to use {@link reactor.rx.Stream#process} to process requests
	 * asynchronously.
	 * <p>
	 * By default the type of emitted data or received data is {@link Buffer}
	 *
	 * @param serverFactory       the given implementation class for this peer
	 * @param configuringFunction a function will apply and return a {@link reactor.io.net.Spec} to customize the peer
	 * @param <IN>                the given input type received by this peer. Any configured codec decoder must match
	 *                            this type.
	 * @param <OUT>               the given output type received by this peer. Any configured codec encoder must match
	 *                            this type.
	 * @return a new Stream of ChannelStream, typically a peer of connections.
	 */
	public static <IN, OUT> ReactorTcpServer<IN, OUT> tcpServer(
	  Class<? extends TcpServer> serverFactory,
	  Function<? super Spec.TcpServerSpec<IN, OUT>, ? extends Spec.TcpServerSpec<IN, OUT>> configuringFunction
	) {
		return ReactorTcpServer.create(ReactiveNet.tcpServer(serverFactory, configuringFunction));
	}

	/**
	 * Bind a new TCP client to the localhost on port 12012. By default the default client implementation is scanned
	 * from the classpath on Class init. Support for Netty first and ZeroMQ then is provided as long as the relevant
	 * library dependencies are on the classpath.
	 * <p>
	 * A {@link reactor.io.net.tcp.TcpClient} is a specific kind of {@link org.reactivestreams.Publisher} that will
	 * emit:
	 * - onNext {@link ChannelStream} to consume data from
	 * - onComplete when client is shutdown
	 * - onError when any error (more specifically IO error) occurs
	 * From the emitted {@link ReactiveChannel}, one can decide to add in-channel consumers to read any incoming
	 * data.
	 * <p>
	 * To reply data on the active connection, {@link ReactiveChannel#writeWith} can subscribe to any passed {@link org
	 * .reactivestreams.Publisher}.
	 * <p>
	 * Note that {@link reactor.rx.Stream#getCapacity} will be used to switch on/off a channel in auto-read / flush on
	 * write mode.
	 * If the capacity is Long.MAX_Value, write on flush and auto read will apply. Otherwise, data will be flushed
	 * every
	 * capacity batch size and read will pause when capacity number of elements have been dispatched.
	 * <p>
	 * Emitted channels will run on the same thread they have beem receiving IO events.
	 * Apart from dispatching the write, it is possible to use {@link reactor.rx.Stream#process} to process requests
	 * asynchronously.
	 * <p>
	 * By default the type of emitted data or received data is {@link Buffer}
	 *
	 * @return a new Stream of ChannelStream, typically a peer of connections.
	 */
	public static ReactorTcpClient<Buffer, Buffer> tcpClient() {
		return ReactorTcpClient.create(ReactiveNet.tcpClient());
	}

	/**
	 * Bind a new TCP client to the specified connect address and port 12012. By default the default client
	 * implementation is scanned
	 * from the classpath on Class init. Support for Netty first and ZeroMQ then is provided as long as the relevant
	 * library dependencies are on the classpath.
	 * <p>
	 * A {@link reactor.io.net.tcp.TcpClient} is a specific kind of {@link org.reactivestreams.Publisher} that will
	 * emit:
	 * - onNext {@link ChannelStream} to consume data from
	 * - onComplete when client is shutdown
	 * - onError when any error (more specifically IO error) occurs
	 * From the emitted {@link ReactiveChannel}, one can decide to add in-channel consumers to read any incoming
	 * data.
	 * <p>
	 * To reply data on the active connection, {@link ReactiveChannel#writeWith} can subscribe to any passed {@link org
	 * .reactivestreams.Publisher}.
	 * <p>
	 * Note that {@link reactor.rx.Stream#getCapacity} will be used to switch on/off a channel in auto-read / flush on
	 * write mode.
	 * If the capacity is Long.MAX_Value, write on flush and auto read will apply. Otherwise, data will be flushed
	 * every
	 * capacity batch size and read will pause when capacity number of elements have been dispatched.
	 * <p>
	 * Emitted channels will run on the same thread they have beem receiving IO events.
	 * Apart from dispatching the write, it is possible to use {@link reactor.rx.Stream#process} to process requests
	 * asynchronously.
	 * <p>
	 * By default the type of emitted data or received data is {@link Buffer}
	 *
	 * @param bindAddress the address to connect to on port 12012
	 * @return a new Stream of ChannelStream, typically a peer of connections.
	 */
	public static ReactorTcpClient<Buffer, Buffer> tcpClient(String bindAddress) {
		return ReactorTcpClient.create(ReactiveNet.tcpClient(bindAddress));
	}

	/**
	 * Bind a new TCP client to "loopback" on the the specified port. By default the default client implementation is
	 * scanned
	 * from the classpath on Class init. Support for Netty first and ZeroMQ then is provided as long as the relevant
	 * library dependencies are on the classpath.
	 * <p>
	 * A {@link reactor.io.net.tcp.TcpClient} is a specific kind of {@link org.reactivestreams.Publisher} that will
	 * emit:
	 * - onNext {@link ChannelStream} to consume data from
	 * - onComplete when client is shutdown
	 * - onError when any error (more specifically IO error) occurs
	 * From the emitted {@link ReactiveChannel}, one can decide to add in-channel consumers to read any incoming
	 * data.
	 * <p>
	 * To reply data on the active connection, {@link ReactiveChannel#writeWith} can subscribe to any passed {@link org
	 * .reactivestreams.Publisher}.
	 * <p>
	 * Note that {@link reactor.rx.Stream#getCapacity} will be used to switch on/off a channel in auto-read / flush on
	 * write mode.
	 * If the capacity is Long.MAX_Value, write on flush and auto read will apply. Otherwise, data will be flushed
	 * every
	 * capacity batch size and read will pause when capacity number of elements have been dispatched.
	 * <p>
	 * Emitted channels will run on the same thread they have beem receiving IO events.
	 * Apart from dispatching the write, it is possible to use {@link reactor.rx.Stream#process} to process requests
	 * asynchronously.
	 * <p>
	 * By default the type of emitted data or received data is {@link Buffer}
	 *
	 * @param port the port to connect to on "loopback"
	 * @return a new Stream of ChannelStream, typically a peer of connections.
	 */
	public static ReactorTcpClient<Buffer, Buffer> tcpClient(int port) {
		return ReactorTcpClient.create(ReactiveNet.tcpClient(port));
	}

	/**
	 * Bind a new TCP client to the specified connect address and port. By default the default client implementation is
	 * scanned
	 * from the classpath on Class init. Support for Netty first and ZeroMQ then is provided as long as the relevant
	 * library dependencies are on the classpath.
	 * <p>
	 * A {@link reactor.io.net.tcp.TcpClient} is a specific kind of {@link org.reactivestreams.Publisher} that will
	 * emit:
	 * - onNext {@link ChannelStream} to consume data from
	 * - onComplete when client is shutdown
	 * - onError when any error (more specifically IO error) occurs
	 * From the emitted {@link ReactiveChannel}, one can decide to add in-channel consumers to read any incoming
	 * data.
	 * <p>
	 * To reply data on the active connection, {@link ReactiveChannel#writeWith} can subscribe to any passed {@link org
	 * .reactivestreams.Publisher}.
	 * <p>
	 * Note that {@link reactor.rx.Stream#getCapacity} will be used to switch on/off a channel in auto-read / flush on
	 * write mode.
	 * If the capacity is Long.MAX_Value, write on flush and auto read will apply. Otherwise, data will be flushed
	 * every
	 * capacity batch size and read will pause when capacity number of elements have been dispatched.
	 * <p>
	 * Emitted channels will run on the same thread they have beem receiving IO events.
	 * Apart from dispatching the write, it is possible to use {@link reactor.rx.Stream#process} to process requests
	 * asynchronously.
	 * <p>
	 * By default the type of emitted data or received data is {@link Buffer}
	 *
	 * @param bindAddress the address to connect to
	 * @param port        the port to connect to
	 * @return a new Stream of ChannelStream, typically a peer of connections.
	 */
	public static ReactorTcpClient<Buffer, Buffer> tcpClient(final String bindAddress, final int port) {
		return ReactorTcpClient.create(ReactiveNet.tcpClient(bindAddress, port));
	}

	/**
	 * Bind a new TCP client to the specified connect address and port. By default the default client implementation is
	 * scanned
	 * from the classpath on Class init. Support for Netty first and ZeroMQ then is provided as long as the relevant
	 * library dependencies are on the classpath.
	 * <p>
	 * A {@link reactor.io.net.tcp.TcpClient} is a specific kind of {@link org.reactivestreams.Publisher} that will
	 * emit:
	 * - onNext {@link ChannelStream} to consume data from
	 * - onComplete when client is shutdown
	 * - onError when any error (more specifically IO error) occurs
	 * From the emitted {@link ReactiveChannel}, one can decide to add in-channel consumers to read any incoming
	 * data.
	 * <p>
	 * To reply data on the active connection, {@link ReactiveChannel#writeWith} can subscribe to any passed {@link org
	 * .reactivestreams.Publisher}.
	 * <p>
	 * Note that {@link reactor.rx.Stream#getCapacity} will be used to switch on/off a channel in auto-read / flush on
	 * write mode.
	 * If the capacity is Long.MAX_Value, write on flush and auto read will apply. Otherwise, data will be flushed
	 * every
	 * capacity batch size and read will pause when capacity number of elements have been dispatched.
	 * <p>
	 * Emitted channels will run on the same thread they have beem receiving IO events.
	 * Apart from dispatching the write, it is possible to use {@link reactor.rx.Stream#process} to process requests
	 * asynchronously.
	 * <p>
	 * By default the type of emitted data or received data is {@link Buffer}
	 *
	 * @param configuringFunction a function will apply and return a {@link reactor.io.net.Spec} to customize the peer
	 * @param <IN>                the given input type received by this peer. Any configured codec decoder must match
	 *                            this type.
	 * @param <OUT>               the given output type received by this peer. Any configured codec encoder must match
	 *                            this type.
	 * @return a new Stream of ChannelStream, typically a peer of connections.
	 */
	public static <IN, OUT> ReactorTcpClient<IN, OUT> tcpClient(
	  Function<? super Spec.TcpClientSpec<IN, OUT>, ? extends Spec.TcpClientSpec<IN, OUT>> configuringFunction
	) {
		return ReactorTcpClient.create(ReactiveNet.tcpClient(configuringFunction));
	}

	/**
	 * Bind a new TCP client to the specified connect address and port.
	 * <p>
	 * A {@link reactor.io.net.tcp.TcpClient} is a specific kind of {@link org.reactivestreams.Publisher} that will
	 * emit:
	 * - onNext {@link ChannelStream} to consume data from
	 * - onComplete when client is shutdown
	 * - onError when any error (more specifically IO error) occurs
	 * From the emitted {@link ReactiveChannel}, one can decide to add in-channel consumers to read any incoming
	 * data.
	 * <p>
	 * To reply data on the active connection, {@link ReactiveChannel#writeWith} can subscribe to any passed {@link org
	 * .reactivestreams.Publisher}.
	 * <p>
	 * Note that {@link reactor.rx.Stream#getCapacity} will be used to switch on/off a channel in auto-read / flush on
	 * write mode.
	 * If the capacity is Long.MAX_Value, write on flush and auto read will apply. Otherwise, data will be flushed
	 * every
	 * capacity batch size and read will pause when capacity number of elements have been dispatched.
	 * <p>
	 * Emitted channels will run on the same thread they have beem receiving IO events.
	 * Apart from dispatching the write, it is possible to use {@link reactor.rx.Stream#process} to process requests
	 * asynchronously.
	 * <p>
	 * By default the type of emitted data or received data is {@link Buffer}
	 *
	 * @param clientFactory       the given implementation class for this peer
	 * @param configuringFunction a function will apply and return a {@link reactor.io.net.Spec} to customize the peer
	 * @param <IN>                the given input type received by this peer. Any configured codec decoder must match
	 *                            this type.
	 * @param <OUT>               the given output type received by this peer. Any configured codec encoder must match
	 *                            this type.
	 * @return a new Stream of ChannelStream, typically a peer of connections.
	 */
	public static <IN, OUT> ReactorTcpClient<IN, OUT> tcpClient(
	  Class<? extends TcpClient> clientFactory,
	  Function<? super Spec.TcpClientSpec<IN, OUT>, ? extends Spec.TcpClientSpec<IN, OUT>> configuringFunction
	) {
		return ReactorTcpClient.create(ReactiveNet.tcpClient(clientFactory, configuringFunction));
	}

	// HTTP

	/**
	 * Build a simple Netty HTTP server listening on 127.0.0.1 and 12012
	 *
	 * @return a simple HTTP Server
	 */
	public static ReactorHttpServer<Buffer, Buffer> httpServer() {
		return ReactorHttpServer.create(ReactiveNet.httpServer());
	}

	/**
	 * Build a simple Netty HTTP server listening on 127.0.0.1 and 12012
	 *
	 * @param bindAddress address to listen for (e.g. 0.0.0.0 or 127.0.0.1)
	 * @return a simple HTTP server
	 */
	public static ReactorHttpServer<Buffer, Buffer> httpServer(String bindAddress) {
		return ReactorHttpServer.create(ReactiveNet.httpServer(bindAddress));
	}

	/**
	 * Build a simple Netty HTTP server listening on 127.0.0.1 and the passed port
	 *
	 * @param port the port to listen to
	 * @return a simple HTTP server
	 */
	public static ReactorHttpServer<Buffer, Buffer> httpServer(int port) {
		return ReactorHttpServer.create(ReactiveNet.httpServer(port));
	}

	/**
	 * Build a simple Netty HTTP server listening othe passed bind address and port
	 *
	 * @param bindAddress address to listen for (e.g. 0.0.0.0 or 127.0.0.1)
	 * @param port        the port to listen to
	 * @return a simple HTTP server
	 */
	public static ReactorHttpServer<Buffer, Buffer> httpServer(final String bindAddress, final int port) {
		return ReactorHttpServer.create(ReactiveNet.httpServer(bindAddress, port));
	}

	/**
	 * Build a Netty HTTP Server with the passed factory
	 *
	 * @param configuringFunction a factory to build server configuration (see also {@link ReactiveNet.HttpServerFactory}
	 * @param <IN>                incoming data type
	 * @param <OUT>               outgoing data type
	 * @return a Netty HTTP server with the passed factory
	 */
	public static <IN, OUT> ReactorHttpServer<IN, OUT> httpServer(
	  Function<? super Spec.HttpServerSpec<IN, OUT>, ? extends Spec.HttpServerSpec<IN, OUT>> configuringFunction
	) {
		return ReactorHttpServer.create(ReactiveNet.httpServer(configuringFunction));
	}


	/**
	 * @param serverFactory       a target implementation server class
	 * @param configuringFunction a factory to build server configuration (see also {@link ReactiveNet.HttpServerFactory}
	 * @param <IN>                incoming data type
	 * @param <OUT>               outgoing data type
	 * @return a simple HTTP server
	 */
	public static <IN, OUT> ReactorHttpServer<IN, OUT> httpServer(
	  Class<? extends HttpServer> serverFactory,
	  Function<? super Spec.HttpServerSpec<IN, OUT>, ? extends Spec.HttpServerSpec<IN, OUT>> configuringFunction
	) {
		return ReactorHttpServer.create(ReactiveNet.httpServer(serverFactory, configuringFunction));
	}

	/**
	 * @return a simple HTTP client
	 */
	public static ReactorHttpClient<Buffer, Buffer> httpClient() {
		return ReactorHttpClient.create(ReactiveNet.httpClient());
	}

	/**
	 * Bind a new HTTP client to the specified connect address and port. By default the default server
	 * implementation is
	 * scanned
	 * from the classpath on Class init. Support for Netty is provided as long as the relevant
	 * library dependencies are on the classpath.
	 * <p>
	 * A {@link reactor.io.net.http.HttpClient} is a specific kind of {@link org.reactivestreams.Publisher} that will
	 * emit:
	 * - onNext {@link ChannelStream} to consume data from
	 * - onComplete when client is shutdown
	 * - onError when any error (more specifically IO error) occurs
	 * From the emitted {@link ReactiveChannel}, one can decide to add in-channel consumers to read any incoming
	 * data.
	 * <p>
	 * To reply data on the active connection, {@link ReactiveChannel#writeWith} can subscribe to any passed {@link org
	 * .reactivestreams.Publisher}.
	 * <p>
	 * Note that {@link reactor.rx.Stream#getCapacity} will be used to switch on/off a channel in auto-read / flush on
	 * write mode.
	 * If the capacity is Long.MAX_Value, write on flush and auto read will apply. Otherwise, data will be flushed
	 * every
	 * capacity batch size and read will pause when capacity number of elements have been dispatched.
	 * <p>
	 * Emitted channels will run on the same thread they have beem receiving IO events.
	 * Apart from dispatching the write, it is possible to use {@link reactor.rx.Stream#process} to process requests
	 * asynchronously.
	 * <p>
	 * By default the type of emitted data or received data is {@link Buffer}
	 *
	 * @param configuringFunction a function will apply and return a {@link reactor.io.net.Spec} to customize the peer
	 * @param <IN>                the given input type received by this peer. Any configured codec decoder must match
	 *                            this type.
	 * @param <OUT>               the given output type received by this peer. Any configured codec encoder must match
	 *                            this type.
	 * @return a new Stream of ChannelStream, typically a peer of connections.
	 */
	public static <IN, OUT> ReactorHttpClient<IN, OUT> httpClient(
	  Function<? super Spec.HttpClientSpec<IN, OUT>, ? extends Spec.HttpClientSpec<IN, OUT>> configuringFunction
	) {
		return ReactorHttpClient.create(ReactiveNet.httpClient(configuringFunction));
	}

	/**
	 * Bind a new HTTP client to the specified connect address and port. By default the default server
	 * implementation is
	 * scanned
	 * from the classpath on Class init. Support for Netty is provided as long as the relevant
	 * library dependencies are on the classpath.
	 * <p>
	 * A {@link reactor.io.net.http.HttpClient} is a specific kind of {@link org.reactivestreams.Publisher} that will
	 * emit:
	 * - onNext {@link ChannelStream} to consume data from
	 * - onComplete when client is shutdown
	 * - onError when any error (more specifically IO error) occurs
	 * From the emitted {@link ReactiveChannel}, one can decide to add in-channel consumers to read any incoming
	 * data.
	 * <p>
	 * To reply data on the active connection, {@link ReactiveChannel#writeWith} can subscribe to any passed {@link org
	 * .reactivestreams.Publisher}.
	 * <p>
	 * Note that {@link reactor.rx.Stream#getCapacity} will be used to switch on/off a channel in auto-read / flush on
	 * write mode.
	 * If the capacity is Long.MAX_Value, write on flush and auto read will apply. Otherwise, data will be flushed
	 * every
	 * capacity batch size and read will pause when capacity number of elements have been dispatched.
	 * <p>
	 * Emitted channels will run on the same thread they have beem receiving IO events.
	 * Apart from dispatching the write, it is possible to use {@link reactor.rx.Stream#process} to process requests
	 * asynchronously.
	 * <p>
	 * By default the type of emitted data or received data is {@link Buffer}
	 *
	 * @param clientFactory       the given implementation class for this peer
	 * @param configuringFunction a function will apply and return a {@link reactor.io.net.Spec} to customize the peer
	 * @param <IN>                the given input type received by this peer. Any configured codec decoder must match
	 *                            this type.
	 * @param <OUT>               the given output type received by this peer. Any configured codec encoder must match
	 *                            this type.
	 * @return a new Stream of ChannelStream, typically a peer of connections.
	 */
	public static <IN, OUT> ReactorHttpClient<IN, OUT> httpClient(
	  Class<? extends HttpClient> clientFactory,
	  Function<? super Spec.HttpClientSpec<IN, OUT>, ? extends Spec.HttpClientSpec<IN, OUT>> configuringFunction
	) {
		return ReactorHttpClient.create(ReactiveNet.httpClient(clientFactory, configuringFunction));
	}

	// UDP

	/**
	 * Bind a new UDP server to the "loopback" address. By default the default server implementation is scanned
	 * from the classpath on Class init. Support for Netty is provided as long as the relevant
	 * library dependencies are on the classpath.
	 * <p>
	 * <p>
	 * From the emitted {@link ReactiveChannel}, one can decide to add in-channel consumers to read any incoming
	 * data.
	 * <p>
	 * To reply data on the active connection, {@link ReactiveChannel#writeWith} can subscribe to any passed {@link org
	 * .reactivestreams.Publisher}.
	 * <p>
	 * Note that {@link reactor.rx.Stream#getCapacity} will be used to switch on/off a channel in auto-read / flush on
	 * write mode.
	 * If the capacity is Long.MAX_Value, write on flush and auto read will apply. Otherwise, data will be flushed
	 * every
	 * capacity batch size and read will pause when capacity number of elements have been dispatched.
	 * <p>
	 * Emitted channels will run on the same thread they have beem receiving IO events.
	 * Apart from dispatching the write, it is possible to use {@link reactor.rx.Stream#process} to process requests
	 * asynchronously.
	 * <p>
	 * By default the type of emitted data or received data is {@link Buffer}
	 *
	 * @return a new Stream of ChannelStream, typically a peer of connections.
	 */
	public static ReactorDatagramServer<Buffer, Buffer> udpServer() {
		return ReactorDatagramServer.create(ReactiveNet.udpServer());
	}

	/**
	 * Bind a new UDP server to the given bind address. By default the default server implementation is scanned
	 * from the classpath on Class init. Support for Netty is provided as long as the relevant
	 * library dependencies are on the classpath.
	 * <p>
	 * <p>
	 * From the emitted {@link ReactiveChannel}, one can decide to add in-channel consumers to read any incoming
	 * data.
	 * <p>
	 * To reply data on the active connection, {@link ReactiveChannel#writeWith} can subscribe to any passed {@link org
	 * .reactivestreams.Publisher}.
	 * <p>
	 * Note that {@link reactor.rx.Stream#getCapacity} will be used to switch on/off a channel in auto-read / flush on
	 * write mode.
	 * If the capacity is Long.MAX_Value, write on flush and auto read will apply. Otherwise, data will be flushed
	 * every
	 * capacity batch size and read will pause when capacity number of elements have been dispatched.
	 * <p>
	 * Emitted channels will run on the same thread they have beem receiving IO events.
	 * Apart from dispatching the write, it is possible to use {@link reactor.rx.Stream#process} to process requests
	 * asynchronously.
	 * <p>
	 * By default the type of emitted data or received data is {@link Buffer}
	 *
	 * @param bindAddress bind address (e.g. "127.0.0.1") to create the server on the passed port
	 * @return a new Stream of ChannelStream, typically a peer of connections.
	 */
	public static ReactorDatagramServer<Buffer, Buffer> udpServer(String bindAddress) {
		return ReactorDatagramServer.create(ReactiveNet.udpServer(bindAddress));
	}

	/**
	 * Bind a new UDP server to the "loopback" address and specified port. By default the default server implementation
	 * is scanned
	 * from the classpath on Class init. Support for Netty is provided as long as the relevant
	 * library dependencies are on the classpath.
	 * <p>
	 * <p>
	 * From the emitted {@link ReactiveChannel}, one can decide to add in-channel consumers to read any incoming
	 * data.
	 * <p>
	 * To reply data on the active connection, {@link ReactiveChannel#writeWith} can subscribe to any passed {@link org
	 * .reactivestreams.Publisher}.
	 * <p>
	 * Note that {@link reactor.rx.Stream#getCapacity} will be used to switch on/off a channel in auto-read / flush on
	 * write mode.
	 * If the capacity is Long.MAX_Value, write on flush and auto read will apply. Otherwise, data will be flushed
	 * every
	 * capacity batch size and read will pause when capacity number of elements have been dispatched.
	 * <p>
	 * Emitted channels will run on the same thread they have beem receiving IO events.
	 * Apart from dispatching the write, it is possible to use {@link reactor.rx.Stream#process} to process requests
	 * asynchronously.
	 * <p>
	 * By default the type of emitted data or received data is {@link Buffer}
	 *
	 * @param port the port to listen on the passed bind address
	 * @return a new Stream of ChannelStream, typically a peer of connections.
	 */
	public static ReactorDatagramServer<Buffer, Buffer> udpServer(int port) {
		return ReactorDatagramServer.create(ReactiveNet.udpServer(port));
	}

	/**
	 * Bind a new UDP server to the given bind address and port. By default the default server implementation is
	 * scanned
	 * from the classpath on Class init. Support for Netty is provided as long as the relevant
	 * library dependencies are on the classpath.
	 * <p>
	 * <p>
	 * From the emitted {@link ReactiveChannel}, one can decide to add in-channel consumers to read any incoming
	 * data.
	 * <p>
	 * To reply data on the active connection, {@link ReactiveChannel#writeWith} can subscribe to any passed {@link org
	 * .reactivestreams.Publisher}.
	 * <p>
	 * Note that {@link reactor.rx.Stream#getCapacity} will be used to switch on/off a channel in auto-read / flush on
	 * write mode.
	 * If the capacity is Long.MAX_Value, write on flush and auto read will apply. Otherwise, data will be flushed
	 * every
	 * capacity batch size and read will pause when capacity number of elements have been dispatched.
	 * <p>
	 * Emitted channels will run on the same thread they have beem receiving IO events.
	 * Apart from dispatching the write, it is possible to use {@link reactor.rx.Stream#process} to process requests
	 * asynchronously.
	 * <p>
	 * By default the type of emitted data or received data is {@link Buffer}
	 *
	 * @param port        the port to listen on the passed bind address
	 * @param bindAddress bind address (e.g. "127.0.0.1") to create the server on the passed port
	 * @return a new Stream of ChannelStream, typically a peer of connections.
	 */
	public static ReactorDatagramServer<Buffer, Buffer> udpServer(final String bindAddress, final int port) {
		return ReactorDatagramServer.create(ReactiveNet.udpServer(bindAddress, port));
	}

	/**
	 * Bind a new UDP server to the specified bind address and port. By default the default server implementation is
	 * scanned
	 * from the classpath on Class init. Support for Netty is provided as long as the relevant
	 * library dependencies are on the classpath.
	 * <p>
	 * <p>
	 * From the emitted {@link ReactiveChannel}, one can decide to add in-channel consumers to read any incoming
	 * data.
	 * <p>
	 * To reply data on the active connection, {@link ReactiveChannel#writeWith} can subscribe to any passed {@link org
	 * .reactivestreams.Publisher}.
	 * <p>
	 * Note that {@link reactor.rx.Stream#getCapacity} will be used to switch on/off a channel in auto-read / flush on
	 * write mode.
	 * If the capacity is Long.MAX_Value, write on flush and auto read will apply. Otherwise, data will be flushed
	 * every
	 * capacity batch size and read will pause when capacity number of elements have been dispatched.
	 * <p>
	 * Emitted channels will run on the same thread they have beem receiving IO events.
	 * Apart from dispatching the write, it is possible to use {@link reactor.rx.Stream#process} to process requests
	 * asynchronously.
	 * <p>
	 * By default the type of emitted data or received data is {@link Buffer}
	 *
	 * @param configuringFunction a function will apply and return a {@link reactor.io.net.Spec} to customize the peer
	 * @param <IN>                the given input type received by this peer. Any configured codec decoder must match
	 *                            this type.
	 * @param <OUT>               the given output type received by this peer. Any configured codec encoder must match
	 *                            this type.
	 * @return a new Stream of ChannelStream, typically a peer of connections.
	 */
	public static <IN, OUT> ReactorDatagramServer<IN, OUT> udpServer(
	  Function<? super Spec.DatagramServerSpec<IN, OUT>, ? extends Spec.DatagramServerSpec<IN, OUT>>
		configuringFunction
	) {
		return ReactorDatagramServer.create(ReactiveNet.udpServer(configuringFunction));
	}


	/**
	 * Bind a new UDP server to the specified bind address and port.
	 * <p>
	 * <p>
	 * From the emitted {@link ReactiveChannel}, one can decide to add in-channel consumers to read any incoming
	 * data.
	 * <p>
	 * To reply data on the active connection, {@link ReactiveChannel#writeWith} can subscribe to any passed {@link org
	 * .reactivestreams.Publisher}.
	 * <p>
	 * Note that {@link reactor.rx.Stream#getCapacity} will be used to switch on/off a channel in auto-read / flush on
	 * write mode.
	 * If the capacity is Long.MAX_Value, write on flush and auto read will apply. Otherwise, data will be flushed
	 * every
	 * capacity batch size and read will pause when capacity number of elements have been dispatched.
	 * <p>
	 * Emitted channels will run on the same thread they have beem receiving IO events.
	 * Apart from dispatching the write, it is possible to use {@link reactor.rx.Stream#process} to process requests
	 * asynchronously.
	 * <p>
	 * By default the type of emitted data or received data is {@link Buffer}
	 *
	 * @param serverFactory       the given implementation class for this peer
	 * @param configuringFunction a function will apply and return a {@link reactor.io.net.Spec} to customize the peer
	 * @param <IN>                the given input type received by this peer. Any configured codec decoder must match
	 *                            this type.
	 * @param <OUT>               the given output type received by this peer. Any configured codec encoder must match
	 *                            this type.
	 * @return a new Stream of ChannelStream, typically a peer of connections.
	 */
	public static <IN, OUT> ReactorDatagramServer<IN, OUT> udpServer(
	  Class<? extends DatagramServer> serverFactory,
	  Function<? super Spec.DatagramServerSpec<IN, OUT>, ? extends Spec.DatagramServerSpec<IN, OUT>>
		configuringFunction
	) {
		return ReactorDatagramServer.create(ReactiveNet.udpServer(serverFactory, configuringFunction));
	}

	/**
	 * Utils to read the ReactiveChannel underlying channel
	 */

	@SuppressWarnings("unchecked")
	public static <E, IN, OUT> E delegate(ReactiveChannel<IN, OUT> channelStream) {
		return (E) delegate(channelStream, Object.class);
	}

	@SuppressWarnings("unchecked")
	public static <E, IN, OUT> E delegate(ReactiveChannel<IN, OUT> channelStream, Class<E> clazz) {
		Assert.isTrue(
		  clazz.isAssignableFrom(channelStream.delegate().getClass()),
		  "Underlying channel is not of the given type: " + clazz.getName()
		);

		return (E) channelStream.delegate();
	}

}
