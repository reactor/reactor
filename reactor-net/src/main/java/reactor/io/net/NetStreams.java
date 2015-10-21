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

import reactor.Timers;
import reactor.core.support.Assert;
import reactor.fn.Function;
import reactor.io.buffer.Buffer;
import reactor.io.net.http.HttpClient;
import reactor.io.net.http.HttpServer;
import reactor.io.net.impl.netty.http.NettyHttpClient;
import reactor.io.net.impl.netty.http.NettyHttpServer;
import reactor.io.net.impl.netty.tcp.NettyTcpClient;
import reactor.io.net.impl.netty.tcp.NettyTcpServer;
import reactor.io.net.impl.netty.udp.NettyDatagramServer;
import reactor.io.net.tcp.TcpClient;
import reactor.io.net.tcp.TcpServer;
import reactor.io.net.udp.DatagramServer;
import reactor.rx.Streams;

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
public class NetStreams extends Streams {

	public static final int    DEFAULT_PORT         = 12012;
	public static final String DEFAULT_BIND_ADDRESS = "127.0.0.1";
	public static final Class<? extends TcpServer>      DEFAULT_TCP_SERVER_TYPE;
	public static final Class<? extends TcpClient>      DEFAULT_TCP_CLIENT_TYPE;
	public static final Class<? extends HttpServer>     DEFAULT_HTTP_SERVER_TYPE;
	public static final Class<? extends HttpClient>     DEFAULT_HTTP_CLIENT_TYPE;
	public static final Class<? extends DatagramServer> DEFAULT_UDP_SERVER_TYPE;

	private NetStreams() {
	}

	// Marker interfaces to avoid complicated generic dance for Java < 8 Users.

	public interface TcpServerFactory<IN, OUT>
	  extends Function<Spec.TcpServerSpec<IN, OUT>, Spec.TcpServerSpec<IN, OUT>> {
	}

	public interface TcpClientFactory<IN, OUT>
	  extends Function<Spec.TcpClientSpec<IN, OUT>, Spec.TcpClientSpec<IN, OUT>> {
	}

	public interface HttpServerFactory<IN, OUT>
	  extends Function<Spec.HttpServerSpec<IN, OUT>, Spec.HttpServerSpec<IN, OUT>> {
	}

	public interface HttpClientFactory<IN, OUT>
	  extends Function<Spec.HttpClientSpec<IN, OUT>, Spec.HttpClientSpec<IN, OUT>> {
	}

	public interface UdpServerFactory<IN, OUT>
	  extends Function<Spec.DatagramServerSpec<IN, OUT>, Spec.DatagramServerSpec<IN, OUT>> {
	}

	// TCP

	/**
	 * Bind a new TCP server to "loopback" on port {@literal 12012}. By default the default server implementation is
	 * scanned
	 * from the classpath on Class init. Support for Netty first and ZeroMQ then is provided as long as the relevant
	 * library dependencies are on the classpath.
	 * <p>
	 * To reply data on the active connection, {@link ReactorChannel#writeWith} can subscribe to any passed {@link org
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
	 * By default the type of emitted data or received data is {@link reactor.io.buffer.Buffer}
	 *
	 * @return a new Stream of ChannelStream, typically a peer of connections.
	 */
	public static TcpServer<Buffer, Buffer> tcpServer() {
		return tcpServer(DEFAULT_BIND_ADDRESS);
	}

	/**
	 * Bind a new TCP server to "loopback" on the given port. By default the default server implementation is scanned
	 * from the classpath on Class init. Support for Netty first and ZeroMQ then is provided as long as the relevant
	 * library dependencies are on the classpath.
	 * <p>
	 * A {@link reactor.io.net.tcp.TcpServer} is a specific kind of {@link org.reactivestreams.Publisher} that will
	 * emit:
	 * - onNext {@link reactor.io.net.ChannelStream} to consume data from
	 * - onComplete when server is shutdown
	 * - onError when any error (more specifically IO error) occurs
	 * From the emitted {@link ReactorChannel}, one can decide to add in-channel consumers to read any incoming
	 * data.
	 * <p>
	 * To reply data on the active connection, {@link ReactorChannel#writeWith} can subscribe to any passed {@link org
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
	 * By default the type of emitted data or received data is {@link reactor.io.buffer.Buffer}
	 *
	 * @param port the port to listen on loopback
	 * @return a new Stream of ChannelStream, typically a peer of connections.
	 */
	public static TcpServer<Buffer, Buffer> tcpServer(int port) {
		return tcpServer(DEFAULT_BIND_ADDRESS, port);
	}

	/**
	 * Bind a new TCP server to the given bind address on port {@literal 12012}. By default the default server
	 * implementation is scanned
	 * from the classpath on Class init. Support for Netty first and ZeroMQ then is provided as long as the relevant
	 * library dependencies are on the classpath.
	 * <p>
	 * A {@link reactor.io.net.tcp.TcpServer} is a specific kind of {@link org.reactivestreams.Publisher} that will
	 * emit:
	 * - onNext {@link reactor.io.net.ChannelStream} to consume data from
	 * - onComplete when server is shutdown
	 * - onError when any error (more specifically IO error) occurs
	 * From the emitted {@link ReactorChannel}, one can decide to add in-channel consumers to read any incoming
	 * data.
	 * <p>
	 * To reply data on the active connection, {@link ReactorChannel#writeWith} can subscribe to any passed {@link org
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
	 * By default the type of emitted data or received data is {@link reactor.io.buffer.Buffer}
	 *
	 * @param bindAddress bind address (e.g. "127.0.0.1") to create the server on the default port 12012
	 * @return a new Stream of ChannelStream, typically a peer of connections.
	 */
	public static TcpServer<Buffer, Buffer> tcpServer(String bindAddress) {
		return tcpServer(bindAddress, DEFAULT_PORT);
	}

	/**
	 * Bind a new TCP server to the given bind address and port. By default the default server implementation is
	 * scanned
	 * from the classpath on Class init. Support for Netty first and ZeroMQ then is provided as long as the relevant
	 * library dependencies are on the classpath.
	 * <p>
	 * A {@link reactor.io.net.tcp.TcpServer} is a specific kind of {@link org.reactivestreams.Publisher} that will
	 * emit:
	 * - onNext {@link reactor.io.net.ChannelStream} to consume data from
	 * - onComplete when server is shutdown
	 * - onError when any error (more specifically IO error) occurs
	 * From the emitted {@link ReactorChannel}, one can decide to add in-channel consumers to read any incoming
	 * data.
	 * <p>
	 * To reply data on the active connection, {@link ReactorChannel#writeWith} can subscribe to any passed {@link org
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
	 * By default the type of emitted data or received data is {@link reactor.io.buffer.Buffer}
	 *
	 * @param port        the port to listen on the passed bind address
	 * @param bindAddress bind address (e.g. "127.0.0.1") to create the server on the passed port
	 * @return a new Stream of ChannelStream, typically a peer of connections.
	 */
	public static TcpServer<Buffer, Buffer> tcpServer(final String bindAddress, final int port) {
		return tcpServer(new Function<Spec.TcpServerSpec<Buffer, Buffer>, Spec.TcpServerSpec<Buffer, Buffer>>() {
			@Override
			public Spec.TcpServerSpec<Buffer, Buffer> apply(Spec.TcpServerSpec<Buffer, Buffer> serverSpec) {
				serverSpec.timer(Timers.globalOrNull());
				return serverSpec.listen(bindAddress, port);
			}
		});
	}

	/**
	 * Bind a new TCP server to the specified bind address and port. By default the default server implementation is
	 * scanned
	 * from the classpath on Class init. Support for Netty first and ZeroMQ then is provided as long as the relevant
	 * library dependencies are on the classpath.
	 * <p>
	 * A {@link reactor.io.net.tcp.TcpServer} is a specific kind of {@link org.reactivestreams.Publisher} that will
	 * emit:
	 * - onNext {@link reactor.io.net.ChannelStream} to consume data from
	 * - onComplete when server is shutdown
	 * - onError when any error (more specifically IO error) occurs
	 * From the emitted {@link ReactorChannel}, one can decide to add in-channel consumers to read any incoming
	 * data.
	 * <p>
	 * To reply data on the active connection, {@link ReactorChannel#writeWith} can subscribe to any passed {@link org
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
	 * By default the type of emitted data or received data is {@link reactor.io.buffer.Buffer}
	 *
	 * @param configuringFunction a function will apply and return a {@link reactor.io.net.Spec} to customize the peer
	 * @param <IN>                the given input type received by this peer. Any configured codec decoder must match
	 *                            this type.
	 * @param <OUT>               the given output type received by this peer. Any configured codec encoder must match
	 *                            this type.
	 * @return a new Stream of ChannelStream, typically a peer of connections.
	 */
	public static <IN, OUT> TcpServer<IN, OUT> tcpServer(
	  Function<? super Spec.TcpServerSpec<IN, OUT>, ? extends Spec.TcpServerSpec<IN, OUT>> configuringFunction
	) {
		return tcpServer(DEFAULT_TCP_SERVER_TYPE, configuringFunction);
	}

	/**
	 * Bind a new TCP server to the specified bind address and port.
	 * <p>
	 * A {@link reactor.io.net.tcp.TcpServer} is a specific kind of {@link org.reactivestreams.Publisher} that will
	 * emit:
	 * - onNext {@link reactor.io.net.ChannelStream} to consume data from
	 * - onComplete when server is shutdown
	 * - onError when any error (more specifically IO error) occurs
	 * From the emitted {@link ReactorChannel}, one can decide to add in-channel consumers to read any incoming
	 * data.
	 * <p>
	 * To reply data on the active connection, {@link ReactorChannel#writeWith} can subscribe to any passed {@link org
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
	 * By default the type of emitted data or received data is {@link reactor.io.buffer.Buffer}
	 *
	 * @param serverFactory       the given implementation class for this peer
	 * @param configuringFunction a function will apply and return a {@link reactor.io.net.Spec} to customize the peer
	 * @param <IN>                the given input type received by this peer. Any configured codec decoder must match
	 *                            this type.
	 * @param <OUT>               the given output type received by this peer. Any configured codec encoder must match
	 *                            this type.
	 * @return a new Stream of ChannelStream, typically a peer of connections.
	 */
	public static <IN, OUT> TcpServer<IN, OUT> tcpServer(
	  Class<? extends TcpServer> serverFactory,
	  Function<? super Spec.TcpServerSpec<IN, OUT>, ? extends Spec.TcpServerSpec<IN, OUT>> configuringFunction
	) {
		return configuringFunction.apply(new Spec.TcpServerSpec<IN, OUT>(serverFactory)).get();
	}


	/**
	 * Bind a new TCP client to the localhost on port 12012. By default the default client implementation is scanned
	 * from the classpath on Class init. Support for Netty first and ZeroMQ then is provided as long as the relevant
	 * library dependencies are on the classpath.
	 * <p>
	 * A {@link reactor.io.net.tcp.TcpClient} is a specific kind of {@link org.reactivestreams.Publisher} that will
	 * emit:
	 * - onNext {@link reactor.io.net.ChannelStream} to consume data from
	 * - onComplete when client is shutdown
	 * - onError when any error (more specifically IO error) occurs
	 * From the emitted {@link ReactorChannel}, one can decide to add in-channel consumers to read any incoming
	 * data.
	 * <p>
	 * To reply data on the active connection, {@link ReactorChannel#writeWith} can subscribe to any passed {@link org
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
	 * By default the type of emitted data or received data is {@link reactor.io.buffer.Buffer}
	 *
	 * @return a new Stream of ChannelStream, typically a peer of connections.
	 */
	public static TcpClient<Buffer, Buffer> tcpClient() {
		return tcpClient(DEFAULT_BIND_ADDRESS);
	}

	/**
	 * Bind a new TCP client to the specified connect address and port 12012. By default the default client
	 * implementation is scanned
	 * from the classpath on Class init. Support for Netty first and ZeroMQ then is provided as long as the relevant
	 * library dependencies are on the classpath.
	 * <p>
	 * A {@link reactor.io.net.tcp.TcpClient} is a specific kind of {@link org.reactivestreams.Publisher} that will
	 * emit:
	 * - onNext {@link reactor.io.net.ChannelStream} to consume data from
	 * - onComplete when client is shutdown
	 * - onError when any error (more specifically IO error) occurs
	 * From the emitted {@link ReactorChannel}, one can decide to add in-channel consumers to read any incoming
	 * data.
	 * <p>
	 * To reply data on the active connection, {@link ReactorChannel#writeWith} can subscribe to any passed {@link org
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
	 * By default the type of emitted data or received data is {@link reactor.io.buffer.Buffer}
	 *
	 * @param bindAddress the address to connect to on port 12012
	 * @return a new Stream of ChannelStream, typically a peer of connections.
	 */
	public static TcpClient<Buffer, Buffer> tcpClient(String bindAddress) {
		return tcpClient(bindAddress, DEFAULT_PORT);
	}

	/**
	 * Bind a new TCP client to "loopback" on the the specified port. By default the default client implementation is
	 * scanned
	 * from the classpath on Class init. Support for Netty first and ZeroMQ then is provided as long as the relevant
	 * library dependencies are on the classpath.
	 * <p>
	 * A {@link reactor.io.net.tcp.TcpClient} is a specific kind of {@link org.reactivestreams.Publisher} that will
	 * emit:
	 * - onNext {@link reactor.io.net.ChannelStream} to consume data from
	 * - onComplete when client is shutdown
	 * - onError when any error (more specifically IO error) occurs
	 * From the emitted {@link ReactorChannel}, one can decide to add in-channel consumers to read any incoming
	 * data.
	 * <p>
	 * To reply data on the active connection, {@link ReactorChannel#writeWith} can subscribe to any passed {@link org
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
	 * By default the type of emitted data or received data is {@link reactor.io.buffer.Buffer}
	 *
	 * @param port the port to connect to on "loopback"
	 * @return a new Stream of ChannelStream, typically a peer of connections.
	 */
	public static TcpClient<Buffer, Buffer> tcpClient(int port) {
		return tcpClient(DEFAULT_BIND_ADDRESS, port);
	}

	/**
	 * Bind a new TCP client to the specified connect address and port. By default the default client implementation is
	 * scanned
	 * from the classpath on Class init. Support for Netty first and ZeroMQ then is provided as long as the relevant
	 * library dependencies are on the classpath.
	 * <p>
	 * A {@link reactor.io.net.tcp.TcpClient} is a specific kind of {@link org.reactivestreams.Publisher} that will
	 * emit:
	 * - onNext {@link reactor.io.net.ChannelStream} to consume data from
	 * - onComplete when client is shutdown
	 * - onError when any error (more specifically IO error) occurs
	 * From the emitted {@link ReactorChannel}, one can decide to add in-channel consumers to read any incoming
	 * data.
	 * <p>
	 * To reply data on the active connection, {@link ReactorChannel#writeWith} can subscribe to any passed {@link org
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
	 * By default the type of emitted data or received data is {@link reactor.io.buffer.Buffer}
	 *
	 * @param bindAddress the address to connect to
	 * @param port        the port to connect to
	 * @return a new Stream of ChannelStream, typically a peer of connections.
	 */
	public static TcpClient<Buffer, Buffer> tcpClient(final String bindAddress, final int port) {
		return tcpClient(new Function<Spec.TcpClientSpec<Buffer, Buffer>, Spec.TcpClientSpec<Buffer, Buffer>>() {
			@Override
			public Spec.TcpClientSpec<Buffer, Buffer> apply(Spec.TcpClientSpec<Buffer, Buffer> clientSpec) {
				clientSpec.timer(Timers.globalOrNull());
				return clientSpec.connect(bindAddress, port);
			}
		});
	}

	/**
	 * Bind a new TCP client to the specified connect address and port. By default the default client implementation is
	 * scanned
	 * from the classpath on Class init. Support for Netty first and ZeroMQ then is provided as long as the relevant
	 * library dependencies are on the classpath.
	 * <p>
	 * A {@link reactor.io.net.tcp.TcpClient} is a specific kind of {@link org.reactivestreams.Publisher} that will
	 * emit:
	 * - onNext {@link reactor.io.net.ChannelStream} to consume data from
	 * - onComplete when client is shutdown
	 * - onError when any error (more specifically IO error) occurs
	 * From the emitted {@link ReactorChannel}, one can decide to add in-channel consumers to read any incoming
	 * data.
	 * <p>
	 * To reply data on the active connection, {@link ReactorChannel#writeWith} can subscribe to any passed {@link org
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
	 * By default the type of emitted data or received data is {@link reactor.io.buffer.Buffer}
	 *
	 * @param configuringFunction a function will apply and return a {@link reactor.io.net.Spec} to customize the peer
	 * @param <IN>                the given input type received by this peer. Any configured codec decoder must match
	 *                            this type.
	 * @param <OUT>               the given output type received by this peer. Any configured codec encoder must match
	 *                            this type.
	 * @return a new Stream of ChannelStream, typically a peer of connections.
	 */
	public static <IN, OUT> TcpClient<IN, OUT> tcpClient(
	  Function<? super Spec.TcpClientSpec<IN, OUT>, ? extends Spec.TcpClientSpec<IN, OUT>> configuringFunction
	) {
		return tcpClient(DEFAULT_TCP_CLIENT_TYPE, configuringFunction);
	}

	/**
	 * Bind a new TCP client to the specified connect address and port.
	 * <p>
	 * A {@link reactor.io.net.tcp.TcpClient} is a specific kind of {@link org.reactivestreams.Publisher} that will
	 * emit:
	 * - onNext {@link reactor.io.net.ChannelStream} to consume data from
	 * - onComplete when client is shutdown
	 * - onError when any error (more specifically IO error) occurs
	 * From the emitted {@link ReactorChannel}, one can decide to add in-channel consumers to read any incoming
	 * data.
	 * <p>
	 * To reply data on the active connection, {@link ReactorChannel#writeWith} can subscribe to any passed {@link org
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
	 * By default the type of emitted data or received data is {@link reactor.io.buffer.Buffer}
	 *
	 * @param clientFactory       the given implementation class for this peer
	 * @param configuringFunction a function will apply and return a {@link reactor.io.net.Spec} to customize the peer
	 * @param <IN>                the given input type received by this peer. Any configured codec decoder must match
	 *                            this type.
	 * @param <OUT>               the given output type received by this peer. Any configured codec encoder must match
	 *                            this type.
	 * @return a new Stream of ChannelStream, typically a peer of connections.
	 */
	public static <IN, OUT> TcpClient<IN, OUT> tcpClient(
	  Class<? extends TcpClient> clientFactory,
	  Function<? super Spec.TcpClientSpec<IN, OUT>, ? extends Spec.TcpClientSpec<IN, OUT>> configuringFunction
	) {
		return configuringFunction.apply(new Spec.TcpClientSpec<IN, OUT>(clientFactory)).get();
	}

	// HTTP

	/**
	 * Build a simple Netty HTTP server listening on 127.0.0.1 and 12012
	 *
	 * @return a simple HTTP Server
	 */
	public static HttpServer<Buffer, Buffer> httpServer() {
		return httpServer(DEFAULT_BIND_ADDRESS);
	}

	/**
	 * Build a simple Netty HTTP server listening on 127.0.0.1 and 12012
	 *
	 * @param bindAddress address to listen for (e.g. 0.0.0.0 or 127.0.0.1)
	 * @return a simple HTTP server
	 */
	public static HttpServer<Buffer, Buffer> httpServer(String bindAddress) {
		return httpServer(bindAddress, DEFAULT_PORT);
	}

	/**
	 * Build a simple Netty HTTP server listening on 127.0.0.1 and the passed port
	 *
	 * @param port the port to listen to
	 * @return a simple HTTP server
	 */
	public static HttpServer<Buffer, Buffer> httpServer(int port) {
		return httpServer(DEFAULT_BIND_ADDRESS, port);
	}

	/**
	 * Build a simple Netty HTTP server listening othe passed bind address and port
	 *
	 * @param bindAddress address to listen for (e.g. 0.0.0.0 or 127.0.0.1)
	 * @param port        the port to listen to
	 * @return a simple HTTP server
	 */
	public static HttpServer<Buffer, Buffer> httpServer(final String bindAddress, final int port) {
		return httpServer(new Function<Spec.HttpServerSpec<Buffer, Buffer>, Spec.HttpServerSpec<Buffer, Buffer>>() {
			@Override
			public Spec.HttpServerSpec<Buffer, Buffer> apply(Spec.HttpServerSpec<Buffer, Buffer> serverSpec) {
				serverSpec.timer(Timers.globalOrNull());
				return serverSpec.listen(bindAddress, port);
			}
		});
	}

	/**
	 * Build a Netty HTTP Server with the passed factory
	 *
	 * @param configuringFunction a factory to build server configuration (see also {@link HttpServerFactory}
	 * @param <IN>                incoming data type
	 * @param <OUT>               outgoing data type
	 * @return a Netty HTTP server with the passed factory
	 */
	public static <IN, OUT> HttpServer<IN, OUT> httpServer(
	  Function<? super Spec.HttpServerSpec<IN, OUT>, ? extends Spec.HttpServerSpec<IN, OUT>> configuringFunction
	) {
		return httpServer(DEFAULT_HTTP_SERVER_TYPE, configuringFunction);
	}

	/**
	 * @param serverFactory       a target implementation server class
	 * @param configuringFunction a factory to build server configuration (see also {@link HttpServerFactory}
	 * @param <IN>                incoming data type
	 * @param <OUT>               outgoing data type
	 * @return a simple HTTP server
	 */
	public static <IN, OUT> HttpServer<IN, OUT> httpServer(
	  Class<? extends HttpServer> serverFactory,
	  Function<? super Spec.HttpServerSpec<IN, OUT>, ? extends Spec.HttpServerSpec<IN, OUT>> configuringFunction
	) {
		return configuringFunction.apply(new Spec.HttpServerSpec<IN, OUT>(serverFactory)).get();
	}


	/**
	 * @return a simple HTTP client
	 */
	public static HttpClient<Buffer, Buffer> httpClient() {
		return httpClient(new Function<Spec.HttpClientSpec<Buffer, Buffer>, Spec.HttpClientSpec<Buffer, Buffer>>() {
			@Override
			public Spec.HttpClientSpec<Buffer, Buffer> apply(Spec.HttpClientSpec<Buffer, Buffer> clientSpec) {
				clientSpec.timer(Timers.globalOrNull());
				return clientSpec;
			}
		});
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
	 * - onNext {@link reactor.io.net.ChannelStream} to consume data from
	 * - onComplete when client is shutdown
	 * - onError when any error (more specifically IO error) occurs
	 * From the emitted {@link ReactorChannel}, one can decide to add in-channel consumers to read any incoming
	 * data.
	 * <p>
	 * To reply data on the active connection, {@link ReactorChannel#writeWith} can subscribe to any passed {@link org
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
	 * By default the type of emitted data or received data is {@link reactor.io.buffer.Buffer}
	 *
	 * @param configuringFunction a function will apply and return a {@link reactor.io.net.Spec} to customize the peer
	 * @param <IN>                the given input type received by this peer. Any configured codec decoder must match
	 *                            this type.
	 * @param <OUT>               the given output type received by this peer. Any configured codec encoder must match
	 *                            this type.
	 * @return a new Stream of ChannelStream, typically a peer of connections.
	 */
	public static <IN, OUT> HttpClient<IN, OUT> httpClient(
	  Function<? super Spec.HttpClientSpec<IN, OUT>, ? extends Spec.HttpClientSpec<IN, OUT>> configuringFunction
	) {
		return httpClient(DEFAULT_HTTP_CLIENT_TYPE, configuringFunction);
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
	 * - onNext {@link reactor.io.net.ChannelStream} to consume data from
	 * - onComplete when client is shutdown
	 * - onError when any error (more specifically IO error) occurs
	 * From the emitted {@link ReactorChannel}, one can decide to add in-channel consumers to read any incoming
	 * data.
	 * <p>
	 * To reply data on the active connection, {@link ReactorChannel#writeWith} can subscribe to any passed {@link org
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
	 * By default the type of emitted data or received data is {@link reactor.io.buffer.Buffer}
	 *
	 * @param clientFactory       the given implementation class for this peer
	 * @param configuringFunction a function will apply and return a {@link reactor.io.net.Spec} to customize the peer
	 * @param <IN>                the given input type received by this peer. Any configured codec decoder must match
	 *                            this type.
	 * @param <OUT>               the given output type received by this peer. Any configured codec encoder must match
	 *                            this type.
	 * @return a new Stream of ChannelStream, typically a peer of connections.
	 */
	public static <IN, OUT> HttpClient<IN, OUT> httpClient(
	  Class<? extends HttpClient> clientFactory,
	  Function<? super Spec.HttpClientSpec<IN, OUT>, ? extends Spec.HttpClientSpec<IN, OUT>> configuringFunction
	) {
		return configuringFunction.apply(new Spec.HttpClientSpec<IN, OUT>(clientFactory)).get();
	}

	// UDP

	/**
	 * Bind a new UDP server to the "loopback" address. By default the default server implementation is scanned
	 * from the classpath on Class init. Support for Netty is provided as long as the relevant
	 * library dependencies are on the classpath.
	 * <p>
	 * <p>
	 * From the emitted {@link ReactorChannel}, one can decide to add in-channel consumers to read any incoming
	 * data.
	 * <p>
	 * To reply data on the active connection, {@link ReactorChannel#writeWith} can subscribe to any passed {@link org
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
	 * By default the type of emitted data or received data is {@link reactor.io.buffer.Buffer}
	 *
	 * @return a new Stream of ChannelStream, typically a peer of connections.
	 */
	public static DatagramServer<Buffer, Buffer> udpServer() {
		return udpServer(DEFAULT_BIND_ADDRESS);
	}

	/**
	 * Bind a new UDP server to the given bind address. By default the default server implementation is scanned
	 * from the classpath on Class init. Support for Netty is provided as long as the relevant
	 * library dependencies are on the classpath.
	 * <p>
	 * <p>
	 * From the emitted {@link ReactorChannel}, one can decide to add in-channel consumers to read any incoming
	 * data.
	 * <p>
	 * To reply data on the active connection, {@link ReactorChannel#writeWith} can subscribe to any passed {@link org
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
	 * By default the type of emitted data or received data is {@link reactor.io.buffer.Buffer}
	 *
	 * @param bindAddress bind address (e.g. "127.0.0.1") to create the server on the passed port
	 * @return a new Stream of ChannelStream, typically a peer of connections.
	 */
	public static DatagramServer<Buffer, Buffer> udpServer(String bindAddress) {
		return udpServer(bindAddress, DEFAULT_PORT);
	}

	/**
	 * Bind a new UDP server to the "loopback" address and specified port. By default the default server implementation
	 * is scanned
	 * from the classpath on Class init. Support for Netty is provided as long as the relevant
	 * library dependencies are on the classpath.
	 * <p>
	 * <p>
	 * From the emitted {@link ReactorChannel}, one can decide to add in-channel consumers to read any incoming
	 * data.
	 * <p>
	 * To reply data on the active connection, {@link ReactorChannel#writeWith} can subscribe to any passed {@link org
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
	 * By default the type of emitted data or received data is {@link reactor.io.buffer.Buffer}
	 *
	 * @param port the port to listen on the passed bind address
	 * @return a new Stream of ChannelStream, typically a peer of connections.
	 */
	public static DatagramServer<Buffer, Buffer> udpServer(int port) {
		return udpServer(DEFAULT_BIND_ADDRESS, port);
	}

	/**
	 * Bind a new UDP server to the given bind address and port. By default the default server implementation is
	 * scanned
	 * from the classpath on Class init. Support for Netty is provided as long as the relevant
	 * library dependencies are on the classpath.
	 * <p>
	 * <p>
	 * From the emitted {@link ReactorChannel}, one can decide to add in-channel consumers to read any incoming
	 * data.
	 * <p>
	 * To reply data on the active connection, {@link ReactorChannel#writeWith} can subscribe to any passed {@link org
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
	 * By default the type of emitted data or received data is {@link reactor.io.buffer.Buffer}
	 *
	 * @param port        the port to listen on the passed bind address
	 * @param bindAddress bind address (e.g. "127.0.0.1") to create the server on the passed port
	 * @return a new Stream of ChannelStream, typically a peer of connections.
	 */
	public static DatagramServer<Buffer, Buffer> udpServer(final String bindAddress, final int port) {
		return udpServer(new Function<Spec.DatagramServerSpec<Buffer, Buffer>, Spec.DatagramServerSpec<Buffer,
		  Buffer>>() {
			@Override
			public Spec.DatagramServerSpec<Buffer, Buffer> apply(Spec.DatagramServerSpec<Buffer, Buffer> serverSpec) {
				serverSpec.timer(Timers.globalOrNull());
				return serverSpec.listen(bindAddress, port);
			}
		});
	}

	/**
	 * Bind a new UDP server to the specified bind address and port. By default the default server implementation is
	 * scanned
	 * from the classpath on Class init. Support for Netty is provided as long as the relevant
	 * library dependencies are on the classpath.
	 * <p>
	 * <p>
	 * From the emitted {@link ReactorChannel}, one can decide to add in-channel consumers to read any incoming
	 * data.
	 * <p>
	 * To reply data on the active connection, {@link ReactorChannel#writeWith} can subscribe to any passed {@link org
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
	 * By default the type of emitted data or received data is {@link reactor.io.buffer.Buffer}
	 *
	 * @param configuringFunction a function will apply and return a {@link reactor.io.net.Spec} to customize the peer
	 * @param <IN>                the given input type received by this peer. Any configured codec decoder must match
	 *                            this type.
	 * @param <OUT>               the given output type received by this peer. Any configured codec encoder must match
	 *                            this type.
	 * @return a new Stream of ChannelStream, typically a peer of connections.
	 */
	public static <IN, OUT> DatagramServer<IN, OUT> udpServer(
	  Function<? super Spec.DatagramServerSpec<IN, OUT>, ? extends Spec.DatagramServerSpec<IN, OUT>>
		configuringFunction
	) {
		return udpServer(DEFAULT_UDP_SERVER_TYPE, configuringFunction);
	}

	/**
	 * Bind a new UDP server to the specified bind address and port.
	 * <p>
	 * <p>
	 * From the emitted {@link ReactorChannel}, one can decide to add in-channel consumers to read any incoming
	 * data.
	 * <p>
	 * To reply data on the active connection, {@link ReactorChannel#writeWith} can subscribe to any passed {@link org
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
	 * By default the type of emitted data or received data is {@link reactor.io.buffer.Buffer}
	 *
	 * @param serverFactory       the given implementation class for this peer
	 * @param configuringFunction a function will apply and return a {@link reactor.io.net.Spec} to customize the peer
	 * @param <IN>                the given input type received by this peer. Any configured codec decoder must match
	 *                            this type.
	 * @param <OUT>               the given output type received by this peer. Any configured codec encoder must match
	 *                            this type.
	 * @return a new Stream of ChannelStream, typically a peer of connections.
	 */
	public static <IN, OUT> DatagramServer<IN, OUT> udpServer(
	  Class<? extends DatagramServer> serverFactory,
	  Function<? super Spec.DatagramServerSpec<IN, OUT>, ? extends Spec.DatagramServerSpec<IN, OUT>>
		configuringFunction
	) {
		return configuringFunction.apply(new Spec.DatagramServerSpec<IN, OUT>(serverFactory)).get();
	}


	/**
	 * Utils to read the ChannelStream underlying channel
	 */

	@SuppressWarnings("unchecked")
	public static <E, IN, OUT> E delegate(ChannelStream<IN, OUT> channelStream) {
		return (E) delegate(channelStream, Object.class);
	}

	@SuppressWarnings("unchecked")
	public static <E, IN, OUT> E delegate(ChannelStream<IN, OUT> channelStream, Class<E> clazz) {
		Assert.isTrue(
		  clazz.isAssignableFrom(channelStream.delegate().getClass()),
		  "Underlying channel is not of the given type: " + clazz.getName()
		);

		return (E) channelStream.delegate();
	}

	/**
	 * @return a Specification to configure and supply a Reconnect handler
	 */
	static public Spec.IncrementalBackoffReconnect backoffReconnect() {
		return new Spec.IncrementalBackoffReconnect();
	}

	/**
	 * INTERNAL CLASSPATH INIT
	 */

	static {
		boolean hasNetty = false;
		try {
			Class.forName("io.netty.channel.Channel");
			hasNetty = true;
		} catch (ClassNotFoundException cnfe) {
			//IGNORE
		}
		if (hasNetty) {
			DEFAULT_TCP_SERVER_TYPE = NettyTcpServer.class;
			DEFAULT_TCP_CLIENT_TYPE = NettyTcpClient.class;
			DEFAULT_UDP_SERVER_TYPE = NettyDatagramServer.class;
			DEFAULT_HTTP_SERVER_TYPE = NettyHttpServer.class;
			DEFAULT_HTTP_CLIENT_TYPE = NettyHttpClient.class;
		} else {
			DEFAULT_UDP_SERVER_TYPE = null;
			DEFAULT_HTTP_SERVER_TYPE = null;
			DEFAULT_HTTP_CLIENT_TYPE = null;

			Class<? extends TcpServer> zmqServer = null;
			Class<? extends TcpClient> zmqClient = null;
			try {
				zmqServer = zmqServerClazz();
				zmqClient = zmqClientClazz();
			} catch (ClassNotFoundException cnfe) {
				//IGNORE
			}

			DEFAULT_TCP_SERVER_TYPE = zmqServer;
			DEFAULT_TCP_CLIENT_TYPE = zmqClient;
		}

	}

	@SuppressWarnings("unchecked")
	private static Class<? extends TcpServer> zmqServerClazz() throws ClassNotFoundException{
		return (Class<? extends TcpServer>)
				Class.forName("reactor.io.net.impl.zmq.tcp.ZeroMQTcpServer");
	}

	@SuppressWarnings("unchecked")
	private static Class<? extends TcpClient> zmqClientClazz() throws ClassNotFoundException{
		return (Class<? extends TcpClient>)
				Class.forName("reactor.io.net.impl.zmq.tcp.ZeroMQTcpClient");
	}
}
