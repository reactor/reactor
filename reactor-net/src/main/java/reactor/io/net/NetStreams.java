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

import reactor.Environment;
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
import reactor.io.net.impl.zmq.tcp.ZeroMQTcpClient;
import reactor.io.net.impl.zmq.tcp.ZeroMQTcpServer;
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
 * NetStreams.tcpServer(1234).pipeline( connection -> connection );
 *
 * NetStreams.tcpClient(1234).pipeline( connection ->
 *    connection
 *      //Listen for any incoming data on that connection, they will be Buffer an IOStream can easily decode
 *      .nest()
 *      .flatMap(self -> IOStreams.decode(new StringCodec('\n'), self))
 *      .consume(log::info);
 *
 *    //Push anything from the publisher returned, here a simple Reactor Stream. By default a Buffer is expected
 *    return Streams.just(Buffer.wrap("hello\n"));
 * });
 *
 * //We can also preconfigure global codecs and other custom client/server parameter with the Function signature:
 * NetStreams.tcpServer(spec -> spec.codec(kryoCodec).listen(1235)).pipeline( intput -> {
 *      input.consume(log::info);
 *      return Streams.period(1l);
 * });
 *
 * //Assigning the same codec to a client and a server greatly improve readability and provide for extended type safety.
 * NetStreams.tcpClient(spec -> spec.connect("localhost", 1235).codec(kryoCodec)).pipeline( input -> {
 *   input.consume(log::info);
 *   return Streams.just("hello");
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

	// TCP

	/**
	 * Bind a new TCP server to "loopback" on port {@literal 12012}. By default the default server implementation is scanned
	 * from the classpath on Class init. Support for Netty first and ZeroMQ then is provided as long as the relevant
	 * library dependencies are on the classpath.
	 *
	 * A {@link reactor.io.net.tcp.TcpServer} is a specific kind of {@link org.reactivestreams.Publisher} that will emit:
	 * - onNext {@link reactor.io.net.ChannelStream} to consume data from
	 * - onComplete when server is shutdown
	 * - onError when any exception (more specifically IO exception) occurs
	 * From the emitted {@link reactor.io.net.Channel}, one can decide to add in-channel consumers to read any incoming data.
	 *
	 * To reply data on the active connection, {@link Channel#sink} can subscribe to any passed {@link org.reactivestreams.Publisher}.
	 * An alternative is to use {@link reactor.io.net.tcp.TcpServer#pipeline} to both define the input consumers and the output
	 * producers in a single call;
	 *
	 * Note that {@link reactor.rx.Stream#getCapacity} will be used to switch on/off a channel in auto-read / flush on write mode.
	 * If the capacity is Long.MAX_Value, write on flush and auto read will apply. Otherwise, data will be flushed every
	 * capacity batch size and read will pause when capacity number of elements have been dispatched.
	 *
	 * Emitted channels will run on the same thread they have beem receiving IO events.
	 * Apart from dispatching the write, it is possible to use {@link reactor.rx.Stream#dispatchOn} to process requests
	 * asynchronously.
	 *
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
	 *
	 * A {@link reactor.io.net.tcp.TcpServer} is a specific kind of {@link org.reactivestreams.Publisher} that will emit:
	 * - onNext {@link reactor.io.net.ChannelStream} to consume data from
	 * - onComplete when server is shutdown
	 * - onError when any exception (more specifically IO exception) occurs
	 * From the emitted {@link reactor.io.net.Channel}, one can decide to add in-channel consumers to read any incoming data.
	 *
	 * To reply data on the active connection, {@link Channel#sink} can subscribe to any passed {@link org.reactivestreams.Publisher}.
	 * An alternative is to use {@link reactor.io.net.tcp.TcpServer#pipeline} to both define the input consumers and the output
	 * producers in a single call;
	 *
	 * Note that {@link reactor.rx.Stream#getCapacity} will be used to switch on/off a channel in auto-read / flush on write mode.
	 * If the capacity is Long.MAX_Value, write on flush and auto read will apply. Otherwise, data will be flushed every
	 * capacity batch size and read will pause when capacity number of elements have been dispatched.
	 *
	 * Emitted channels will run on the same thread they have beem receiving IO events.
	 * Apart from dispatching the write, it is possible to use {@link reactor.rx.Stream#dispatchOn} to process requests
	 * asynchronously.
	 *
	 * By default the type of emitted data or received data is {@link reactor.io.buffer.Buffer}
	 *
	 * @param port the port to listen on loopback
	 * @return a new Stream of ChannelStream, typically a peer of connections.
	 */
	public static TcpServer<Buffer, Buffer> tcpServer(int port) {
		return tcpServer(DEFAULT_BIND_ADDRESS, port);
	}

	/**
	 * Bind a new TCP server to the given bind address on port {@literal 12012}. By default the default server implementation is scanned
	 * from the classpath on Class init. Support for Netty first and ZeroMQ then is provided as long as the relevant
	 * library dependencies are on the classpath.
	 *
	 * A {@link reactor.io.net.tcp.TcpServer} is a specific kind of {@link org.reactivestreams.Publisher} that will emit:
	 * - onNext {@link reactor.io.net.ChannelStream} to consume data from
	 * - onComplete when server is shutdown
	 * - onError when any exception (more specifically IO exception) occurs
	 * From the emitted {@link reactor.io.net.Channel}, one can decide to add in-channel consumers to read any incoming data.
	 *
	 * To reply data on the active connection, {@link Channel#sink} can subscribe to any passed {@link org.reactivestreams.Publisher}.
	 * An alternative is to use {@link reactor.io.net.tcp.TcpServer#pipeline} to both define the input consumers and the output
	 * producers in a single call;
	 *
	 * Note that {@link reactor.rx.Stream#getCapacity} will be used to switch on/off a channel in auto-read / flush on write mode.
	 * If the capacity is Long.MAX_Value, write on flush and auto read will apply. Otherwise, data will be flushed every
	 * capacity batch size and read will pause when capacity number of elements have been dispatched.
	 *
	 * Emitted channels will run on the same thread they have beem receiving IO events.
	 * Apart from dispatching the write, it is possible to use {@link reactor.rx.Stream#dispatchOn} to process requests
	 * asynchronously.
	 *
	 * By default the type of emitted data or received data is {@link reactor.io.buffer.Buffer}
	 *
	 * @param bindAddress bind address (e.g. "127.0.0.1") to create the server on the default port 12012
	 *
	 * @return a new Stream of ChannelStream, typically a peer of connections.
	 */
	public static TcpServer<Buffer, Buffer> tcpServer(String bindAddress) {
		return tcpServer(bindAddress, DEFAULT_PORT);
	}

	/**
	 * Bind a new TCP server to the given bind address and port. By default the default server implementation is scanned
	 * from the classpath on Class init. Support for Netty first and ZeroMQ then is provided as long as the relevant
	 * library dependencies are on the classpath.
	 *
	 * A {@link reactor.io.net.tcp.TcpServer} is a specific kind of {@link org.reactivestreams.Publisher} that will emit:
	 * - onNext {@link reactor.io.net.ChannelStream} to consume data from
	 * - onComplete when server is shutdown
	 * - onError when any exception (more specifically IO exception) occurs
	 * From the emitted {@link reactor.io.net.Channel}, one can decide to add in-channel consumers to read any incoming data.
	 *
	 * To reply data on the active connection, {@link Channel#sink} can subscribe to any passed {@link org.reactivestreams.Publisher}.
	 * An alternative is to use {@link reactor.io.net.tcp.TcpServer#pipeline} to both define the input consumers and the output
	 * producers in a single call;
	 *
	 * Note that {@link reactor.rx.Stream#getCapacity} will be used to switch on/off a channel in auto-read / flush on write mode.
	 * If the capacity is Long.MAX_Value, write on flush and auto read will apply. Otherwise, data will be flushed every
	 * capacity batch size and read will pause when capacity number of elements have been dispatched.
	 *
	 * Emitted channels will run on the same thread they have beem receiving IO events.
	 * Apart from dispatching the write, it is possible to use {@link reactor.rx.Stream#dispatchOn} to process requests
	 * asynchronously.
	 *
	 * By default the type of emitted data or received data is {@link reactor.io.buffer.Buffer}
	 *
	 * @param port the port to listen on the passed bind address
	 * @param bindAddress bind address (e.g. "127.0.0.1") to create the server on the passed port
	 *
	 * @return a new Stream of ChannelStream, typically a peer of connections.
	 */
	public static TcpServer<Buffer, Buffer> tcpServer(final String bindAddress, final int port) {
		return tcpServer(new Function<Spec.TcpServer<Buffer, Buffer>, Spec.TcpServer<Buffer, Buffer>>() {
			@Override
			public Spec.TcpServer<Buffer, Buffer> apply(Spec.TcpServer<Buffer, Buffer> serverSpec) {
				if(Environment.alive()){
					serverSpec.env(Environment.get());
				}
				return serverSpec.listen(bindAddress, port);
			}
		});
	}

	/**
	 * Bind a new TCP server to the specified bind address and port. By default the default server implementation is scanned
	 * from the classpath on Class init. Support for Netty first and ZeroMQ then is provided as long as the relevant
	 * library dependencies are on the classpath.
	 *
	 * A {@link reactor.io.net.tcp.TcpServer} is a specific kind of {@link org.reactivestreams.Publisher} that will emit:
	 * - onNext {@link reactor.io.net.ChannelStream} to consume data from
	 * - onComplete when server is shutdown
	 * - onError when any exception (more specifically IO exception) occurs
	 * From the emitted {@link reactor.io.net.Channel}, one can decide to add in-channel consumers to read any incoming data.
	 *
	 * To reply data on the active connection, {@link Channel#sink} can subscribe to any passed {@link org.reactivestreams.Publisher}.
	 * An alternative is to use {@link reactor.io.net.tcp.TcpServer#pipeline} to both define the input consumers and the output
	 * producers in a single call;
	 *
	 * Note that {@link reactor.rx.Stream#getCapacity} will be used to switch on/off a channel in auto-read / flush on write mode.
	 * If the capacity is Long.MAX_Value, write on flush and auto read will apply. Otherwise, data will be flushed every
	 * capacity batch size and read will pause when capacity number of elements have been dispatched.
	 *
	 * Emitted channels will run on the same thread they have beem receiving IO events.
	 * Apart from dispatching the write, it is possible to use {@link reactor.rx.Stream#dispatchOn} to process requests
	 * asynchronously.
	 *
	 * By default the type of emitted data or received data is {@link reactor.io.buffer.Buffer}
	 *
	 * @param configuringFunction a function will apply and return a {@link reactor.io.net.Spec} to customize the peer
	 * @param <IN> the given input type received by this peer. Any configured codec decoder must match this type.
	 * @param <OUT> the given output type received by this peer. Any configured codec encoder must match this type.
	 *
	 * @return a new Stream of ChannelStream, typically a peer of connections.
	 */
	public static <IN, OUT> TcpServer<IN, OUT> tcpServer(
			Function<? super Spec.TcpServer<IN, OUT>, ? extends Spec.TcpServer<IN, OUT>> configuringFunction
	) {
		return tcpServer(DEFAULT_TCP_SERVER_TYPE, configuringFunction);
	}

	/**
	 * Bind a new TCP server to the specified bind address and port.
	 *
	 * A {@link reactor.io.net.tcp.TcpServer} is a specific kind of {@link org.reactivestreams.Publisher} that will emit:
	 * - onNext {@link reactor.io.net.ChannelStream} to consume data from
	 * - onComplete when server is shutdown
	 * - onError when any exception (more specifically IO exception) occurs
	 * From the emitted {@link reactor.io.net.Channel}, one can decide to add in-channel consumers to read any incoming data.
	 *
	 * To reply data on the active connection, {@link Channel#sink} can subscribe to any passed {@link org.reactivestreams.Publisher}.
	 * An alternative is to use {@link reactor.io.net.tcp.TcpServer#pipeline} to both define the input consumers and the output
	 * producers in a single call;
	 *
	 * Note that {@link reactor.rx.Stream#getCapacity} will be used to switch on/off a channel in auto-read / flush on write mode.
	 * If the capacity is Long.MAX_Value, write on flush and auto read will apply. Otherwise, data will be flushed every
	 * capacity batch size and read will pause when capacity number of elements have been dispatched.
	 *
	 * Emitted channels will run on the same thread they have beem receiving IO events.
	 * Apart from dispatching the write, it is possible to use {@link reactor.rx.Stream#dispatchOn} to process requests
	 * asynchronously.
	 *
	 * By default the type of emitted data or received data is {@link reactor.io.buffer.Buffer}
	 *
	 * @param serverFactory the given implementation class for this peer
	 * @param configuringFunction a function will apply and return a {@link reactor.io.net.Spec} to customize the peer
	 * @param <IN> the given input type received by this peer. Any configured codec decoder must match this type.
	 * @param <OUT> the given output type received by this peer. Any configured codec encoder must match this type.
	 *
	 * @return a new Stream of ChannelStream, typically a peer of connections.
	 */
	public static <IN, OUT> TcpServer<IN, OUT> tcpServer(
			Class<? extends TcpServer> serverFactory,
			Function<? super Spec.TcpServer<IN, OUT>, ? extends Spec.TcpServer<IN, OUT>> configuringFunction
	) {
		return configuringFunction.apply(new Spec.TcpServer<IN, OUT>(serverFactory)).get();
	}


	/**
	 * Bind a new TCP client to the localhost on port 12012. By default the default client implementation is scanned
	 * from the classpath on Class init. Support for Netty first and ZeroMQ then is provided as long as the relevant
	 * library dependencies are on the classpath.
	 *
	 * A {@link reactor.io.net.tcp.TcpClient} is a specific kind of {@link org.reactivestreams.Publisher} that will emit:
	 * - onNext {@link reactor.io.net.ChannelStream} to consume data from
	 * - onComplete when client is shutdown
	 * - onError when any exception (more specifically IO exception) occurs
	 * From the emitted {@link reactor.io.net.Channel}, one can decide to add in-channel consumers to read any incoming data.
	 *
	 * To reply data on the active connection, {@link Channel#sink} can subscribe to any passed {@link org.reactivestreams.Publisher}.
	 * An alternative is to use {@link reactor.io.net.tcp.TcpClient#pipeline} to both define the input consumers and the output
	 * producers in a single call;
	 *
	 * Note that {@link reactor.rx.Stream#getCapacity} will be used to switch on/off a channel in auto-read / flush on write mode.
	 * If the capacity is Long.MAX_Value, write on flush and auto read will apply. Otherwise, data will be flushed every
	 * capacity batch size and read will pause when capacity number of elements have been dispatched.
	 *
	 * Emitted channels will run on the same thread they have beem receiving IO events.
	 * Apart from dispatching the write, it is possible to use {@link reactor.rx.Stream#dispatchOn} to process requests
	 * asynchronously.
	 *
	 * By default the type of emitted data or received data is {@link reactor.io.buffer.Buffer}
	 *
	 *
	 * @return a new Stream of ChannelStream, typically a peer of connections.
	 */
	public static TcpClient<Buffer, Buffer> tcpClient() {
		return tcpClient(DEFAULT_BIND_ADDRESS);
	}

	/**
	 * Bind a new TCP client to the specified connect address and port 12012. By default the default client implementation is scanned
	 * from the classpath on Class init. Support for Netty first and ZeroMQ then is provided as long as the relevant
	 * library dependencies are on the classpath.
	 *
	 * A {@link reactor.io.net.tcp.TcpClient} is a specific kind of {@link org.reactivestreams.Publisher} that will emit:
	 * - onNext {@link reactor.io.net.ChannelStream} to consume data from
	 * - onComplete when client is shutdown
	 * - onError when any exception (more specifically IO exception) occurs
	 * From the emitted {@link reactor.io.net.Channel}, one can decide to add in-channel consumers to read any incoming data.
	 *
	 * To reply data on the active connection, {@link Channel#sink} can subscribe to any passed {@link org.reactivestreams.Publisher}.
	 * An alternative is to use {@link reactor.io.net.tcp.TcpClient#pipeline} to both define the input consumers and the output
	 * producers in a single call;
	 *
	 * Note that {@link reactor.rx.Stream#getCapacity} will be used to switch on/off a channel in auto-read / flush on write mode.
	 * If the capacity is Long.MAX_Value, write on flush and auto read will apply. Otherwise, data will be flushed every
	 * capacity batch size and read will pause when capacity number of elements have been dispatched.
	 *
	 * Emitted channels will run on the same thread they have beem receiving IO events.
	 * Apart from dispatching the write, it is possible to use {@link reactor.rx.Stream#dispatchOn} to process requests
	 * asynchronously.
	 *
	 * By default the type of emitted data or received data is {@link reactor.io.buffer.Buffer}
	 *
	 * @param bindAddress the address to connect to on port 12012
	 *
	 * @return a new Stream of ChannelStream, typically a peer of connections.
	 */
	public static TcpClient<Buffer, Buffer> tcpClient(String bindAddress) {
		return tcpClient(bindAddress, DEFAULT_PORT);
	}

	/**
	 * Bind a new TCP client to "loopback" on the the specified port. By default the default client implementation is scanned
	 * from the classpath on Class init. Support for Netty first and ZeroMQ then is provided as long as the relevant
	 * library dependencies are on the classpath.
	 *
	 * A {@link reactor.io.net.tcp.TcpClient} is a specific kind of {@link org.reactivestreams.Publisher} that will emit:
	 * - onNext {@link reactor.io.net.ChannelStream} to consume data from
	 * - onComplete when client is shutdown
	 * - onError when any exception (more specifically IO exception) occurs
	 * From the emitted {@link reactor.io.net.Channel}, one can decide to add in-channel consumers to read any incoming data.
	 *
	 * To reply data on the active connection, {@link Channel#sink} can subscribe to any passed {@link org.reactivestreams.Publisher}.
	 * An alternative is to use {@link reactor.io.net.tcp.TcpClient#pipeline} to both define the input consumers and the output
	 * producers in a single call;
	 *
	 * Note that {@link reactor.rx.Stream#getCapacity} will be used to switch on/off a channel in auto-read / flush on write mode.
	 * If the capacity is Long.MAX_Value, write on flush and auto read will apply. Otherwise, data will be flushed every
	 * capacity batch size and read will pause when capacity number of elements have been dispatched.
	 *
	 * Emitted channels will run on the same thread they have beem receiving IO events.
	 * Apart from dispatching the write, it is possible to use {@link reactor.rx.Stream#dispatchOn} to process requests
	 * asynchronously.
	 *
	 * By default the type of emitted data or received data is {@link reactor.io.buffer.Buffer}
	 *
	 * @param port the port to connect to on "loopback"
	 *
	 * @return a new Stream of ChannelStream, typically a peer of connections.
	 */
	public static TcpClient<Buffer, Buffer> tcpClient(int port) {
		return tcpClient(DEFAULT_BIND_ADDRESS, port);
	}

	/**
	 * Bind a new TCP client to the specified connect address and port. By default the default client implementation is scanned
	 * from the classpath on Class init. Support for Netty first and ZeroMQ then is provided as long as the relevant
	 * library dependencies are on the classpath.
	 *
	 * A {@link reactor.io.net.tcp.TcpClient} is a specific kind of {@link org.reactivestreams.Publisher} that will emit:
	 * - onNext {@link reactor.io.net.ChannelStream} to consume data from
	 * - onComplete when client is shutdown
	 * - onError when any exception (more specifically IO exception) occurs
	 * From the emitted {@link reactor.io.net.Channel}, one can decide to add in-channel consumers to read any incoming data.
	 *
	 * To reply data on the active connection, {@link Channel#sink} can subscribe to any passed {@link org.reactivestreams.Publisher}.
	 * An alternative is to use {@link reactor.io.net.tcp.TcpClient#pipeline} to both define the input consumers and the output
	 * producers in a single call;
	 *
	 * Note that {@link reactor.rx.Stream#getCapacity} will be used to switch on/off a channel in auto-read / flush on write mode.
	 * If the capacity is Long.MAX_Value, write on flush and auto read will apply. Otherwise, data will be flushed every
	 * capacity batch size and read will pause when capacity number of elements have been dispatched.
	 *
	 * Emitted channels will run on the same thread they have beem receiving IO events.
	 * Apart from dispatching the write, it is possible to use {@link reactor.rx.Stream#dispatchOn} to process requests
	 * asynchronously.
	 *
	 * By default the type of emitted data or received data is {@link reactor.io.buffer.Buffer}
	 *
	 * @param bindAddress the address to connect to
	 * @param port the port to connect to
	 *
	 * @return a new Stream of ChannelStream, typically a peer of connections.
	 */
	public static TcpClient<Buffer, Buffer> tcpClient(final String bindAddress, final int port) {
		return tcpClient(new Function<Spec.TcpClient<Buffer, Buffer>, Spec.TcpClient<Buffer, Buffer>>() {
			@Override
			public Spec.TcpClient<Buffer, Buffer> apply(Spec.TcpClient<Buffer, Buffer> clientSpec) {
				if(Environment.alive()){
					clientSpec.env(Environment.get());
				}
				return clientSpec.connect(bindAddress, port);
			}
		});
	}

	/**
	 * Bind a new TCP client to the specified connect address and port. By default the default client implementation is scanned
	 * from the classpath on Class init. Support for Netty first and ZeroMQ then is provided as long as the relevant
	 * library dependencies are on the classpath.
	 *
	 * A {@link reactor.io.net.tcp.TcpClient} is a specific kind of {@link org.reactivestreams.Publisher} that will emit:
	 * - onNext {@link reactor.io.net.ChannelStream} to consume data from
	 * - onComplete when client is shutdown
	 * - onError when any exception (more specifically IO exception) occurs
	 * From the emitted {@link reactor.io.net.Channel}, one can decide to add in-channel consumers to read any incoming data.
	 *
	 * To reply data on the active connection, {@link Channel#sink} can subscribe to any passed {@link org.reactivestreams.Publisher}.
	 * An alternative is to use {@link reactor.io.net.tcp.TcpClient#pipeline} to both define the input consumers and the output
	 * producers in a single call;
	 *
	 * Note that {@link reactor.rx.Stream#getCapacity} will be used to switch on/off a channel in auto-read / flush on write mode.
	 * If the capacity is Long.MAX_Value, write on flush and auto read will apply. Otherwise, data will be flushed every
	 * capacity batch size and read will pause when capacity number of elements have been dispatched.
	 *
	 * Emitted channels will run on the same thread they have beem receiving IO events.
	 * Apart from dispatching the write, it is possible to use {@link reactor.rx.Stream#dispatchOn} to process requests
	 * asynchronously.
	 *
	 * By default the type of emitted data or received data is {@link reactor.io.buffer.Buffer}
	 *
	 * @param configuringFunction a function will apply and return a {@link reactor.io.net.Spec} to customize the peer
	 * @param <IN> the given input type received by this peer. Any configured codec decoder must match this type.
	 * @param <OUT> the given output type received by this peer. Any configured codec encoder must match this type.
	 *
	 * @return a new Stream of ChannelStream, typically a peer of connections.
	 */
	public static <IN, OUT> TcpClient<IN, OUT> tcpClient(
			Function<? super Spec.TcpClient<IN, OUT>, ? extends Spec.TcpClient<IN, OUT>> configuringFunction
	) {
		return tcpClient(DEFAULT_TCP_CLIENT_TYPE, configuringFunction);
	}

	/**
	 * Bind a new TCP client to the specified connect address and port.
	 *
	 * A {@link reactor.io.net.tcp.TcpClient} is a specific kind of {@link org.reactivestreams.Publisher} that will emit:
	 * - onNext {@link reactor.io.net.ChannelStream} to consume data from
	 * - onComplete when client is shutdown
	 * - onError when any exception (more specifically IO exception) occurs
	 * From the emitted {@link reactor.io.net.Channel}, one can decide to add in-channel consumers to read any incoming data.
	 *
	 * To reply data on the active connection, {@link Channel#sink} can subscribe to any passed {@link org.reactivestreams.Publisher}.
	 * An alternative is to use {@link reactor.io.net.tcp.TcpClient#pipeline} to both define the input consumers and the output
	 * producers in a single call;
	 *
	 * Note that {@link reactor.rx.Stream#getCapacity} will be used to switch on/off a channel in auto-read / flush on write mode.
	 * If the capacity is Long.MAX_Value, write on flush and auto read will apply. Otherwise, data will be flushed every
	 * capacity batch size and read will pause when capacity number of elements have been dispatched.
	 *
	 * Emitted channels will run on the same thread they have beem receiving IO events.
	 * Apart from dispatching the write, it is possible to use {@link reactor.rx.Stream#dispatchOn} to process requests
	 * asynchronously.
	 *
	 * By default the type of emitted data or received data is {@link reactor.io.buffer.Buffer}
	 *
	 * @param clientFactory the given implementation class for this peer
	 * @param configuringFunction a function will apply and return a {@link reactor.io.net.Spec} to customize the peer
	 * @param <IN> the given input type received by this peer. Any configured codec decoder must match this type.
	 * @param <OUT> the given output type received by this peer. Any configured codec encoder must match this type.
	 *
	 * @return a new Stream of ChannelStream, typically a peer of connections.
	 */
	public static <IN, OUT> TcpClient<IN, OUT> tcpClient(
			Class<? extends TcpClient> clientFactory,
			Function<? super Spec.TcpClient<IN, OUT>, ? extends Spec.TcpClient<IN, OUT>> configuringFunction
	) {
		return configuringFunction.apply(new Spec.TcpClient<IN, OUT>(clientFactory)).get();
	}

	// HTTP

	/**
	 * @return
	 */
	public static HttpServer<Buffer, Buffer> httpServer() {
		return httpServer(DEFAULT_BIND_ADDRESS);
	}

	/**
	 * @param bindAddress
	 * @return
	 */
	public static <IN, OUT> HttpServer<IN, OUT> httpServer(String bindAddress) {
		return httpServer(bindAddress, DEFAULT_PORT);
	}

	/**
	 * @param port
	 * @return
	 */
	public static <IN, OUT> HttpServer<IN, OUT> httpServer(int port) {
		return httpServer(DEFAULT_BIND_ADDRESS, port);
	}

	/**
	 * @param bindAddress
	 * @param port
	 * @return
	 */
	public static <IN, OUT> HttpServer<IN, OUT> httpServer(final String bindAddress, final int port) {
		return httpServer(new Function<Spec.HttpServer<IN, OUT>, Spec.HttpServer<IN, OUT>>() {
			@Override
			public Spec.HttpServer<IN, OUT> apply(Spec.HttpServer<IN, OUT> serverSpec) {
				if(Environment.alive()){
					serverSpec.env(Environment.get());
				}
				return serverSpec.listen(bindAddress, port);
			}
		});
	}

	/**
	 * @param configuringFunction
	 * @param <IN>
	 * @param <OUT>
	 * @return
	 */
	public static <IN, OUT> HttpServer<IN, OUT> httpServer(
			Function<? super Spec.HttpServer<IN, OUT>, ? extends Spec.HttpServer<IN, OUT>> configuringFunction
	) {
		return httpServer(DEFAULT_HTTP_SERVER_TYPE, configuringFunction);
	}

	/**
	 * @param serverFactory
	 * @param configuringFunction
	 * @param <IN>
	 * @param <OUT>
	 * @return
	 */
	public static <IN, OUT> HttpServer<IN, OUT> httpServer(
			Class<? extends HttpServer> serverFactory,
			Function<? super Spec.HttpServer<IN, OUT>, ? extends Spec.HttpServer<IN, OUT>> configuringFunction
	) {
		return configuringFunction.apply(new Spec.HttpServer<IN, OUT>(serverFactory)).get();
	}


	/**
	 * @return
	 */
	public static HttpClient<Buffer, Buffer> httpClient() {
		return httpClient(DEFAULT_BIND_ADDRESS);
	}

	/**
	 * @param bindAddress
	 * @return
	 */
	public static  HttpClient<Buffer, Buffer> httpClient(String bindAddress) {
		return httpClient(bindAddress, DEFAULT_PORT);
	}

	/**
	 * @param port
	 * @return
	 */
	public static HttpClient<Buffer, Buffer> httpClient(int port) {
		return httpClient(DEFAULT_BIND_ADDRESS, port);
	}

	/**
	 * @param bindAddress
	 * @param port
	 * @return
	 */
	public static HttpClient<Buffer, Buffer> httpClient(final String bindAddress, final int port) {
		return httpClient(new Function<Spec.HttpClient<Buffer, Buffer>, Spec.HttpClient<Buffer, Buffer>>() {
			@Override
			public Spec.HttpClient<Buffer, Buffer> apply(Spec.HttpClient<Buffer, Buffer> clientSpec) {
				if(Environment.alive()){
					clientSpec.env(Environment.get());
				}
				return clientSpec.connect(bindAddress, port);
			}
		});
	}

	/**
	 * Bind a new HTTP client to the specified connect address and port. By default the default server implementation is scanned
	 * from the classpath on Class init. Support for Netty is provided as long as the relevant
	 * library dependencies are on the classpath.
	 *
	 * A {@link reactor.io.net.http.HttpClient} is a specific kind of {@link org.reactivestreams.Publisher} that will emit:
	 * - onNext {@link reactor.io.net.ChannelStream} to consume data from
	 * - onComplete when client is shutdown
	 * - onError when any exception (more specifically IO exception) occurs
	 * From the emitted {@link reactor.io.net.Channel}, one can decide to add in-channel consumers to read any incoming data.
	 *
	 * To reply data on the active connection, {@link Channel#sink} can subscribe to any passed {@link org.reactivestreams.Publisher}.
	 * An alternative is to use {@link reactor.io.net.http.HttpClient#pipeline} to both define the input consumers and the output
	 * producers in a single call;
	 *
	 * Note that {@link reactor.rx.Stream#getCapacity} will be used to switch on/off a channel in auto-read / flush on write mode.
	 * If the capacity is Long.MAX_Value, write on flush and auto read will apply. Otherwise, data will be flushed every
	 * capacity batch size and read will pause when capacity number of elements have been dispatched.
	 *
	 * Emitted channels will run on the same thread they have beem receiving IO events.
	 * Apart from dispatching the write, it is possible to use {@link reactor.rx.Stream#dispatchOn} to process requests
	 * asynchronously.
	 *
	 * By default the type of emitted data or received data is {@link reactor.io.buffer.Buffer}
	 *
	 * @param configuringFunction a function will apply and return a {@link reactor.io.net.Spec} to customize the peer
	 * @param <IN> the given input type received by this peer. Any configured codec decoder must match this type.
	 * @param <OUT> the given output type received by this peer. Any configured codec encoder must match this type.
	 *
	 * @return a new Stream of ChannelStream, typically a peer of connections.
	 */
	public static <IN, OUT> HttpClient<IN, OUT> httpClient(
			Function<? super Spec.HttpClient<IN, OUT>, ? extends Spec.HttpClient<IN, OUT>> configuringFunction
	) {
		return httpClient(DEFAULT_HTTP_CLIENT_TYPE, configuringFunction);
	}

	/**
	 * Bind a new HTTP client to the specified connect address and port. By default the default server implementation is scanned
	 * from the classpath on Class init. Support for Netty is provided as long as the relevant
	 * library dependencies are on the classpath.
	 *
	 * A {@link reactor.io.net.http.HttpClient} is a specific kind of {@link org.reactivestreams.Publisher} that will emit:
	 * - onNext {@link reactor.io.net.ChannelStream} to consume data from
	 * - onComplete when client is shutdown
	 * - onError when any exception (more specifically IO exception) occurs
	 * From the emitted {@link reactor.io.net.Channel}, one can decide to add in-channel consumers to read any incoming data.
	 *
	 * To reply data on the active connection, {@link Channel#sink} can subscribe to any passed {@link org.reactivestreams.Publisher}.
	 * An alternative is to use {@link reactor.io.net.http.HttpClient#pipeline} to both define the input consumers and the output
	 * producers in a single call;
	 *
	 * Note that {@link reactor.rx.Stream#getCapacity} will be used to switch on/off a channel in auto-read / flush on write mode.
	 * If the capacity is Long.MAX_Value, write on flush and auto read will apply. Otherwise, data will be flushed every
	 * capacity batch size and read will pause when capacity number of elements have been dispatched.
	 *
	 * Emitted channels will run on the same thread they have beem receiving IO events.
	 * Apart from dispatching the write, it is possible to use {@link reactor.rx.Stream#dispatchOn} to process requests
	 * asynchronously.
	 *
	 * By default the type of emitted data or received data is {@link reactor.io.buffer.Buffer}
	 *
	 * @param clientFactory the given implementation class for this peer
	 * @param configuringFunction a function will apply and return a {@link reactor.io.net.Spec} to customize the peer
	 * @param <IN> the given input type received by this peer. Any configured codec decoder must match this type.
	 * @param <OUT> the given output type received by this peer. Any configured codec encoder must match this type.
	 *
	 * @return a new Stream of ChannelStream, typically a peer of connections.
	 */
	public static <IN, OUT> HttpClient<IN, OUT> httpClient(
			Class<? extends HttpClient> clientFactory,
			Function<? super Spec.HttpClient<IN, OUT>, ? extends Spec.HttpClient<IN, OUT>> configuringFunction
	) {
		return configuringFunction.apply(new Spec.HttpClient<IN, OUT>(clientFactory)).get();
	}

	// UDP

	/**
	 * Bind a new UDP server to the "loopback" address. By default the default server implementation is scanned
	 * from the classpath on Class init. Support for Netty is provided as long as the relevant
	 * library dependencies are on the classpath.
	 *
	 * A {@link reactor.io.net.udp.DatagramServer} is a specific kind of {@link org.reactivestreams.Publisher} that will emit:
	 * - onNext {@link reactor.io.net.ChannelStream} to consume data from
	 * - onComplete when server is shutdown
	 * - onError when any exception (more specifically IO exception) occurs
	 * From the emitted {@link reactor.io.net.Channel}, one can decide to add in-channel consumers to read any incoming data.
	 *
	 * To reply data on the active connection, {@link Channel#sink} can subscribe to any passed {@link org.reactivestreams.Publisher}.
	 * An alternative is to use {@link reactor.io.net.udp.DatagramServer#pipeline} to both define the input consumers and the output
	 * producers in a single call;
	 *
	 * Note that {@link reactor.rx.Stream#getCapacity} will be used to switch on/off a channel in auto-read / flush on write mode.
	 * If the capacity is Long.MAX_Value, write on flush and auto read will apply. Otherwise, data will be flushed every
	 * capacity batch size and read will pause when capacity number of elements have been dispatched.
	 *
	 * Emitted channels will run on the same thread they have beem receiving IO events.
	 * Apart from dispatching the write, it is possible to use {@link reactor.rx.Stream#dispatchOn} to process requests
	 * asynchronously.
	 *
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
	 *
	 * A {@link reactor.io.net.udp.DatagramServer} is a specific kind of {@link org.reactivestreams.Publisher} that will emit:
	 * - onNext {@link reactor.io.net.ChannelStream} to consume data from
	 * - onComplete when server is shutdown
	 * - onError when any exception (more specifically IO exception) occurs
	 * From the emitted {@link reactor.io.net.Channel}, one can decide to add in-channel consumers to read any incoming data.
	 *
	 * To reply data on the active connection, {@link Channel#sink} can subscribe to any passed {@link org.reactivestreams.Publisher}.
	 * An alternative is to use {@link reactor.io.net.udp.DatagramServer#pipeline} to both define the input consumers and the output
	 * producers in a single call;
	 *
	 * Note that {@link reactor.rx.Stream#getCapacity} will be used to switch on/off a channel in auto-read / flush on write mode.
	 * If the capacity is Long.MAX_Value, write on flush and auto read will apply. Otherwise, data will be flushed every
	 * capacity batch size and read will pause when capacity number of elements have been dispatched.
	 *
	 * Emitted channels will run on the same thread they have beem receiving IO events.
	 * Apart from dispatching the write, it is possible to use {@link reactor.rx.Stream#dispatchOn} to process requests
	 * asynchronously.
	 *
	 * By default the type of emitted data or received data is {@link reactor.io.buffer.Buffer}
	 *
	 * @param bindAddress bind address (e.g. "127.0.0.1") to create the server on the passed port
	 *
	 * @return a new Stream of ChannelStream, typically a peer of connections.
	 */
	public static DatagramServer<Buffer, Buffer> udpServer(String bindAddress) {
		return udpServer(bindAddress, DEFAULT_PORT);
	}

	/**
	 * Bind a new UDP server to the "loopback" address and specified port. By default the default server implementation is scanned
	 * from the classpath on Class init. Support for Netty is provided as long as the relevant
	 * library dependencies are on the classpath.
	 *
	 * A {@link reactor.io.net.udp.DatagramServer} is a specific kind of {@link org.reactivestreams.Publisher} that will emit:
	 * - onNext {@link reactor.io.net.ChannelStream} to consume data from
	 * - onComplete when server is shutdown
	 * - onError when any exception (more specifically IO exception) occurs
	 * From the emitted {@link reactor.io.net.Channel}, one can decide to add in-channel consumers to read any incoming data.
	 *
	 * To reply data on the active connection, {@link Channel#sink} can subscribe to any passed {@link org.reactivestreams.Publisher}.
	 * An alternative is to use {@link reactor.io.net.udp.DatagramServer#pipeline} to both define the input consumers and the output
	 * producers in a single call;
	 *
	 * Note that {@link reactor.rx.Stream#getCapacity} will be used to switch on/off a channel in auto-read / flush on write mode.
	 * If the capacity is Long.MAX_Value, write on flush and auto read will apply. Otherwise, data will be flushed every
	 * capacity batch size and read will pause when capacity number of elements have been dispatched.
	 *
	 * Emitted channels will run on the same thread they have beem receiving IO events.
	 * Apart from dispatching the write, it is possible to use {@link reactor.rx.Stream#dispatchOn} to process requests
	 * asynchronously.
	 *
	 * By default the type of emitted data or received data is {@link reactor.io.buffer.Buffer}
	 *
	 * @param port the port to listen on the passed bind address
	 *
	 * @return a new Stream of ChannelStream, typically a peer of connections.
	 */
	public static DatagramServer<Buffer, Buffer> udpServer(int port) {
		return udpServer(DEFAULT_BIND_ADDRESS, port);
	}

	/**
	 * Bind a new UDP server to the given bind address and port. By default the default server implementation is scanned
	 * from the classpath on Class init. Support for Netty is provided as long as the relevant
	 * library dependencies are on the classpath.
	 *
	 * A {@link reactor.io.net.udp.DatagramServer} is a specific kind of {@link org.reactivestreams.Publisher} that will emit:
	 * - onNext {@link reactor.io.net.ChannelStream} to consume data from
	 * - onComplete when server is shutdown
	 * - onError when any exception (more specifically IO exception) occurs
	 * From the emitted {@link reactor.io.net.Channel}, one can decide to add in-channel consumers to read any incoming data.
	 *
	 * To reply data on the active connection, {@link Channel#sink} can subscribe to any passed {@link org.reactivestreams.Publisher}.
	 * An alternative is to use {@link reactor.io.net.udp.DatagramServer#pipeline} to both define the input consumers and the output
	 * producers in a single call;
	 *
	 * Note that {@link reactor.rx.Stream#getCapacity} will be used to switch on/off a channel in auto-read / flush on write mode.
	 * If the capacity is Long.MAX_Value, write on flush and auto read will apply. Otherwise, data will be flushed every
	 * capacity batch size and read will pause when capacity number of elements have been dispatched.
	 *
	 * Emitted channels will run on the same thread they have beem receiving IO events.
	 * Apart from dispatching the write, it is possible to use {@link reactor.rx.Stream#dispatchOn} to process requests
	 * asynchronously.
	 *
	 * By default the type of emitted data or received data is {@link reactor.io.buffer.Buffer}
	 *
	 * @param port the port to listen on the passed bind address
	 * @param bindAddress bind address (e.g. "127.0.0.1") to create the server on the passed port
	 *
	 * @return a new Stream of ChannelStream, typically a peer of connections.
	 */
	public static DatagramServer<Buffer, Buffer> udpServer(final String bindAddress, final int port) {
		return udpServer(new Function<Spec.DatagramServer<Buffer, Buffer>, Spec.DatagramServer<Buffer, Buffer>>() {
			@Override
			public Spec.DatagramServer<Buffer, Buffer> apply(Spec.DatagramServer<Buffer, Buffer> serverSpec) {
				if(Environment.alive()){
					serverSpec.env(Environment.get());
				}
				return serverSpec.listen(bindAddress, port);
			}
		});
	}

	/**
	 * Bind a new UDP server to the specified bind address and port. By default the default server implementation is scanned
	 * from the classpath on Class init. Support for Netty is provided as long as the relevant
	 * library dependencies are on the classpath.
	 *
	 * A {@link reactor.io.net.udp.DatagramServer} is a specific kind of {@link org.reactivestreams.Publisher} that will emit:
	 * - onNext {@link reactor.io.net.ChannelStream} to consume data from
	 * - onComplete when server is shutdown
	 * - onError when any exception (more specifically IO exception) occurs
	 * From the emitted {@link reactor.io.net.Channel}, one can decide to add in-channel consumers to read any incoming data.
	 *
	 * To reply data on the active connection, {@link Channel#sink} can subscribe to any passed {@link org.reactivestreams.Publisher}.
	 * An alternative is to use {@link reactor.io.net.udp.DatagramServer#pipeline} to both define the input consumers and the output
	 * producers in a single call;
	 *
	 * Note that {@link reactor.rx.Stream#getCapacity} will be used to switch on/off a channel in auto-read / flush on write mode.
	 * If the capacity is Long.MAX_Value, write on flush and auto read will apply. Otherwise, data will be flushed every
	 * capacity batch size and read will pause when capacity number of elements have been dispatched.
	 *
	 * Emitted channels will run on the same thread they have beem receiving IO events.
	 * Apart from dispatching the write, it is possible to use {@link reactor.rx.Stream#dispatchOn} to process requests
	 * asynchronously.
	 *
	 * By default the type of emitted data or received data is {@link reactor.io.buffer.Buffer}
	 *
	 * @param configuringFunction a function will apply and return a {@link reactor.io.net.Spec} to customize the peer
	 * @param <IN> the given input type received by this peer. Any configured codec decoder must match this type.
	 * @param <OUT> the given output type received by this peer. Any configured codec encoder must match this type.
	 *
	 * @return a new Stream of ChannelStream, typically a peer of connections.
	 */
	public static <IN, OUT> DatagramServer<IN, OUT> udpServer(
			Function<? super Spec.DatagramServer<IN, OUT>, ? extends Spec.DatagramServer<IN, OUT>> configuringFunction
	) {
		return udpServer(DEFAULT_UDP_SERVER_TYPE, configuringFunction);
	}

	/**
	 * Bind a new UDP server to the specified bind address and port.
	 *
	 * A {@link reactor.io.net.udp.DatagramServer} is a specific kind of {@link org.reactivestreams.Publisher} that will emit:
	 * - onNext {@link reactor.io.net.ChannelStream} to consume data from
	 * - onComplete when server is shutdown
	 * - onError when any exception (more specifically IO exception) occurs
	 * From the emitted {@link reactor.io.net.Channel}, one can decide to add in-channel consumers to read any incoming data.
	 *
	 * To reply data on the active connection, {@link Channel#sink} can subscribe to any passed {@link org.reactivestreams.Publisher}.
	 * An alternative is to use {@link reactor.io.net.udp.DatagramServer#pipeline} to both define the input consumers and the output
	 * producers in a single call;
	 *
	 * Note that {@link reactor.rx.Stream#getCapacity} will be used to switch on/off a channel in auto-read / flush on write mode.
	 * If the capacity is Long.MAX_Value, write on flush and auto read will apply. Otherwise, data will be flushed every
	 * capacity batch size and read will pause when capacity number of elements have been dispatched.
	 *
	 * Emitted channels will run on the same thread they have beem receiving IO events.
	 * Apart from dispatching the write, it is possible to use {@link reactor.rx.Stream#dispatchOn} to process requests
	 * asynchronously.
	 *
	 * By default the type of emitted data or received data is {@link reactor.io.buffer.Buffer}
	 *
	 * @param serverFactory the given implementation class for this peer
	 * @param configuringFunction a function will apply and return a {@link reactor.io.net.Spec} to customize the peer
	 * @param <IN> the given input type received by this peer. Any configured codec decoder must match this type.
	 * @param <OUT> the given output type received by this peer. Any configured codec encoder must match this type.
	 *
	 * @return a new Stream of ChannelStream, typically a peer of connections.
	 */
	public static <IN, OUT> DatagramServer<IN, OUT> udpServer(
			Class<? extends DatagramServer> serverFactory,
			Function<? super Spec.DatagramServer<IN, OUT>, ? extends Spec.DatagramServer<IN, OUT>> configuringFunction
	) {
		return configuringFunction.apply(new Spec.DatagramServer<IN, OUT>(serverFactory)).get();
	}


	/**
	 * Utils to read the ChannelStream underlying channel
	 */

	@SuppressWarnings("unchecked")
	public static <E, IN, OUT> E delegate(ChannelStream<IN, OUT> channelStream) {
		return (E)delegate(channelStream, Object.class);
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
			boolean hasZMQ = false;

			DEFAULT_UDP_SERVER_TYPE = null;
			DEFAULT_HTTP_SERVER_TYPE = null;
			DEFAULT_HTTP_CLIENT_TYPE = null;

			try {
				Class.forName("org.zeromq.ZMQ");
				hasZMQ = true;
			} catch (ClassNotFoundException cnfe) {
				//IGNORE
			}


			if (hasZMQ) {
				DEFAULT_TCP_SERVER_TYPE = ZeroMQTcpServer.class;
				DEFAULT_TCP_CLIENT_TYPE = ZeroMQTcpClient.class;
			} else {
				DEFAULT_TCP_SERVER_TYPE = null;
				DEFAULT_TCP_CLIENT_TYPE = null;
			}
		}

	}

	/**
	 * @return a Specification to configure and supply a Reconnect handler
	 */
	static public Spec.IncrementalBackoffReconnect backoffReconnect(){
		return new Spec.IncrementalBackoffReconnect();
	}
}
