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

import reactor.core.support.Assert;
import reactor.fn.Function;
import reactor.io.net.http.HttpClient;
import reactor.io.net.http.HttpServer;
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
 * NetStreams.tcpServer(1234).service( connection ->
 *   connection
 * )
 *
 * NetStreams.tcpClient(1234).connect( output ->
 *   output
 *   .sendAndReceive(Buffer.wrap("hello"))
 *   .onSuccess(log::info)
 * )
 *
 * NetStreams.tcpServer(spec -> spec.listen(1234)).service( intput, output -> {
 *      input.consume(log::info);
 *     Streams.period(1l).subscribe(output);
 * })
 *
 * NetStreams.tcpClient(spec -> spec.codec(kryoCodec)).connect( output, input -> {
 *   input.consume(log::info);
 *   output.send("hello");
 * })
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
	 * @param <IN>
	 * @param <OUT>
	 * @return
	 */
	public static <IN, OUT> TcpServer<IN, OUT> tcpServer() {
		return tcpServer(DEFAULT_BIND_ADDRESS);
	}

	/**
	 * @param port
	 * @param <IN>
	 * @param <OUT>
	 * @return
	 */
	public static <IN, OUT> TcpServer<IN, OUT> tcpServer(int port) {
		return tcpServer(DEFAULT_BIND_ADDRESS, port);
	}

	/**
	 * @param bindAddress
	 * @param <IN>
	 * @param <OUT>
	 * @return
	 */
	public static <IN, OUT> TcpServer<IN, OUT> tcpServer(String bindAddress) {
		return tcpServer(bindAddress, DEFAULT_PORT);
	}

	/**
	 * @param bindAddress
	 * @param port
	 * @param <IN>
	 * @param <OUT>
	 * @return
	 */
	public static <IN, OUT> TcpServer<IN, OUT> tcpServer(final String bindAddress, final int port) {
		return tcpServer(new Function<Spec.TcpServer<IN, OUT>, Spec.TcpServer<IN, OUT>>() {
			@Override
			public Spec.TcpServer<IN, OUT> apply(Spec.TcpServer<IN, OUT> serverSpec) {
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
	public static <IN, OUT> TcpServer<IN, OUT> tcpServer(
			Function<? super Spec.TcpServer<IN, OUT>, ? extends Spec.TcpServer<IN, OUT>> configuringFunction
	) {
		return tcpServer(DEFAULT_TCP_SERVER_TYPE, configuringFunction);
	}

	/**
	 * @param serverFactory
	 * @param configuringFunction
	 * @param <IN>
	 * @param <OUT>
	 * @return
	 */
	public static <IN, OUT> TcpServer<IN, OUT> tcpServer(
			Class<? extends TcpServer> serverFactory,
			Function<? super Spec.TcpServer<IN, OUT>, ? extends Spec.TcpServer<IN, OUT>> configuringFunction
	) {
		return configuringFunction.apply(new Spec.TcpServer<IN, OUT>(serverFactory)).get();
	}


	/**
	 * @param <IN>
	 * @param <OUT>
	 * @return
	 */
	public static <IN, OUT> TcpClient<IN, OUT> tcpClient() {
		return tcpClient(DEFAULT_BIND_ADDRESS);
	}

	/**
	 * @param bindAddress
	 * @param <IN>
	 * @param <OUT>
	 * @return
	 */
	public static <IN, OUT> TcpClient<IN, OUT> tcpClient(String bindAddress) {
		return tcpClient(bindAddress, DEFAULT_PORT);
	}

	/**
	 * @param port
	 * @param <IN>
	 * @param <OUT>
	 * @return
	 */
	public static <IN, OUT> TcpClient<IN, OUT> tcpClient(int port) {
		return tcpClient(DEFAULT_BIND_ADDRESS, port);
	}

	/**
	 * @param bindAddress
	 * @param port
	 * @param <IN>
	 * @param <OUT>
	 * @return
	 */
	public static <IN, OUT> TcpClient<IN, OUT> tcpClient(final String bindAddress, final int port) {
		return tcpClient(new Function<Spec.TcpClient<IN, OUT>, Spec.TcpClient<IN, OUT>>() {
			@Override
			public Spec.TcpClient<IN, OUT> apply(Spec.TcpClient<IN, OUT> clientSpec) {
				return clientSpec.connect(bindAddress, port);
			}
		});
	}

	/**
	 * @param configuringFunction
	 * @param <IN>
	 * @param <OUT>
	 * @return
	 */
	public static <IN, OUT> TcpClient<IN, OUT> tcpClient(
			Function<? super Spec.TcpClient<IN, OUT>, ? extends Spec.TcpClient<IN, OUT>> configuringFunction
	) {
		return tcpClient(DEFAULT_TCP_CLIENT_TYPE, configuringFunction);
	}

	/**
	 * @param clientFactory
	 * @param configuringFunction
	 * @param <IN>
	 * @param <OUT>
	 * @return
	 */
	public static <IN, OUT> TcpClient<IN, OUT> tcpClient(
			Class<? extends TcpClient> clientFactory,
			Function<? super Spec.TcpClient<IN, OUT>, ? extends Spec.TcpClient<IN, OUT>> configuringFunction
	) {
		return configuringFunction.apply(new Spec.TcpClient<IN, OUT>(clientFactory)).get();
	}

	// HTTP

	/**
	 * @param <IN>
	 * @param <OUT>
	 * @return
	 */
	public static <IN, OUT> HttpServer<IN, OUT> httpServer() {
		return httpServer(DEFAULT_BIND_ADDRESS);
	}

	/**
	 * @param bindAddress
	 * @param <IN>
	 * @param <OUT>
	 * @return
	 */
	public static <IN, OUT> HttpServer<IN, OUT> httpServer(String bindAddress) {
		return httpServer(bindAddress, DEFAULT_PORT);
	}

	/**
	 * @param port
	 * @param <IN>
	 * @param <OUT>
	 * @return
	 */
	public static <IN, OUT> HttpServer<IN, OUT> httpServer(int port) {
		return httpServer(DEFAULT_BIND_ADDRESS, port);
	}

	/**
	 * @param bindAddress
	 * @param port
	 * @param <IN>
	 * @param <OUT>
	 * @return
	 */
	public static <IN, OUT> HttpServer<IN, OUT> httpServer(final String bindAddress, final int port) {
		return httpServer(new Function<Spec.HttpServer<IN, OUT>, Spec.HttpServer<IN, OUT>>() {
			@Override
			public Spec.HttpServer<IN, OUT> apply(Spec.HttpServer<IN, OUT> serverSpec) {
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
	 * @param <IN>
	 * @param <OUT>
	 * @return
	 */
	public static <IN, OUT> HttpClient<IN, OUT> httpClient() {
		return httpClient(DEFAULT_BIND_ADDRESS);
	}

	/**
	 * @param bindAddress
	 * @param <IN>
	 * @param <OUT>
	 * @return
	 */
	public static <IN, OUT> HttpClient<IN, OUT> httpClient(String bindAddress) {
		return httpClient(bindAddress, DEFAULT_PORT);
	}

	/**
	 * @param port
	 * @param <IN>
	 * @param <OUT>
	 * @return
	 */
	public static <IN, OUT> HttpClient<IN, OUT> httpClient(int port) {
		return httpClient(DEFAULT_BIND_ADDRESS, port);
	}

	/**
	 * @param bindAddress
	 * @param port
	 * @param <IN>
	 * @param <OUT>
	 * @return
	 */
	public static <IN, OUT> HttpClient<IN, OUT> httpClient(final String bindAddress, final int port) {
		return httpClient(new Function<Spec.HttpClient<IN, OUT>, Spec.HttpClient<IN, OUT>>() {
			@Override
			public Spec.HttpClient<IN, OUT> apply(Spec.HttpClient<IN, OUT> clientSpec) {
				return clientSpec.connect(bindAddress, port);
			}
		});
	}

	/**
	 * @param configuringFunction
	 * @param <IN>
	 * @param <OUT>
	 * @return
	 */
	public static <IN, OUT> HttpClient<IN, OUT> httpClient(
			Function<? super Spec.HttpClient<IN, OUT>, ? extends Spec.HttpClient<IN, OUT>> configuringFunction
	) {
		return httpClient(DEFAULT_HTTP_CLIENT_TYPE, configuringFunction);
	}

	/**
	 * @param clientFactory
	 * @param configuringFunction
	 * @param <IN>
	 * @param <OUT>
	 * @return
	 */
	public static <IN, OUT> HttpClient<IN, OUT> httpClient(
			Class<? extends HttpClient> clientFactory,
			Function<? super Spec.HttpClient<IN, OUT>, ? extends Spec.HttpClient<IN, OUT>> configuringFunction
	) {
		return configuringFunction.apply(new Spec.HttpClient<IN, OUT>(clientFactory)).get();
	}

	// UDP

	/**
	 * @param <IN>
	 * @param <OUT>
	 * @return
	 */
	public static <IN, OUT> DatagramServer<IN, OUT> udpServer() {
		return udpServer(DEFAULT_BIND_ADDRESS);
	}

	/**
	 * @param bindAddress
	 * @param <IN>
	 * @param <OUT>
	 * @return
	 */
	public static <IN, OUT> DatagramServer udpServer(String bindAddress) {
		return udpServer(bindAddress, DEFAULT_PORT);
	}

	/**
	 * @param port
	 * @param <IN>
	 * @param <OUT>
	 * @return
	 */
	public static <IN, OUT> DatagramServer udpServer(int port) {
		return udpServer(DEFAULT_BIND_ADDRESS, port);
	}

	/**
	 * @param bindAddress
	 * @param port
	 * @param <IN>
	 * @param <OUT>
	 * @return
	 */
	public static <IN, OUT> DatagramServer<IN, OUT> udpServer(final String bindAddress, final int port) {
		return udpServer(new Function<Spec.DatagramServer<IN, OUT>, Spec.DatagramServer<IN, OUT>>() {
			@Override
			public Spec.DatagramServer<IN, OUT> apply(Spec.DatagramServer<IN, OUT> serverSpec) {
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
	public static <IN, OUT> DatagramServer<IN, OUT> udpServer(
			Function<? super Spec.DatagramServer<IN, OUT>, ? extends Spec.DatagramServer<IN, OUT>> configuringFunction
	) {
		return udpServer(DEFAULT_UDP_SERVER_TYPE, configuringFunction);
	}

	/**
	 * @param serverFactory
	 * @param configuringFunction
	 * @param <IN>
	 * @param <OUT>
	 * @return
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
			DEFAULT_TCP_SERVER_TYPE = reactor.io.net.netty.tcp.NettyTcpServer.class;
			DEFAULT_TCP_CLIENT_TYPE = reactor.io.net.netty.tcp.NettyTcpClient.class;
			DEFAULT_UDP_SERVER_TYPE = reactor.io.net.netty.udp.NettyDatagramServer.class;
			DEFAULT_HTTP_SERVER_TYPE = reactor.io.net.netty.http.NettyHttpServer.class;
			DEFAULT_HTTP_CLIENT_TYPE = reactor.io.net.netty.http.NettyHttpClient.class;
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
				DEFAULT_TCP_SERVER_TYPE = reactor.io.net.zmq.tcp.ZeroMQTcpServer.class;
				DEFAULT_TCP_CLIENT_TYPE = reactor.io.net.zmq.tcp.ZeroMQTcpClient.class;
			} else {
				DEFAULT_TCP_SERVER_TYPE = null;
				DEFAULT_TCP_CLIENT_TYPE = null;
			}
		}

	}
}
