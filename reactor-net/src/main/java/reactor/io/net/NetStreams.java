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

import reactor.fn.Function;
import reactor.io.net.http.HttpClient;
import reactor.io.net.http.HttpServer;
import reactor.io.net.http.spec.HttpClientSpec;
import reactor.io.net.http.spec.HttpServerSpec;
import reactor.io.net.tcp.TcpClient;
import reactor.io.net.tcp.TcpServer;
import reactor.io.net.tcp.spec.TcpClientSpec;
import reactor.io.net.tcp.spec.TcpServerSpec;
import reactor.io.net.udp.DatagramServer;
import reactor.io.net.udp.spec.DatagramServerSpec;
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
	 *
	 * @param <IN>
	 * @param <OUT>
	 * @return
	 */
	public static <IN, OUT> TcpServer<IN, OUT> tcpServer() {
		return tcpServer(DEFAULT_BIND_ADDRESS);
	}

	/**
	 *
	 * @param port
	 * @param <IN>
	 * @param <OUT>
	 * @return
	 */
	public static <IN, OUT> TcpServer<IN, OUT> tcpServer(int port) {
		return tcpServer(DEFAULT_BIND_ADDRESS, port);
	}

	/**
	 *
	 * @param bindAddress
	 * @param <IN>
	 * @param <OUT>
	 * @return
	 */
	public static <IN, OUT> TcpServer<IN, OUT> tcpServer(String bindAddress) {
		return tcpServer(bindAddress, DEFAULT_PORT);
	}

	/**
	 *
	 * @param bindAddress
	 * @param port
	 * @param <IN>
	 * @param <OUT>
	 * @return
	 */
	public static <IN, OUT> TcpServer<IN, OUT> tcpServer(final String bindAddress, final int port) {
		return tcpServer(new Function<TcpServerSpec<IN, OUT>, TcpServerSpec<IN, OUT>>() {
			@Override
			public TcpServerSpec<IN, OUT> apply(TcpServerSpec<IN, OUT> serverSpec) {
				return serverSpec.listen(bindAddress, port);
			}
		});
	}

	/**
	 *
	 * @param configuringFunction
	 * @param <IN>
	 * @param <OUT>
	 * @return
	 */
	public static <IN, OUT> TcpServer<IN, OUT> tcpServer(
			Function<? super TcpServerSpec<IN, OUT>, ? extends TcpServerSpec<IN, OUT>> configuringFunction
	) {
		return tcpServer(DEFAULT_TCP_SERVER_TYPE, configuringFunction);
	}

	/**
	 *
	 * @param serverFactory
	 * @param configuringFunction
	 * @param <IN>
	 * @param <OUT>
	 * @return
	 */
	public static <IN, OUT> TcpServer<IN, OUT> tcpServer(
			Class<? extends TcpServer> serverFactory,
			Function<? super TcpServerSpec<IN, OUT>, ? extends TcpServerSpec<IN, OUT>> configuringFunction
	) {
		return configuringFunction.apply(new TcpServerSpec<IN, OUT>(serverFactory)).get();
	}


	/**
	 *
	 * @param <IN>
	 * @param <OUT>
	 * @return
	 */
	public static <IN, OUT> TcpClient<IN, OUT> tcpClient() {
		return tcpClient(DEFAULT_BIND_ADDRESS);
	}

	/**
	 *
	 * @param bindAddress
	 * @param <IN>
	 * @param <OUT>
	 * @return
	 */
	public static <IN, OUT> TcpClient<IN, OUT> tcpClient(String bindAddress) {
		return tcpClient(bindAddress, DEFAULT_PORT);
	}

	/**
	 *
	 * @param port
	 * @param <IN>
	 * @param <OUT>
	 * @return
	 */
	public static <IN, OUT> TcpClient<IN, OUT> tcpClient(int port) {
		return tcpClient(DEFAULT_BIND_ADDRESS, port);
	}

	/**
	 *
	 * @param bindAddress
	 * @param port
	 * @param <IN>
	 * @param <OUT>
	 * @return
	 */
	public static <IN, OUT> TcpClient<IN, OUT> tcpClient(final String bindAddress, final int port) {
		return tcpClient(new Function<TcpClientSpec<IN, OUT>, TcpClientSpec<IN, OUT>>() {
			@Override
			public TcpClientSpec<IN, OUT> apply(TcpClientSpec<IN, OUT> clientSpec) {
				return clientSpec.connect(bindAddress, port);
			}
		});
	}

	/**
	 *
	 * @param configuringFunction
	 * @param <IN>
	 * @param <OUT>
	 * @return
	 */
	public static <IN, OUT> TcpClient<IN, OUT> tcpClient(
			Function<? super TcpClientSpec<IN, OUT>, ? extends TcpClientSpec<IN, OUT>> configuringFunction
	) {
		return tcpClient(DEFAULT_TCP_CLIENT_TYPE, configuringFunction);
	}

	/**
	 *
	 * @param clientFactory
	 * @param configuringFunction
	 * @param <IN>
	 * @param <OUT>
	 * @return
	 */
	public static <IN, OUT> TcpClient<IN, OUT> tcpClient(
			Class<? extends TcpClient> clientFactory,
			Function<? super TcpClientSpec<IN, OUT>, ? extends TcpClientSpec<IN, OUT>> configuringFunction
	) {
		return configuringFunction.apply(new TcpClientSpec<IN, OUT>(clientFactory)).get();
	}

	// HTTP

	/**
	 *
	 * @param <IN>
	 * @param <OUT>
	 * @return
	 */
	public static <IN, OUT> HttpServer<IN, OUT> httpServer() {
		return httpServer(DEFAULT_BIND_ADDRESS);
	}

	/**
	 *
	 * @param bindAddress
	 * @param <IN>
	 * @param <OUT>
	 * @return
	 */
	public static <IN, OUT> HttpServer<IN, OUT> httpServer(String bindAddress) {
		return httpServer(bindAddress, DEFAULT_PORT);
	}

	/**
	 *
	 * @param port
	 * @param <IN>
	 * @param <OUT>
	 * @return
	 */
	public static <IN, OUT> HttpServer<IN, OUT> httpServer(int port) {
		return httpServer(DEFAULT_BIND_ADDRESS, port);
	}

	/**
	 *
	 * @param bindAddress
	 * @param port
	 * @param <IN>
	 * @param <OUT>
	 * @return
	 */
	public static <IN, OUT> HttpServer<IN, OUT> httpServer(final String bindAddress, final int port) {
		return httpServer(new Function<HttpServerSpec<IN, OUT>, HttpServerSpec<IN, OUT>>() {
			@Override
			public HttpServerSpec<IN, OUT> apply(HttpServerSpec<IN, OUT> serverSpec) {
				return serverSpec.listen(bindAddress, port);
			}
		});
	}

	/**
	 *
	 * @param configuringFunction
	 * @param <IN>
	 * @param <OUT>
	 * @return
	 */
	public static <IN, OUT> HttpServer<IN, OUT> httpServer(
			Function<? super HttpServerSpec<IN, OUT>, ? extends HttpServerSpec<IN, OUT>> configuringFunction
	) {
		return httpServer(DEFAULT_HTTP_SERVER_TYPE, configuringFunction);
	}

	/**
	 *
	 * @param serverFactory
	 * @param configuringFunction
	 * @param <IN>
	 * @param <OUT>
	 * @return
	 */
	public static <IN, OUT> HttpServer<IN, OUT> httpServer(
			Class<? extends HttpServer> serverFactory,
			Function<? super HttpServerSpec<IN, OUT>, ? extends HttpServerSpec<IN, OUT>> configuringFunction
	) {
		return configuringFunction.apply(new HttpServerSpec<IN, OUT>(serverFactory)).get();
	}


	/**
	 *
	 * @param <IN>
	 * @param <OUT>
	 * @return
	 */
	public static <IN, OUT> HttpClient<IN, OUT> httpClient() {
		return httpClient(DEFAULT_BIND_ADDRESS);
	}

	/**
	 *
	 * @param bindAddress
	 * @param <IN>
	 * @param <OUT>
	 * @return
	 */
	public static <IN, OUT> HttpClient<IN, OUT> httpClient(String bindAddress) {
		return httpClient(bindAddress, DEFAULT_PORT);
	}

	/**
	 *
	 * @param port
	 * @param <IN>
	 * @param <OUT>
	 * @return
	 */
	public static <IN, OUT> HttpClient<IN, OUT> httpClient(int port) {
		return httpClient(DEFAULT_BIND_ADDRESS, port);
	}

	/**
	 *
	 * @param bindAddress
	 * @param port
	 * @param <IN>
	 * @param <OUT>
	 * @return
	 */
	public static <IN, OUT> HttpClient<IN, OUT> httpClient(final String bindAddress, final int port) {
		return httpClient(new Function<HttpClientSpec<IN, OUT>, HttpClientSpec<IN, OUT>>() {
			@Override
			public HttpClientSpec<IN, OUT> apply(HttpClientSpec<IN, OUT> clientSpec) {
				return clientSpec.connect(bindAddress, port);
			}
		});
	}

	/**
	 *
	 * @param configuringFunction
	 * @param <IN>
	 * @param <OUT>
	 * @return
	 */
	public static <IN, OUT> HttpClient<IN, OUT> httpClient(
			Function<? super HttpClientSpec<IN, OUT>, ? extends HttpClientSpec<IN, OUT>> configuringFunction
	) {
		return httpClient(DEFAULT_HTTP_CLIENT_TYPE, configuringFunction);
	}

	/**
	 *
	 * @param clientFactory
	 * @param configuringFunction
	 * @param <IN>
	 * @param <OUT>
	 * @return
	 */
	public static <IN, OUT> HttpClient<IN, OUT> httpClient(
			Class<? extends HttpClient> clientFactory,
			Function<? super HttpClientSpec<IN, OUT>, ? extends HttpClientSpec<IN, OUT>> configuringFunction
	) {
		return configuringFunction.apply(new HttpClientSpec<IN, OUT>(clientFactory)).get();
	}

	// UDP

	/**
	 *
	 * @param <IN>
	 * @param <OUT>
	 * @return
	 */
	public static <IN, OUT> DatagramServer<IN, OUT> udpServer() {
		return udpServer(DEFAULT_BIND_ADDRESS);
	}

	/**
	 *
	 * @param bindAddress
	 * @param <IN>
	 * @param <OUT>
	 * @return
	 */
	public static <IN, OUT> DatagramServer<IN, OUT> udpServer(String bindAddress) {
		return udpServer(bindAddress, DEFAULT_PORT);
	}

	/**
	 *
	 * @param port
	 * @param <IN>
	 * @param <OUT>
	 * @return
	 */
	public static <IN, OUT> DatagramServer<IN, OUT> udpServer(int port) {
		return udpServer(DEFAULT_BIND_ADDRESS, port);
	}

	/**
	 *
	 * @param bindAddress
	 * @param port
	 * @param <IN>
	 * @param <OUT>
	 * @return
	 */
	public static <IN, OUT> DatagramServer<IN, OUT> udpServer(final String bindAddress, final int port) {
		return udpServer(new Function<DatagramServerSpec<IN, OUT>, DatagramServerSpec<IN, OUT>>() {
			@Override
			public DatagramServerSpec<IN, OUT> apply(DatagramServerSpec<IN, OUT> serverSpec) {
				return serverSpec.listen(bindAddress, port);
			}
		});
	}

	/**
	 *
	 * @param configuringFunction
	 * @param <IN>
	 * @param <OUT>
	 * @return
	 */
	public static <IN, OUT> DatagramServer<IN, OUT> udpServer(
			Function<? super DatagramServerSpec<IN, OUT>, ? extends DatagramServerSpec<IN, OUT>> configuringFunction
	) {
		return udpServer(DEFAULT_UDP_SERVER_TYPE, configuringFunction);
	}

	/**
	 *
	 * @param serverFactory
	 * @param configuringFunction
	 * @param <IN>
	 * @param <OUT>
	 * @return
	 */
	public static <IN, OUT> DatagramServer<IN, OUT> udpServer(
			Class<? extends DatagramServer> serverFactory,
			Function<? super DatagramServerSpec<IN, OUT>, ? extends DatagramServerSpec<IN, OUT>> configuringFunction
	) {
		return configuringFunction.apply(new DatagramServerSpec<IN, OUT>(serverFactory)).get();
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
