/*
 * Copyright (c) 2011-2014 Pivotal Software, Inc.
 *
 *  Licensed under the Apache License, Version 2.0 (the "License");
 *  you may not use this file except in compliance with the License.
 *  You may obtain a copy of the License at
 *
 *         http://www.apache.org/licenses/LICENSE-2.0
 *
 *  Unless required by applicable law or agreed to in writing, software
 *  distributed under the License is distributed on an "AS IS" BASIS,
 *  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  See the License for the specific language governing permissions and
 *  limitations under the License.
 */

package reactor.io.netty.impl.zmq.tcp;

import org.reactivestreams.Publisher;
import org.zeromq.ZContext;
import org.zeromq.ZMQ;
import reactor.Environment;
import reactor.bus.registry.Registration;
import reactor.bus.registry.Registries;
import reactor.bus.registry.Registry;
import reactor.bus.selector.Selectors;
import reactor.core.Dispatcher;
import reactor.core.support.Assert;
import reactor.fn.Consumer;
import reactor.fn.Function;
import reactor.io.buffer.Buffer;
import reactor.io.codec.Codec;
import reactor.io.codec.StandardCodecs;
import reactor.io.netty.*;
import reactor.io.netty.impl.zmq.ZeroMQClientSocketOptions;
import reactor.io.netty.impl.zmq.ZeroMQServerSocketOptions;
import reactor.io.netty.tcp.TcpClient;
import reactor.io.netty.tcp.TcpServer;
import reactor.rx.Promise;
import reactor.rx.Promises;
import reactor.rx.Streams;

import java.lang.reflect.Field;
import java.util.ArrayList;
import java.util.List;

/**
 * @author Jon Brisbin
 * @author Stephane Maldini
 */
public class ZeroMQ<T> {

	private final static Registry<Integer, String> SOCKET_TYPES = Registries.create();

	static {
		for (Field f : ZMQ.class.getDeclaredFields()) {
			if (int.class.isAssignableFrom(f.getType())) {
				f.setAccessible(true);
				try {
					int val = f.getInt(null);
					SOCKET_TYPES.register(Selectors.$(val), f.getName());
				} catch (IllegalAccessException e) {
				}
			}
		}
	}

	private final Environment env;
	private final Dispatcher  dispatcher;
	private final ZContext    zmqCtx;

	private final List<ReactorPeer<T, T, ChannelStream<T, T>>> peers = new ArrayList<>();

	@SuppressWarnings("unchecked")
	private volatile Codec<Buffer, T, T> codec    = (Codec<Buffer, T, T>) StandardCodecs.PASS_THROUGH_CODEC;
	private volatile boolean             shutdown = false;

	public ZeroMQ(Environment env) {
		this(env, env.getDefaultDispatcher());
	}

	public ZeroMQ(Environment env, String dispatcher) {
		this(env, env.getDispatcher(dispatcher));
	}

	public ZeroMQ(Environment env, Dispatcher dispatcher) {
		this.env = env;
		this.dispatcher = dispatcher;
		this.zmqCtx = new ZContext();
		this.zmqCtx.setLinger(100);
	}

	public static String findSocketTypeName(final int socketType) {
		List<Registration<Integer, ? extends String>> registrations = SOCKET_TYPES.select(socketType);
		if(registrations.isEmpty()){
			return "";
		}else{
			return registrations.get(0).getObject();
		}

	}

	public ZeroMQ<T> codec(Codec<Buffer, T, T> codec) {
		this.codec = codec;
		return this;
	}

	public Promise<ChannelStream<T, T>> dealer(String addrs) {
		return createClient(addrs, ZMQ.DEALER);
	}

	public Promise<ChannelStream<T, T>> push(String addrs) {
		return createClient(addrs, ZMQ.PUSH);
	}

	public Promise<ChannelStream<T, T>> pull(String addrs) {
		return createServer(addrs, ZMQ.PULL);
	}

	public Promise<ChannelStream<T, T>> request(String addrs) {
		return createClient(addrs, ZMQ.REQ);
	}

	public Promise<ChannelStream<T, T>> reply(String addrs) {
		return createServer(addrs, ZMQ.REP);
	}

	public Promise<ChannelStream<T, T>> router(String addrs) {
		return createServer(addrs, ZMQ.ROUTER);
	}

	public Promise<ChannelStream<T, T>> createClient(final String addrs, final int socketType) {
		Assert.isTrue(!shutdown, "This ZeroMQ instance has been shut down");

		TcpClient<T, T> client = NetStreams.tcpClient(ZeroMQTcpClient.class,
				new NetStreams.TcpClientFactory<T, T>() {

					@Override
					public Spec.TcpClientSpec<T, T> apply(Spec.TcpClientSpec<T, T> spec) {
						return spec.env(env).dispatcher(dispatcher).codec(codec)
								.options(new ZeroMQClientSocketOptions()
										.context(zmqCtx)
										.connectAddresses(addrs)
										.socketType(socketType));
					}
				});

		final Promise<ChannelStream<T, T>> promise = Promises.ready(env, dispatcher);
		client.start(new ReactorChannelHandler<T, T, ChannelStream<T, T>>() {
			@Override
			public Publisher<Void> apply(ChannelStream<T, T> ttChannelStream) {
				promise.onNext(ttChannelStream);
				return Streams.never();
			}
		});

		synchronized (peers) {
			peers.add(client);
		}

		return promise;
	}

	public Promise<ChannelStream<T, T>> createServer(final String addrs, final int socketType) {
		Assert.isTrue(!shutdown, "This ZeroMQ instance has been shut down");

		final TcpServer<T, T> server = NetStreams.tcpServer(ZeroMQTcpServer.class,
				new NetStreams.TcpServerFactory<T, T>() {

					@Override
					public Spec.TcpServerSpec<T, T> apply(Spec.TcpServerSpec<T, T> spec) {
						return spec
								.env(env).dispatcher(dispatcher).codec(codec)
								.options(new ZeroMQServerSocketOptions()
										.context(zmqCtx)
										.listenAddresses(addrs)
										.socketType(socketType));
					}
				});

		final Promise<ChannelStream<T, T>> promise = Promises.ready(env, dispatcher);
		server.start(new ReactorChannelHandler<T, T, ChannelStream<T, T>>() {
			@Override
			public Publisher<Void> apply(ChannelStream<T, T> ttChannelStream) {
				promise.onNext(ttChannelStream);
				return Streams.never();
			}
		});

		synchronized (peers) {
			peers.add(server);
		}

		return promise;
	}

	public void shutdown() {
		if (shutdown) {
			return;
		}
		shutdown = true;

		List<ReactorPeer<T, T, ChannelStream<T, T>>> _peers;
		synchronized (peers) {
			_peers = new ArrayList<>(peers);
		}

		Streams.from(_peers).flatMap(new Function<ReactorPeer, Publisher<Void>>() {
			@Override
			@SuppressWarnings("unchecked")
			public Publisher<Void> apply(final ReactorPeer ttChannelStreamReactorPeer) {
				return ttChannelStreamReactorPeer.shutdown().onSuccess(new Consumer() {
					@Override
					public void accept(Object o) {
						peers.remove(ttChannelStreamReactorPeer);
					}
				});
			}
		}).consume();

//		if (log.isDebugEnabled()) {
//			log.debug("Destroying {} on {}", zmqCtx, this);
//		}
//		zmqCtx.destroy();
	}

}
