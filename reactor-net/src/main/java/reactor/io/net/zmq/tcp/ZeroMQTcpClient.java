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

package reactor.io.net.zmq.tcp;

import com.gs.collections.api.map.MutableMap;
import com.gs.collections.impl.block.procedure.checked.CheckedProcedure2;
import com.gs.collections.impl.map.mutable.SynchronizedMutableMap;
import com.gs.collections.impl.map.mutable.UnifiedMap;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.zeromq.ZContext;
import org.zeromq.ZFrame;
import org.zeromq.ZMQ;
import org.zeromq.ZMsg;
import reactor.Environment;
import reactor.core.Dispatcher;
import reactor.core.support.NamedDaemonThreadFactory;
import reactor.core.support.UUIDUtils;
import reactor.fn.Consumer;
import reactor.io.buffer.Buffer;
import reactor.io.codec.Codec;
import reactor.io.net.NetChannelStream;
import reactor.io.net.Reconnect;
import reactor.io.net.config.ClientSocketOptions;
import reactor.io.net.config.SslOptions;
import reactor.io.net.tcp.TcpClient;
import reactor.io.net.zmq.ZeroMQClientSocketOptions;
import reactor.io.net.zmq.ZeroMQNetChannel;
import reactor.io.net.zmq.ZeroMQWorker;
import reactor.rx.Promise;
import reactor.rx.Promises;
import reactor.rx.Stream;
import reactor.rx.Streams;
import reactor.rx.stream.Broadcaster;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import java.net.InetSocketAddress;
import java.util.UUID;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;

import static reactor.io.net.zmq.tcp.ZeroMQ.findSocketTypeName;

/**
 * @author Jon Brisbin
 * @author Stephane Maldini
 */
public class ZeroMQTcpClient<IN, OUT> extends TcpClient<IN, OUT> {

	private final Logger                              log     = LoggerFactory.getLogger(getClass());
	private final MutableMap<ZeroMQWorker, Future<?>> workers =
			SynchronizedMutableMap.of(UnifiedMap.<ZeroMQWorker, Future<?>>newMap());

	private final int                       ioThreadCount;
	private final ZeroMQClientSocketOptions zmqOpts;
	private final ExecutorService           threadPool;

	public ZeroMQTcpClient(@Nonnull Environment env,
	                       @Nonnull Dispatcher eventsDispatcher,
	                       @Nonnull InetSocketAddress connectAddress,
	                       @Nullable ClientSocketOptions options,
	                       @Nullable SslOptions sslOptions,
	                       @Nullable Codec<Buffer, IN, OUT> codec) {
		super(env, eventsDispatcher, connectAddress, options, sslOptions, codec);

		this.ioThreadCount = env.getProperty("reactor.zmq.ioThreadCount", Integer.class, 1);

		if (options instanceof ZeroMQClientSocketOptions) {
			this.zmqOpts = (ZeroMQClientSocketOptions) options;
		} else {
			this.zmqOpts = null;
		}

		this.threadPool = Executors.newCachedThreadPool(new NamedDaemonThreadFactory("zmq-client"));
	}

	@Override
	public Promise<NetChannelStream<IN, OUT>> open() {
		Promise<NetChannelStream<IN, OUT>> d =
				Promises.ready(getEnvironment(), getDispatcher());

		doOpen(d);

		return d;
	}

	@Override
	public Stream<NetChannelStream<IN, OUT>> open(Reconnect reconnect) {
		throw new IllegalStateException("Reconnects are handled transparently by the ZeroMQ network library");
	}

	@Override
	public Promise<Void> close() {
		if (workers.isEmpty()) {
			throw new IllegalStateException("This ZeroMQ server has not been started");
		}

		Promise<Void> promise = Promises.ready(getEnvironment(), getDispatcher());

		workers.forEachKeyValue(new CheckedProcedure2<ZeroMQWorker, Future<?>>() {
			@Override
			public void safeValue(ZeroMQWorker w, Future<?> f) throws Exception {
				w.shutdown();
				if (!f.isDone()) {
					f.cancel(true);
				}
			}
		});
		threadPool.shutdownNow();

		notifyShutdown();
		promise.onComplete();

		return promise;
	}

	@Override
	protected ZeroMQNetChannel<IN, OUT> createChannel(Object ioChannel) {
		final ZeroMQNetChannel<IN, OUT> ch = new ZeroMQNetChannel<IN, OUT>(
				getEnvironment(),
				getDispatcher(),
				getDispatcher(),
				getCodec()
		);
		return ch;
	}

	private void doOpen(final Promise<NetChannelStream<IN, OUT>> promise) {
		final UUID id = UUIDUtils.random();

		final int socketType = (null != zmqOpts ? zmqOpts.socketType() : ZMQ.DEALER);
		final ZContext zmq = (null != zmqOpts ? zmqOpts.context() : null);

		final Broadcaster<ZMsg> broadcaster = Streams.broadcast(getEnvironment(), getDispatcher());

		ZeroMQWorker worker = new ZeroMQWorker(id, socketType, ioThreadCount, zmq, broadcaster) {
			@Override
			protected void configure(ZMQ.Socket socket) {
				socket.setReceiveBufferSize(getOptions().rcvbuf());
				socket.setSendBufferSize(getOptions().sndbuf());
				if (getOptions().keepAlive()) {
					socket.setTCPKeepAlive(1);
				}
				if (null != zmqOpts && null != zmqOpts.socketConfigurer()) {
					zmqOpts.socketConfigurer().accept(socket);
				}
			}

			@Override
			protected void start(final ZMQ.Socket socket) {
				try {
					String addr = createConnectAddress();
					if (log.isInfoEnabled()) {
						String type = findSocketTypeName(socket.getType());
						log.info("CONNECT: connecting ZeroMQ {} socket to {}", type, addr);
					}

					socket.connect(addr);
					notifyStart();

					final ZeroMQNetChannel<IN, OUT> netChannel = createChannel(null)
							.setConnectionId(id.toString())
							.setSocket(socket);

					notifyNewChannel(netChannel);
					promise.onNext(netChannel);

					broadcaster.consume(new Consumer<ZMsg>() {
						@Override
						public void accept(ZMsg msg) {
							ZFrame content;
							while (null != (content = msg.pop())) {
								netChannel.read(Buffer.wrap(content.getData()));
							}
							msg.destroy();
						}
					}, null, new Consumer<Void>() {
						@Override
						public void accept(Void aVoid) {
							netChannel.close();
						}
					});



				} catch (Exception e) {
					promise.onError(e);
				}
			}
		};
		workers.put(worker, threadPool.submit(worker));
	}

	private String createConnectAddress() {
		String addrs;
		if (null != zmqOpts && null != zmqOpts.connectAddresses()) {
			addrs = zmqOpts.connectAddresses();
		} else {
			addrs = "tcp://" + getConnectAddress().getHostString() + ":" + getConnectAddress().getPort();
		}
		return addrs;
	}

}
