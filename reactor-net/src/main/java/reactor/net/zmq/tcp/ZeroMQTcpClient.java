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

package reactor.net.zmq.tcp;

import com.gs.collections.api.map.MutableMap;
import com.gs.collections.impl.block.procedure.checked.CheckedProcedure2;
import com.gs.collections.impl.map.mutable.SynchronizedMutableMap;
import com.gs.collections.impl.map.mutable.UnifiedMap;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.zeromq.ZContext;
import org.zeromq.ZMQ;
import reactor.Environment;
import reactor.bus.EventBus;
import reactor.core.support.NamedDaemonThreadFactory;
import reactor.core.support.UUIDUtils;
import reactor.fn.Consumer;
import reactor.io.buffer.Buffer;
import reactor.io.codec.Codec;
import reactor.net.NetChannel;
import reactor.net.Reconnect;
import reactor.net.config.ClientSocketOptions;
import reactor.net.config.SslOptions;
import reactor.net.tcp.TcpClient;
import reactor.net.zmq.ZeroMQClientSocketOptions;
import reactor.net.zmq.ZeroMQNetChannel;
import reactor.net.zmq.ZeroMQWorker;
import reactor.rx.Promise;
import reactor.rx.Promises;
import reactor.rx.Stream;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import java.net.InetSocketAddress;
import java.util.Collection;
import java.util.UUID;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;

import static reactor.net.zmq.tcp.ZeroMQ.findSocketTypeName;

/**
 * @author Jon Brisbin
 * @author Stephane Maldini
 */
public class ZeroMQTcpClient<IN, OUT> extends TcpClient<IN, OUT> {

	private final Logger                                       log     = LoggerFactory.getLogger(getClass());
	private final MutableMap<ZeroMQWorker<IN, OUT>, Future<?>> workers =
			SynchronizedMutableMap.of(UnifiedMap.<ZeroMQWorker<IN, OUT>, Future<?>>newMap());

	private final int                       ioThreadCount;
	private final ZeroMQClientSocketOptions zmqOpts;
	private final ExecutorService           threadPool;

	public ZeroMQTcpClient(@Nonnull Environment env,
	                       @Nonnull EventBus reactor,
	                       @Nonnull InetSocketAddress connectAddress,
	                       @Nullable ClientSocketOptions options,
	                       @Nullable SslOptions sslOptions,
	                       @Nullable Codec<Buffer, IN, OUT> codec,
	                       @Nonnull Collection<Consumer<NetChannel<IN, OUT>>> consumers) {
		super(env, reactor, connectAddress, options, sslOptions, codec, consumers);

		this.ioThreadCount = env.getProperty("reactor.zmq.ioThreadCount", Integer.class, 1);

		if (options instanceof ZeroMQClientSocketOptions) {
			this.zmqOpts = (ZeroMQClientSocketOptions) options;
		} else {
			this.zmqOpts = null;
		}

		this.threadPool = Executors.newCachedThreadPool(new NamedDaemonThreadFactory("zmq-client"));
	}

	@Override
	public Promise<NetChannel<IN, OUT>> open() {
		Promise<NetChannel<IN, OUT>> d =
				Promises.ready(getEnvironment(), getReactor().getDispatcher());

		doOpen(d);

		return d;
	}

	@Override
	public Stream<NetChannel<IN, OUT>> open(Reconnect reconnect) {
		throw new IllegalStateException("Reconnects are handled transparently by the ZeroMQ network library");
	}

	@Override
	public void close(@Nullable Consumer<Boolean> onClose) {
		if (workers.isEmpty()) {
			throw new IllegalStateException("This ZeroMQ server has not been started");
		}

		super.close(null);

		workers.forEachKeyValue(new CheckedProcedure2<ZeroMQWorker<IN, OUT>, Future<?>>() {
			@Override
			public void safeValue(ZeroMQWorker<IN, OUT> w, Future<?> f) throws Exception {
				w.shutdown();
				if (!f.isDone()) {
					f.cancel(true);
				}
			}
		});
		threadPool.shutdownNow();

		getReactor().schedule(onClose, true);
		notifyShutdown();
	}

	@Override
	protected <C> NetChannel<IN, OUT> createChannel(C ioChannel) {
		final ZeroMQNetChannel<IN, OUT> ch = new ZeroMQNetChannel<IN, OUT>(
				getEnvironment(),
				getReactor(),
				getReactor().getDispatcher(),
				getCodec()
		);
		ch.on().close(new Runnable() {
			@Override
			public void run() {
				notifyClose(ch);
			}
		});
		return ch;
	}

	private void doOpen(final Consumer<NetChannel<IN, OUT>> consumer) {
		final UUID id = UUIDUtils.random();

		int socketType = (null != zmqOpts ? zmqOpts.socketType() : ZMQ.DEALER);
		ZContext zmq = (null != zmqOpts ? zmqOpts.context() : null);

		ZeroMQWorker<IN, OUT> worker = new ZeroMQWorker<IN, OUT>(id, socketType, ioThreadCount, zmq) {
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
				String addr = createConnectAddress();
				if (log.isInfoEnabled()) {
					String type = findSocketTypeName(socket.getType());
					log.info("CONNECT: connecting ZeroMQ {} socket to {}", type, addr);
				}

				socket.connect(addr);
				notifyStart(new Runnable() {
					@Override
					public void run() {
						ZeroMQNetChannel<IN, OUT> ch = select(id.toString())
								.setConnectionId(id.toString())
								.setSocket(socket);
						consumer.accept(ch);
					}
				});
			}

			@Override
			protected ZeroMQNetChannel<IN, OUT> select(Object id) {
				return (ZeroMQNetChannel<IN, OUT>) ZeroMQTcpClient.this.select(id);
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
