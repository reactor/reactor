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

package reactor.io.net.impl.zmq.tcp;

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
import reactor.io.net.ChannelStream;
import reactor.io.net.Reconnect;
import reactor.io.net.config.ClientSocketOptions;
import reactor.io.net.config.SslOptions;
import reactor.io.net.impl.zmq.ZeroMQChannelStream;
import reactor.io.net.impl.zmq.ZeroMQClientSocketOptions;
import reactor.io.net.impl.zmq.ZeroMQWorker;
import reactor.io.net.tcp.TcpClient;
import reactor.rx.Promise;
import reactor.rx.Promises;
import reactor.rx.Stream;
import reactor.rx.broadcast.Broadcaster;
import reactor.rx.broadcast.SerializedBroadcaster;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import java.net.InetSocketAddress;
import java.util.UUID;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;

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
	public Promise<ChannelStream<IN, OUT>> open() {
		Promise<ChannelStream<IN, OUT>> d = next();
		doOpen();
		return d;
	}

	@Override
	public Stream<ChannelStream<IN, OUT>> open(Reconnect reconnect) {
		throw new IllegalStateException("Reconnects are handled transparently by the ZeroMQ network library");
	}

	@Override
	public Promise<Boolean> close() {
		if (workers.isEmpty()) {
			throw new IllegalStateException("This ZeroMQ server has not been started");
		}

		Promise<Boolean> promise = Promises.ready(getEnvironment(), getDispatcher());

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
		promise.onNext(true);

		return promise;
	}

	@Override
	protected ZeroMQChannelStream<IN, OUT> bindChannel(Object ioChannel, long prefetch) {

		return new ZeroMQChannelStream<IN, OUT>(
				getEnvironment(),
				prefetch == -1l ? getPrefetchSize() : prefetch,
				this,
				getDispatcher(),
				getDispatcher(),
				getDefaultCodec()
		);
	}

	private void doOpen() {
		final UUID id = UUIDUtils.random();

		final int socketType = (null != zmqOpts ? zmqOpts.socketType() : ZMQ.DEALER);
		final ZContext zmq = (null != zmqOpts ? zmqOpts.context() : null);

		final Broadcaster<ZMsg> broadcaster = SerializedBroadcaster.create(getEnvironment());

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
			@SuppressWarnings("unchecked")
			protected void start(final ZMQ.Socket socket) {
				try {
					String addr = createConnectAddress();
					if (log.isInfoEnabled()) {
						String type = ZeroMQ.findSocketTypeName(socket.getType());
						log.info("CONNECT: connecting ZeroMQ {} socket to {}", type, addr);
					}

					socket.connect(addr);
					notifyStart();

					final ZeroMQChannelStream<IN, OUT> netChannel =
							bindChannel(null, null != zmqOpts ? zmqOpts.prefetch() : -1l)
							.setConnectionId(id.toString())
							.setSocket(socket);

					netChannel.registerOnPeer();

					broadcaster.consume(new Consumer<ZMsg>() {
						@Override
						public void accept(ZMsg msg) {
							ZFrame content;
							while (null != (content = msg.pop())) {
								if(netChannel.getDecoder() != null){
									netChannel.getDecoder().apply(Buffer.wrap(content.getData()));
								}else{
									netChannel.in().onNext((IN)Buffer.wrap(content.getData()));
								}
							}
							msg.destroy();
						}
					}, createErrorConsumer(netChannel), new Consumer<Void>() {
						@Override
						public void accept(Void aVoid) {
							netChannel.close();
						}
					});



				} catch (Exception e) {
					notifyError(e);
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
