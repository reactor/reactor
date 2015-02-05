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

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.zeromq.ZContext;
import org.zeromq.ZFrame;
import org.zeromq.ZMQ;
import org.zeromq.ZMsg;
import reactor.Environment;
import reactor.core.Dispatcher;
import reactor.core.support.Assert;
import reactor.core.support.NamedDaemonThreadFactory;
import reactor.core.support.UUIDUtils;
import reactor.fn.Consumer;
import reactor.fn.Function;
import reactor.io.buffer.Buffer;
import reactor.io.codec.Codec;
import reactor.io.net.config.ServerSocketOptions;
import reactor.io.net.config.SslOptions;
import reactor.io.net.impl.zmq.ZeroMQChannelStream;
import reactor.io.net.impl.zmq.ZeroMQServerSocketOptions;
import reactor.io.net.impl.zmq.ZeroMQWorker;
import reactor.io.net.tcp.TcpServer;
import reactor.rx.Promise;
import reactor.rx.Promises;
import reactor.rx.Stream;
import reactor.rx.broadcast.Broadcaster;
import reactor.rx.broadcast.SerializedBroadcaster;
import reactor.rx.stream.GroupedStream;

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
public class ZeroMQTcpServer<IN, OUT> extends TcpServer<IN, OUT> {

	private final Logger log = LoggerFactory.getLogger(getClass());

	private final int                       ioThreadCount;
	private final ZeroMQServerSocketOptions zmqOpts;
	private final ExecutorService           threadPool;

	private volatile ZeroMQWorker worker;
	private volatile Future<?>    workerFuture;

	public ZeroMQTcpServer(@Nonnull Environment env,
	                       @Nonnull Dispatcher eventsDispatcher,
	                       @Nullable InetSocketAddress listenAddress,
	                       ServerSocketOptions options,
	                       SslOptions sslOptions,
	                       @Nullable Codec<Buffer, IN, OUT> codec) {
		super(env, eventsDispatcher, listenAddress, options, sslOptions, codec);

		this.ioThreadCount = getEnvironment().getProperty("reactor.zmq.ioThreadCount", Integer.class, 1);

		if (options instanceof ZeroMQServerSocketOptions) {
			this.zmqOpts = (ZeroMQServerSocketOptions) options;
		} else {
			this.zmqOpts = null;
		}

		this.threadPool = Executors.newCachedThreadPool(new NamedDaemonThreadFactory("zmq-server"));
	}

	@Override
	public Promise<Boolean> start() {
		Assert.isNull(worker, "This ZeroMQ server has already been started");

		final Promise<Boolean> promise = Promises.ready(getEnvironment(), getDispatcher());

		final UUID id = UUIDUtils.random();
		final int socketType = (null != zmqOpts ? zmqOpts.socketType() : ZMQ.ROUTER);
		ZContext zmq = (null != zmqOpts ? zmqOpts.context() : null);

		Broadcaster<ZMsg> broadcaster = SerializedBroadcaster.create(getEnvironment());

		final Stream<GroupedStream<String, ZMsg>> grouped = broadcaster.groupBy(new Function<ZMsg, String>() {
			@Override
			public String apply(ZMsg msg) {
				String connId;
				switch (socketType) {
					case ZMQ.ROUTER:
						connId = msg.popString();
						break;
					default:
						connId = id.toString();
				}

				return connId;
			}
		});

		this.worker = new ZeroMQWorker(id, socketType, ioThreadCount, zmq, broadcaster) {
			@Override
			protected void configure(ZMQ.Socket socket) {
				socket.setReceiveBufferSize(getOptions().rcvbuf());
				socket.setSendBufferSize(getOptions().sndbuf());
				socket.setBacklog(getOptions().backlog());
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
				String addr;
				try {
					if (null != zmqOpts && null != zmqOpts.listenAddresses()) {
						addr = zmqOpts.listenAddresses();
					} else {
						addr = "tcp://" + getListenAddress().getHostString() + ":" + getListenAddress().getPort();
					}
					if (log.isInfoEnabled()) {
						String type = ZeroMQ.findSocketTypeName(socket.getType());
						log.info("BIND: starting ZeroMQ {} socket on {}", type, addr);
					}
					socket.bind(addr);
					grouped.consume(new Consumer<GroupedStream<String, ZMsg>>() {
						@Override
						public void accept(GroupedStream<String, ZMsg> stringZMsgGroupedStream) {

							final ZeroMQChannelStream<IN, OUT> netChannel =
									bindChannel(null, null != zmqOpts ? zmqOpts.prefetch() : -1l)
											.setConnectionId(stringZMsgGroupedStream.key())
											.setSocket(socket);

							netChannel.registerOnPeer();

							stringZMsgGroupedStream.consume(new Consumer<ZMsg>() {
								@Override
								public void accept(ZMsg msg) {
									ZFrame content;
									while (null != (content = msg.pop())) {
										if (netChannel.getDecoder() != null) {
											netChannel.getDecoder().apply(Buffer.wrap(content.getData()));
										} else {
											netChannel.in().onNext((IN) Buffer.wrap(content.getData()));
										}
									}
									msg.destroy();
								}
							}, createErrorConsumer(netChannel), new Consumer<Void>() {
								@Override
								public void accept(Void aVoid) {
									try {
										netChannel.close();
									} catch (Throwable t) {
										notifyError(t);
									}
								}
							});


						}
					});
					promise.onNext(true);
				} catch (Exception e) {
					promise.onError(e);
				}
			}
		};
		this.workerFuture = threadPool.submit(this.worker);

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

	@Override
	public Promise<Boolean> shutdown() {
		if (null == worker) {
			return Promises.<Boolean>error(new IllegalStateException("This ZeroMQ server has not been started"));
		}

		Promise<Boolean> d = Promises.ready(getEnvironment(), getDispatcher());

		worker.shutdown();
		if (!workerFuture.isDone()) {
			workerFuture.cancel(true);
		}
		threadPool.shutdownNow();

		notifyShutdown();
		d.onNext(true);


		return d;
	}

}
