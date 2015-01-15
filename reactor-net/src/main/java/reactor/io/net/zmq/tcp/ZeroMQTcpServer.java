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

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.zeromq.ZContext;
import org.zeromq.ZMQ;
import reactor.Environment;
import reactor.core.Dispatcher;
import reactor.core.support.Assert;
import reactor.core.support.NamedDaemonThreadFactory;
import reactor.core.support.UUIDUtils;
import reactor.fn.Consumer;
import reactor.io.buffer.Buffer;
import reactor.io.codec.Codec;
import reactor.io.net.NetChannel;
import reactor.io.net.config.ServerSocketOptions;
import reactor.io.net.config.SslOptions;
import reactor.io.net.tcp.TcpServer;
import reactor.io.net.zmq.ZeroMQNetChannel;
import reactor.io.net.zmq.ZeroMQServerSocketOptions;
import reactor.io.net.zmq.ZeroMQWorker;
import reactor.rx.Promise;
import reactor.rx.Promises;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import java.net.InetSocketAddress;
import java.util.Collection;
import java.util.UUID;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;

import static reactor.io.net.zmq.tcp.ZeroMQ.findSocketTypeName;

/**
 * @author Jon Brisbin
 * @author Stephane Maldini
 */
public class ZeroMQTcpServer<IN, OUT> extends TcpServer<IN, OUT> {

	private final Logger log = LoggerFactory.getLogger(getClass());

	private final int                       ioThreadCount;
	private final ZeroMQServerSocketOptions zmqOpts;
	private final ExecutorService           threadPool;

	private volatile ZeroMQWorker<IN, OUT> worker;
	private volatile Future<?>             workerFuture;

	public ZeroMQTcpServer(@Nonnull Environment env,
	                       @Nonnull Dispatcher eventsDispatcher,
	                       @Nullable InetSocketAddress listenAddress,
	                       ServerSocketOptions options,
	                       SslOptions sslOptions,
	                       @Nullable Codec<Buffer, IN, OUT> codec,
	                       @Nonnull Collection<Consumer<NetChannel<IN, OUT>>> consumers) {
		super(env, eventsDispatcher, listenAddress, options, sslOptions, codec, consumers);

		this.ioThreadCount = env.getProperty("reactor.zmq.ioThreadCount", Integer.class, 1);

		if (options instanceof ZeroMQServerSocketOptions) {
			this.zmqOpts = (ZeroMQServerSocketOptions) options;
		} else {
			this.zmqOpts = null;
		}

		this.threadPool = Executors.newCachedThreadPool(new NamedDaemonThreadFactory("zmq-server"));
	}

	@Override
	public TcpServer<IN, OUT> start(@Nullable final Runnable started) {
		Assert.isNull(worker, "This ZeroMQ server has already been started");

		UUID id = UUIDUtils.random();
		int socketType = (null != zmqOpts ? zmqOpts.socketType() : ZMQ.ROUTER);
		ZContext zmq = (null != zmqOpts ? zmqOpts.context() : null);
		this.worker = new ZeroMQWorker<IN, OUT>(id, socketType, ioThreadCount, zmq) {
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
			protected void start(ZMQ.Socket socket) {
				String addr;
				if (null != zmqOpts && null != zmqOpts.listenAddresses()) {
					addr = zmqOpts.listenAddresses();
				} else {
					addr = "tcp://" + getListenAddress().getHostString() + ":" + getListenAddress().getPort();
				}
				if (log.isInfoEnabled()) {
					String type = findSocketTypeName(socket.getType());
					log.info("BIND: starting ZeroMQ {} socket on {}", type, addr);
				}

				socket.bind(addr);
				notifyStart(started);
			}

			@Override
			protected ZeroMQNetChannel<IN, OUT> select(Object id) {
				return (ZeroMQNetChannel<IN, OUT>) ZeroMQTcpServer.this.select(id);
			}
		};
		this.workerFuture = threadPool.submit(this.worker);

		return this;
	}

	@Override
	protected <C> NetChannel<IN, OUT> createChannel(C ioChannel) {
		return new ZeroMQNetChannel<IN, OUT>(
				getEnvironment(),
				getDispatcher(),
				getDispatcher(),
				getCodec()
		);
	}

	@Override
	public Promise<Boolean> shutdown() {
		if (null == worker) {
			return Promises.<Boolean>error(new IllegalStateException("This ZeroMQ server has not been started"));
		}

		Promise<Boolean> d = Promises.ready(getEnvironment(), getDispatcher());

		super.close(null);

		worker.shutdown();
		if (!workerFuture.isDone()) {
			workerFuture.cancel(true);
		}
		threadPool.shutdownNow();

		notifyShutdown();
		d.accept(true);


		return d;
	}

}
