package reactor.net.zmq.tcp;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.zeromq.ZMQ;
import reactor.core.Environment;
import reactor.core.Reactor;
import reactor.core.composable.Deferred;
import reactor.core.composable.Promise;
import reactor.core.composable.spec.Promises;
import reactor.function.Consumer;
import reactor.io.Buffer;
import reactor.io.encoding.Codec;
import reactor.net.NetChannel;
import reactor.net.config.ServerSocketOptions;
import reactor.net.config.SslOptions;
import reactor.net.tcp.TcpServer;
import reactor.net.zmq.ZeroMQNetChannel;
import reactor.net.zmq.ZeroMQServerSocketOptions;
import reactor.net.zmq.ZeroMQWorker;
import reactor.support.NamedDaemonThreadFactory;
import reactor.util.Assert;
import reactor.util.UUIDUtils;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import java.net.InetSocketAddress;
import java.util.Collection;
import java.util.UUID;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;

/**
 * @author Jon Brisbin
 */
public class ZeroMQTcpServer<IN, OUT> extends TcpServer<IN, OUT> {

	private final Logger log = LoggerFactory.getLogger(getClass());

	private final int                       ioThreadCount;
	private final ZeroMQServerSocketOptions zmqOpts;
	private final ExecutorService           threadPool;

	private volatile ZeroMQWorker<IN, OUT> worker;
	private volatile Future<?>             workerFuture;

	public ZeroMQTcpServer(@Nonnull Environment env,
	                       @Nonnull Reactor reactor,
	                       @Nullable InetSocketAddress listenAddress,
	                       ServerSocketOptions options,
	                       SslOptions sslOptions,
	                       @Nullable Codec<Buffer, IN, OUT> codec,
	                       @Nonnull Collection<Consumer<NetChannel<IN, OUT>>> consumers) {
		super(env, reactor, listenAddress, options, sslOptions, codec, consumers);

		this.ioThreadCount = env.getProperty("reactor.zmq.ioThreadCount", Integer.class, 1);

		if (options instanceof ZeroMQServerSocketOptions) {
			this.zmqOpts = (ZeroMQServerSocketOptions) options;
		} else {
			this.zmqOpts = null;
		}

		this.threadPool = Executors.newCachedThreadPool(new NamedDaemonThreadFactory("zmq-tcp-server"));
	}

	@Override
	public TcpServer<IN, OUT> start(@Nullable final Runnable started) {
		Assert.isNull(worker, "This ZeroMQ server has already been started");

		UUID id = UUIDUtils.random();
		int socketType = (null != zmqOpts ? zmqOpts.socketType() : ZMQ.ROUTER);
		ZMQ.Context zmq = (null != zmqOpts ? zmqOpts.context() : null);
		this.worker = new ZeroMQWorker<IN, OUT>(id, socketType, ioThreadCount, zmq) {
			@Override
			protected void configure(ZMQ.Socket socket) {
				socket.setReceiveBufferSize(getOptions().rcvbuf());
				socket.setSendBufferSize(getOptions().sndbuf());
				socket.setBacklog(getOptions().backlog());
				if (getOptions().keepAlive()) {
					socket.setTCPKeepAlive(1);
				}
			}

			@Override
			protected void start(ZMQ.Socket socket) {
				if (log.isInfoEnabled()) {
					log.info("BIND: starting ZeroMQ server on {}", getListenAddress());
				}
				String addr = String.format("tcp://%s:%s",
				                            getListenAddress().getHostString(),
				                            getListenAddress().getPort());
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
				getReactor(),
				getReactor().getDispatcher(),
				getCodec()
		);
	}

	@Override
	public Promise<Boolean> shutdown() {
		if (null == worker) {
			return Promises.<Boolean>error(new IllegalStateException("This ZeroMQ server has not been started")).get();
		}

		Deferred<Boolean, Promise<Boolean>> d = Promises.defer(getEnvironment(), getReactor().getDispatcher());

		worker.shutdown();
		if (!workerFuture.isDone()) {
			workerFuture.cancel(true);
		}
		threadPool.shutdownNow();
		try {
			threadPool.awaitTermination(30, TimeUnit.SECONDS);
			d.accept(true);
		} catch (InterruptedException e) {
			d.accept(e);
		}
		notifyShutdown();

		return d.compose();
	}

}
