package reactor.net.zmq.tcp;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.zeromq.ZMQ;
import reactor.core.Environment;
import reactor.core.Reactor;
import reactor.core.composable.Deferred;
import reactor.core.composable.Promise;
import reactor.core.composable.Stream;
import reactor.core.composable.spec.Promises;
import reactor.function.Consumer;
import reactor.io.Buffer;
import reactor.io.encoding.Codec;
import reactor.net.NetChannel;
import reactor.net.Reconnect;
import reactor.net.config.ClientSocketOptions;
import reactor.net.config.SslOptions;
import reactor.net.tcp.TcpClient;
import reactor.net.zmq.ZeroMQClientSocketOptions;
import reactor.net.zmq.ZeroMQNetChannel;
import reactor.net.zmq.ZeroMQWorker;
import reactor.support.NamedDaemonThreadFactory;
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
public class ZeroMQTcpClient<IN, OUT> extends TcpClient<IN, OUT> {

	private final Logger log = LoggerFactory.getLogger(getClass());

	private final int                       ioThreadCount;
	private final ZeroMQClientSocketOptions zmqOpts;
	private final ExecutorService           threadPool;

	private volatile ZeroMQWorker<IN, OUT> worker;
	private volatile Future<?>             workerFuture;

	public ZeroMQTcpClient(@Nonnull Environment env,
	                       @Nonnull Reactor reactor,
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

		this.threadPool = Executors.newCachedThreadPool(new NamedDaemonThreadFactory("zmq-tcp-client"));
	}

	@Override
	public Promise<NetChannel<IN, OUT>> open() {
		final Deferred<NetChannel<IN, OUT>, Promise<NetChannel<IN, OUT>>> d =
				Promises.defer(getEnvironment(), getReactor().getDispatcher());

		final UUID id = UUIDUtils.random();
		int socketType = (null != zmqOpts ? zmqOpts.socketType() : ZMQ.DEALER);
		ZMQ.Context zmq = (null != zmqOpts ? zmqOpts.context() : null);
		this.worker = new ZeroMQWorker<IN, OUT>(id, socketType, ioThreadCount, zmq) {
			@Override
			protected void configure(ZMQ.Socket socket) {
				socket.setReceiveBufferSize(getOptions().rcvbuf());
				socket.setSendBufferSize(getOptions().sndbuf());
				if (getOptions().keepAlive()) {
					socket.setTCPKeepAlive(1);
				}
			}

			@Override
			protected void start(final ZMQ.Socket socket) {
				if (log.isInfoEnabled()) {
					log.info("CONNECT: connecting ZeroMQ client to {}", getConnectAddress());
				}
				String addr = String.format("tcp://%s:%s",
				                            getConnectAddress().getHostString(),
				                            getConnectAddress().getPort());
				socket.connect(addr);
				notifyStart(new Runnable() {
					@Override
					public void run() {
						d.accept(select(id.toString())
								         .setConnectionId(id.toString())
								         .setSocket(socket));
					}
				});
			}

			@Override
			protected ZeroMQNetChannel<IN, OUT> select(Object id) {
				return (ZeroMQNetChannel<IN, OUT>) ZeroMQTcpClient.this.select(id);
			}
		};
		this.workerFuture = this.threadPool.submit(this.worker);

		return d.compose();
	}

	@Override
	public Stream<NetChannel<IN, OUT>> open(Reconnect reconnect) {
		return null;
	}

	@Override
	public void close(@Nullable Consumer<Boolean> onClose) {
		if (null == worker) {
			throw new IllegalStateException("This ZeroMQ server has not been started");
		}

		worker.shutdown();
		if (!workerFuture.isDone()) {
			workerFuture.cancel(true);
		}
		threadPool.shutdownNow();
		try {
			threadPool.awaitTermination(30, TimeUnit.SECONDS);
			super.close(onClose);
		} catch (InterruptedException e) {
			Thread.currentThread().interrupt();
		} finally {
			notifyShutdown();
		}
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

}
