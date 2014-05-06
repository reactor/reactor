package reactor.net.zmq.tcp;

import com.gs.collections.api.list.MutableList;
import com.gs.collections.impl.block.function.checked.CheckedFunction0;
import com.gs.collections.impl.block.predicate.checked.CheckedPredicate;
import com.gs.collections.impl.list.mutable.FastList;
import com.gs.collections.impl.list.mutable.SynchronizedMutableList;
import com.gs.collections.impl.map.mutable.SynchronizedMutableMap;
import com.gs.collections.impl.map.mutable.UnifiedMap;
import org.zeromq.ZMQ;
import reactor.core.Environment;
import reactor.core.Reactor;
import reactor.core.spec.Reactors;
import reactor.event.dispatch.Dispatcher;
import reactor.io.Buffer;
import reactor.io.encoding.Codec;
import reactor.net.NetChannel;
import reactor.net.tcp.TcpClient;
import reactor.net.tcp.TcpServer;
import reactor.net.tcp.spec.TcpClientSpec;
import reactor.net.tcp.spec.TcpServerSpec;
import reactor.net.zmq.ZeroMQClientSocketOptions;
import reactor.net.zmq.ZeroMQServerSocketOptions;
import reactor.rx.Promise;
import reactor.rx.spec.Promises;
import reactor.util.Assert;

import java.lang.reflect.Field;
import java.util.concurrent.TimeUnit;

/**
 * @author Jon Brisbin
 */
public class ZeroMQ<T> {

	private static final SynchronizedMutableMap<Integer, String> SOCKET_TYPES = SynchronizedMutableMap.of(UnifiedMap.<Integer, String>newMap());

	private final MutableList<TcpClient<T, T>> clients = SynchronizedMutableList.of(FastList.<TcpClient<T, T>>newList());
	private final MutableList<TcpServer<T, T>> servers = SynchronizedMutableList.of(FastList.<TcpServer<T, T>>newList());

	private final Environment env;
	private final Dispatcher  dispatcher;
	private final Reactor     reactor;

	private volatile Codec<Buffer, T, T> codec;
	private volatile boolean             shutdown;

	public ZeroMQ(Environment env) {
		this(env, env.getDefaultDispatcher());
	}

	public ZeroMQ(Environment env, String dispatcher) {
		this(env, env.getDispatcher(dispatcher));
	}

	public ZeroMQ(Environment env, Dispatcher dispatcher) {
		this.env = env;
		this.dispatcher = dispatcher;
		this.reactor = Reactors.reactor(env, dispatcher);
	}

	public static String findSocketTypeName(final int socketType) {
		return SOCKET_TYPES.getIfAbsentPut(socketType, new CheckedFunction0<String>() {
			@Override
			public String safeValue() throws Exception {
				for (Field f : ZMQ.class.getDeclaredFields()) {
					if (int.class.isAssignableFrom(f.getType())) {
						f.setAccessible(true);
						try {
							int val = f.getInt(null);
							if (socketType == val) {
								return f.getName();
							}
						} catch (IllegalAccessException e) {
						}
					}
				}
				return "";
			}
		});
	}

	public ZeroMQ<T> codec(Codec<Buffer, T, T> codec) {
		this.codec = codec;
		return this;
	}

	public Promise<NetChannel<T, T>> dealer(String addrs) {
		return createClient(addrs, ZMQ.DEALER);
	}

	public Promise<NetChannel<T, T>> push(String addrs) {
		return createClient(addrs, ZMQ.PUSH);
	}

	public Promise<NetChannel<T, T>> pull(String addrs) {
		return createServer(addrs, ZMQ.PULL);
	}

	public Promise<NetChannel<T, T>> router(String addrs) {
		return createServer(addrs, ZMQ.ROUTER);
	}

	private Promise<NetChannel<T, T>> createClient(String addrs, int socketType) {
		Assert.isTrue(!shutdown, "This ZeroMQ instance has been shut down");
		Assert.isTrue(addrs.startsWith("tcp:"), "Only tcp:// addresses currently supported.");

		TcpClient<T, T> client = new TcpClientSpec<T, T>(ZeroMQTcpClient.class)
				.env(env).dispatcher(dispatcher).codec(codec)
				.options(new ZeroMQClientSocketOptions()
						         .connectAddresses(addrs)
						         .socketType(socketType))
				.get();

		clients.add(client);

		return client.open();
	}

	public Promise<NetChannel<T, T>> createServer(String addrs, int socketType) {
		Assert.isTrue(!shutdown, "This ZeroMQ instance has been shut down");
		Assert.isTrue(addrs.startsWith("tcp:"), "Only tcp:// addresses currently supported.");

		Promise<NetChannel<T, T>> d = Promises.defer(env, dispatcher);

		TcpServer<T, T> server = new TcpServerSpec<T, T>(ZeroMQTcpServer.class)
				.env(env).dispatcher(dispatcher).codec(codec)
				.options(new ZeroMQServerSocketOptions()
						         .listenAddresses(addrs)
						         .socketType(socketType))
				.consume(d)
				.get();

		servers.add(server);

		server.start();

		return d;
	}

	public void shutdown() {
		clients.removeIf(new CheckedPredicate<TcpClient<T, T>>() {
			@Override
			public boolean safeAccept(TcpClient<T, T> client) throws Exception {
				Assert.isTrue(client.close().await(60, TimeUnit.SECONDS), "Client " + client + " not properly shut down");
				return true;
			}
		});
		servers.removeIf(new CheckedPredicate<TcpServer<T, T>>() {
			@Override
			public boolean safeAccept(TcpServer<T, T> server) throws Exception {
				Assert.isTrue(server.shutdown().await(60, TimeUnit.SECONDS), "Server " + server + " not properly shut down");
				return true;
			}
		});
		shutdown = true;
	}

}
