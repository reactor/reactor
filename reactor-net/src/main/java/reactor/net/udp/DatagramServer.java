package reactor.net.udp;

import reactor.core.Environment;
import reactor.core.Reactor;
import reactor.core.composable.Deferred;
import reactor.core.composable.Promise;
import reactor.core.composable.Stream;
import reactor.core.composable.spec.Promises;
import reactor.function.Consumer;
import reactor.function.batch.BatchConsumer;
import reactor.io.Buffer;
import reactor.io.encoding.Codec;
import reactor.net.AbstractNetPeer;
import reactor.net.NetChannel;
import reactor.net.NetServer;
import reactor.net.config.ServerSocketOptions;
import reactor.util.Assert;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.net.NetworkInterface;
import java.util.Collection;

/**
 * @author Jon Brisbin
 */
public abstract class DatagramServer<IN, OUT>
		extends AbstractNetPeer<IN, OUT>
		implements NetServer<IN, OUT> {

	private final InetSocketAddress   listenAddress;
	private final NetworkInterface    multicastInterface;
	private final ServerSocketOptions options;

	protected DatagramServer(@Nonnull Environment env,
	                         @Nonnull Reactor reactor,
	                         @Nullable InetSocketAddress listenAddress,
	                         @Nullable NetworkInterface multicastInterface,
	                         @Nonnull ServerSocketOptions options,
	                         @Nullable Codec<Buffer, IN, OUT> codec,
	                         @Nonnull Collection<Consumer<NetChannel<IN, OUT>>> consumers) {
		super(env, reactor, codec, consumers);
		Assert.notNull(options, "ServerSocketOptions cannot be null");
		this.listenAddress = listenAddress;
		this.multicastInterface = multicastInterface;
		this.options = options;
	}

	/**
	 * Start this server.
	 *
	 * @return {@literal this}
	 */
	public Promise<Boolean> start() {
		final Deferred<Boolean, Promise<Boolean>> d = Promises.defer(getEnvironment(), getReactor().getDispatcher());
		start(new Runnable() {
			@Override
			public void run() {
				d.accept(true);
			}
		});
		return d.compose();
	}

	@Override
	public abstract DatagramServer<IN, OUT> start(@Nullable Runnable started);

	/**
	 * Send data to peers.
	 *
	 * @param data
	 * 		the data to send
	 *
	 * @return {@literal this}
	 */
	public abstract DatagramServer<IN, OUT> send(OUT data);

	/**
	 * Retrieve the {@link reactor.core.composable.Stream} on which can be composed actions to take when data comes into
	 * this {@literal DatagramServer}.
	 *
	 * @return the input {@link reactor.core.composable.Stream}
	 */
	public abstract Stream<IN> in();

	/**
	 * Retrieve the {@link reactor.core.composable.Stream} into which data can accepted for sending to peers.
	 *
	 * @return a {@link reactor.function.batch.BatchConsumer} for sending data out
	 */
	public abstract BatchConsumer<OUT> out();

	/**
	 * Join a multicast group.
	 *
	 * @param multicastAddress
	 * 		multicast address of the group to join
	 * @param iface
	 * 		interface to use for multicast
	 *
	 * @return {@literal this}
	 */
	public abstract Promise<Void> join(InetAddress multicastAddress, NetworkInterface iface);

	/**
	 * Join a multicast group.
	 *
	 * @param multicastAddress
	 * 		multicast address of the group to join
	 *
	 * @return {@literal this}
	 */
	public Promise<Void> join(InetAddress multicastAddress) {
		return join(multicastAddress, null);
	}

	/**
	 * Leave a multicast group.
	 *
	 * @param multicastAddress
	 * 		multicast address of the group to leave
	 * @param iface
	 * 		interface to use for multicast
	 *
	 * @return {@literal this}
	 */
	public abstract Promise<Void> leave(InetAddress multicastAddress, NetworkInterface iface);

	/**
	 * Leave a multicast group.
	 *
	 * @param multicastAddress
	 * 		multicast address of the group to leave
	 *
	 * @return {@literal this}
	 */
	public Promise<Void> leave(InetAddress multicastAddress) {
		return leave(multicastAddress, null);
	}

	/**
	 * Get the address to which this server is bound.
	 *
	 * @return
	 */
	protected InetSocketAddress getListenAddress() {
		return listenAddress;
	}

	/**
	 * Get the {@link java.net.NetworkInterface} on which multicast will be performed.
	 *
	 * @return
	 */
	protected NetworkInterface getMulticastInterface() { return multicastInterface; }

	/**
	 * Get the {@link reactor.net.config.ServerSocketOptions} currently in effect.
	 *
	 * @return
	 */
	protected ServerSocketOptions getOptions() {
		return options;
	}

}
