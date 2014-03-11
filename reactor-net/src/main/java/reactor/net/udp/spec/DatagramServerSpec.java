package reactor.net.udp.spec;

import reactor.core.Environment;
import reactor.core.Reactor;
import reactor.io.encoding.Codec;
import reactor.net.config.ServerSocketOptions;
import reactor.net.spec.NetServerSpec;
import reactor.net.udp.DatagramServer;
import reactor.util.Assert;

import java.lang.reflect.Constructor;
import java.net.InetSocketAddress;
import java.net.NetworkInterface;
import java.util.Collection;

/**
 * @author Jon Brisbin
 */
public class DatagramServerSpec<IN, OUT>
		extends NetServerSpec<IN, OUT, DatagramServerSpec<IN, OUT>, DatagramServer<IN, OUT>> {

	protected final Constructor<? extends DatagramServer> serverImplCtor;

	private NetworkInterface multicastInterface;

	public DatagramServerSpec(Class<? extends DatagramServer> serverImpl) {
		Assert.notNull(serverImpl, "NetServer implementation class cannot be null.");
		try {
			this.serverImplCtor = serverImpl.getDeclaredConstructor(
					Environment.class,
					Reactor.class,
					InetSocketAddress.class,
					NetworkInterface.class,
					ServerSocketOptions.class,
					Codec.class,
					Collection.class
			);
			this.serverImplCtor.setAccessible(true);
		} catch(NoSuchMethodException e) {
			throw new IllegalArgumentException(
					"No public constructor found that matches the signature of the one found in the DatagramServer class.");
		}
	}

	/**
	 * Set the interface to use for multicast.
	 *
	 * @param iface
	 * 		the {@link java.net.NetworkInterface} to use for multicast.
	 *
	 * @return {@literal this}
	 */
	public DatagramServerSpec<IN, OUT> multicastInterface(NetworkInterface iface) {
		this.multicastInterface = iface;
		return this;
	}

	@SuppressWarnings("unchecked")
	@Override
	protected DatagramServer<IN, OUT> configure(Reactor reactor, Environment environment) {
		try {
			return serverImplCtor.newInstance(
					environment,
					reactor,
					listenAddress,
					multicastInterface,
					options,
					codec,
					channelConsumers
			);
		} catch(Throwable t) {
			throw new IllegalStateException(t);
		}
	}

}
