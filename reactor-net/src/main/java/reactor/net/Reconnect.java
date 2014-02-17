package reactor.net;

import reactor.tuple.Tuple2;

import java.net.InetSocketAddress;

/**
 * Implementations of this interface will be instantiated by a {@link reactor.function.Supplier} to provide information
 * to the {@link reactor.net.tcp.TcpClient} whether or not to attempt to reconnect a broken connection.
 * <p/>
 * The {@link #reconnect(java.net.InetSocketAddress, int)} method will be invoked, passing the currently-connected
 * address and the number of times a reconnection has been attempted on this connection. If the client is to reconnect
 * to a different host, then provide that different address in the return value. If you don't want to try and reconnect
 * at all, simply return {@code null}.
 *
 * @author Jon Brisbin
 */
public interface Reconnect {

	/**
	 * Provide an {@link InetSocketAddress} to which a reconnection attempt should be made.
	 *
	 * @param currentAddress the address to which the client is currently connected
	 * @return a possibly different {@link InetSocketAddress} to which a reconnection attempt should be made and a {@code
	 *         Long} denoting the time to delay a reconnection attempt
	 */
	Tuple2<InetSocketAddress, Long> reconnect(InetSocketAddress currentAddress, int attempt);

}
