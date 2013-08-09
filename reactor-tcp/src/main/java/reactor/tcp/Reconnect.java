package reactor.tcp;

import java.net.InetSocketAddress;

/**
 * Implementations of this interface will be instantiated by a {@link reactor.function.Supplier} to provide information
 * to the {@link TcpClient} whether or not to attempt to reconnect a broken connection.
 * <p/>
 * First, the {@link #reconnectTo(java.net.InetSocketAddress)} method will be invoked, passing the currently-connected
 * address. If the client is to reconnect to a different host, then provide that different address in the return value.
 * If you don't want to try and reconnect at all, simply return {@code null}.
 * <p/>
 * Second, the {@link #reconnectAfter(long)} method will be invoked. A number of milliseconds after which to attempt to
 * reconnect is expected as a return value.
 * <p/>
 * If the connection was successful, then the {@link #close()} method is called. If all reconnection attempts have been
 * exhausted (i.e. {@link #reconnectTo(java.net.InetSocketAddress)} returned {@code null}) then the {@link #close()}
 * will be invoked at that point.
 *
 * @author Jon Brisbin
 */
public interface Reconnect {

	/**
	 * Provide an {@link InetSocketAddress} to which a reconnection attempt should be made.
	 *
	 * @param currentAddress the address to which the client is currently connected
	 * @return a possibly different {@link InetSocketAddress} to which a reconnection attempt should be made
	 */
	InetSocketAddress reconnectTo(InetSocketAddress currentAddress);

	/**
	 * Provide a number of milliseconds to delay reconnection.
	 *
	 * @param elapsedSinceDisconnect the elapsed time, in milliseconds, since this connection was disconnected
	 * @return the delay, in milliseconds
	 */
	long reconnectAfter(long elapsedSinceDisconnect);

	/**
	 * Clear any resources created for reconnection as this object will now go out-of-scope.
	 */
	void close();

}
