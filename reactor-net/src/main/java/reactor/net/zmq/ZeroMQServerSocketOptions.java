package reactor.net.zmq;

import org.zeromq.ZMQ;
import reactor.function.Consumer;
import reactor.net.config.ServerSocketOptions;
import reactor.util.Assert;

/**
 * {@link reactor.net.config.ServerSocketOptions} that include ZeroMQ-specific configuration options.
 *
 * @author Jon Brisbin
 */
public class ZeroMQServerSocketOptions extends ServerSocketOptions {

	private ZMQ.Context context;
	private int socketType = ZMQ.ROUTER;
	private Consumer<ZMQ.Socket> socketConfigurer;
	private String               listenAddresses;

	/**
	 * Get the {@link org.zeromq.ZMQ.Context} to use for IO.
	 *
	 * @return the {@link org.zeromq.ZMQ.Context} to use
	 */
	public ZMQ.Context context() {
		return context;
	}

	/**
	 * Set the {@link org.zeromq.ZMQ.Context} to use for IO.
	 *
	 * @param context
	 * 		the {@link org.zeromq.ZMQ.Context} to use
	 *
	 * @return {@literal this}
	 */
	public ZeroMQServerSocketOptions context(ZMQ.Context context) {
		Assert.notNull(context, "ZeroMQ Context cannot be null");
		this.context = context;
		return this;
	}

	/**
	 * The type of the ZMQ socket to create.
	 *
	 * @return the ZMQ socket type
	 */
	public int socketType() {
		return socketType;
	}

	/**
	 * Set the type of ZMQ socket to create.
	 *
	 * @param socketType
	 * 		the ZMQ socket type
	 *
	 * @return {@literal this}
	 */
	public ZeroMQServerSocketOptions socketType(int socketType) {
		this.socketType = socketType;
		return this;
	}


	/**
	 * The {@link reactor.function.Consumer} responsible for configuring the underlying ZeroMQ socket.
	 *
	 * @return the ZMQ.Socket configurer
	 */
	public Consumer<ZMQ.Socket> socketConfigurer() {
		return socketConfigurer;
	}

	/**
	 * Set the {@link reactor.function.Consumer} responsible for configure the underlying ZeroMQ socket.
	 *
	 * @param socketConfigurer
	 * 		the ZMQ.Socket configurer
	 *
	 * @return {@literal this}
	 */
	public ZeroMQServerSocketOptions socketConfigurer(Consumer<ZMQ.Socket> socketConfigurer) {
		this.socketConfigurer = socketConfigurer;
		return this;
	}


	public String listenAddresses() {
		return listenAddresses;
	}

	public ZeroMQServerSocketOptions listenAddresses(String listenAddresses) {
		this.listenAddresses = listenAddresses;
		return this;
	}

}
