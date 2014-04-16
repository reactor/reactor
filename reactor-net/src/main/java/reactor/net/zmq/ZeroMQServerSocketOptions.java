package reactor.net.zmq;

import org.zeromq.ZMQ;
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

}
