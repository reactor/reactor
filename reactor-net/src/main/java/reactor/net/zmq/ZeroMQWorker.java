package reactor.net.zmq;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.zeromq.*;
import reactor.io.Buffer;

import java.util.UUID;

/**
 * @author Jon Brisbin
 */
public abstract class ZeroMQWorker<IN, OUT> implements Runnable {

	private final Logger log   = LoggerFactory.getLogger(getClass());
	private final ZLoop  zloop = new ZLoop();

	private final UUID                id;
	private final int                 socketType;
	private final int                 ioThreadCount;
	private final ZLoop.IZLoopHandler inputHandler;

	private volatile boolean      closed;
	private volatile boolean      shutdownCtx;
	private volatile ZContext     zmq;
	private volatile ZMQ.Socket   socket;
	private volatile ZMQ.PollItem pollin;

	public ZeroMQWorker(UUID id, int socketType, int ioThreadCount, ZContext zmq) {
		this.id = id;
		this.socketType = socketType;
		this.ioThreadCount = ioThreadCount;
		this.zmq = zmq;
		this.inputHandler = new ZLoop.IZLoopHandler() {
			@Override
			public int handle(ZLoop loop, ZMQ.PollItem item, Object arg) {
				ZMsg msg = ZMsg.recvMsg(socket);
				if (null == msg || msg.size() == 0) {
					return 0;
				}
				if (closed) {
					return -1;
				}

				String connId;
				switch (ZeroMQWorker.this.socketType) {
					case ZMQ.ROUTER:
						connId = msg.popString();
						break;
					default:
						connId = ZeroMQWorker.this.id.toString();
				}
				ZeroMQNetChannel<IN, OUT> netChannel = select(connId)
						.setConnectionId(connId)
						.setSocket(socket);

				ZFrame content;
				while (null != (content = msg.pop())) {
					netChannel.read(Buffer.wrap(content.getData()));
				}
				msg.destroy();

				return 0;
			}
		};
	}

	@Override
	public void run() {
		if (closed) {
			return;
		}
		if (null == zmq) {
			zmq = new ZContext(ioThreadCount);
			shutdownCtx = true;
		}
		socket = zmq.createSocket(socketType);
		socket.setIdentity(id.toString().getBytes());
		socket.setReceiveTimeOut(50);
		configure(socket);

		pollin = new ZMQ.PollItem(socket, ZMQ.Poller.POLLIN);
		if (log.isTraceEnabled()) {
			zloop.verbose(true);
		}
		zloop.addPoller(pollin, inputHandler, null);

		start(socket);

		zloop.start();
	}

	public void shutdown() {
		if (closed) {
			return;
		}
		zloop.removePoller(pollin);
		zloop.destroy();

		//zmq.destroySocket(socket);
		closed = true;

		if (shutdownCtx) {
			zmq.destroy();
		}
	}

	protected abstract void configure(ZMQ.Socket socket);

	protected abstract void start(ZMQ.Socket socket);

	protected abstract ZeroMQNetChannel<IN, OUT> select(Object id);

}
