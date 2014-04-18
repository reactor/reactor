package reactor.net.zmq;

import org.zeromq.ZFrame;
import org.zeromq.ZLoop;
import org.zeromq.ZMQ;
import org.zeromq.ZMsg;
import reactor.io.Buffer;

import java.util.UUID;

/**
 * @author Jon Brisbin
 */
public abstract class ZeroMQWorker<IN, OUT> implements Runnable {

	private final ZLoop zloop = new ZLoop();

	private final UUID                id;
	private final int                 socketType;
	private final int                 ioThreadCount;
	private final ZLoop.IZLoopHandler inputHandler;

	private volatile boolean      shutdownCtx;
	private volatile ZMQ.Context  zmq;
	private volatile ZMQ.Socket   socket;
	private volatile ZMQ.PollItem pollin;

	public ZeroMQWorker(UUID id, int socketType, int ioThreadCount, ZMQ.Context zmq) {
		this.id = id;
		this.socketType = socketType;
		this.ioThreadCount = ioThreadCount;
		this.zmq = zmq;
		this.inputHandler = new ZLoop.IZLoopHandler() {
			@Override
			public int handle(ZLoop loop, ZMQ.PollItem item, Object arg) {
				ZMsg msg = ZMsg.recvMsg(socket);

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
		if (null == zmq) {
			zmq = ZMQ.context(ioThreadCount);
			shutdownCtx = true;
		}
		socket = zmq.socket(socketType);
		socket.setIdentity(id.toString().getBytes());
		configure(socket);

		pollin = new ZMQ.PollItem(socket, ZMQ.Poller.POLLIN);
		zloop.addPoller(pollin, inputHandler, null);

		start(socket);

		zloop.start();
	}

	public void shutdown() {
		zloop.removePoller(pollin);
		zloop.destroy();
		socket.close();
		if (shutdownCtx) {
			zmq.term();
		}
	}

	protected abstract void configure(ZMQ.Socket socket);

	protected abstract void start(ZMQ.Socket socket);

	protected abstract ZeroMQNetChannel<IN, OUT> select(Object id);

}
