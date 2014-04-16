package reactor.net.zmq;

import com.gs.collections.api.list.MutableList;
import com.gs.collections.impl.block.procedure.checked.CheckedProcedure;
import com.gs.collections.impl.list.mutable.FastList;
import com.gs.collections.impl.list.mutable.SynchronizedMutableList;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.zeromq.ZFrame;
import org.zeromq.ZMQ;
import org.zeromq.ZMsg;
import reactor.core.Environment;
import reactor.core.Reactor;
import reactor.core.composable.Deferred;
import reactor.core.composable.Promise;
import reactor.event.dispatch.Dispatcher;
import reactor.function.Consumer;
import reactor.io.Buffer;
import reactor.io.encoding.Codec;
import reactor.net.AbstractNetChannel;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import java.net.InetSocketAddress;
import java.nio.ByteBuffer;
import java.util.concurrent.atomic.AtomicReferenceFieldUpdater;

/**
 * @author Jon Brisbin
 */
public class ZeroMQNetChannel<IN, OUT> extends AbstractNetChannel<IN, OUT> {

	private static final AtomicReferenceFieldUpdater<ZeroMQNetChannel, ZMsg> MSG_UPD =
			AtomicReferenceFieldUpdater.newUpdater(ZeroMQNetChannel.class, ZMsg.class, "currentMsg");

	private final Logger                log           = LoggerFactory.getLogger(getClass());
	private final ZeroMQConsumerSpec    eventSpec     = new ZeroMQConsumerSpec();
	private final MutableList<Runnable> closeHandlers = SynchronizedMutableList.of(FastList.<Runnable>newList());

	private volatile String     connectionId;
	private volatile ZMQ.Socket socket;
	private volatile ZMsg       currentMsg;

	public ZeroMQNetChannel(@Nonnull Environment env,
	                        @Nonnull Reactor eventsReactor,
	                        @Nonnull Dispatcher ioDispatcher,
	                        @Nullable Codec<Buffer, IN, OUT> codec) {
		super(env, codec, ioDispatcher, eventsReactor);
	}

	public ZeroMQNetChannel<IN, OUT> setConnectionId(String connectionId) {
		this.connectionId = connectionId;
		return this;
	}

	public ZeroMQNetChannel<IN, OUT> setSocket(ZMQ.Socket socket) {
		this.socket = socket;
		return this;
	}

	@Override
	public InetSocketAddress remoteAddress() {
		return null;
	}

	@Override
	protected void write(ByteBuffer data, final Deferred<Void, Promise<Void>> onComplete, boolean flush) {
		byte[] bytes = new byte[data.remaining()];
		data.get(bytes);
		boolean isNewMsg = MSG_UPD.compareAndSet(this, null, new ZMsg());
		ZMsg msg = MSG_UPD.get(this);
		if (isNewMsg) {
			switch (socket.getType()) {
				case ZMQ.DEALER:
					break;
				default:
					msg.add(new ZFrame(connectionId));
			}
		}
		msg.add(new ZFrame(bytes));

		if (flush) {
			doFlush(onComplete);
		}
	}

	@SuppressWarnings("unchecked")
	@Override
	protected void write(Object data, Deferred<Void, Promise<Void>> onComplete, boolean flush) {
		Buffer buff = getEncoder().apply((OUT) data);
		write(buff.byteBuffer(), onComplete, flush);
	}

	@Override
	protected synchronized void flush() {
		doFlush(null);
	}

	private void doFlush(final Deferred<Void, Promise<Void>> onComplete) {
		ZMsg msg = MSG_UPD.getAndSet(ZeroMQNetChannel.this, null);
		if (null != msg) {
			msg.send(socket);
			if (null != onComplete) {
				onComplete.accept((Void) null);
			}
		}
	}

	@Override
	public void close(final Consumer<Boolean> onClose) {
		socket.close();
		getEventsReactor().schedule(new Consumer<Void>() {
			@Override
			public void accept(Void v) {
				closeHandlers.forEach(new CheckedProcedure<Runnable>() {
					@Override
					public void safeValue(Runnable r) throws Exception {
						r.run();
					}
				});
				closeHandlers.clear();
				onClose.accept(true);
			}
		}, null);
	}

	@Override
	public ConsumerSpec on() {
		return eventSpec;
	}

	private class ZeroMQConsumerSpec implements ConsumerSpec {
		@Override
		public ConsumerSpec close(Runnable onClose) {
			closeHandlers.add(onClose);
			return this;
		}

		@Override
		public ConsumerSpec readIdle(long idleTimeout, Runnable onReadIdle) {
			return this;
		}

		@Override
		public ConsumerSpec writeIdle(long idleTimeout, Runnable onWriteIdle) {
			return this;
		}
	}

}
