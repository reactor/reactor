/*
 * Copyright (c) 2011-2014 Pivotal Software, Inc.
 *
 *  Licensed under the Apache License, Version 2.0 (the "License");
 *  you may not use this file except in compliance with the License.
 *  You may obtain a copy of the License at
 *
 *         http://www.apache.org/licenses/LICENSE-2.0
 *
 *  Unless required by applicable law or agreed to in writing, software
 *  distributed under the License is distributed on an "AS IS" BASIS,
 *  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  See the License for the specific language governing permissions and
 *  limitations under the License.
 */

package reactor.io.net.impl.zmq;

import com.gs.collections.api.list.MutableList;
import com.gs.collections.impl.block.predicate.checked.CheckedPredicate;
import com.gs.collections.impl.list.mutable.FastList;
import com.gs.collections.impl.list.mutable.SynchronizedMutableList;
import org.reactivestreams.Subscriber;
import org.zeromq.ZFrame;
import org.zeromq.ZMQ;
import org.zeromq.ZMsg;
import reactor.Environment;
import reactor.core.Dispatcher;
import reactor.fn.Consumer;
import reactor.io.buffer.Buffer;
import reactor.io.codec.Codec;
import reactor.io.net.ChannelStream;
import reactor.io.net.PeerStream;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import java.net.InetSocketAddress;
import java.nio.ByteBuffer;

/**
 * @author Jon Brisbin
 * @author Stephane Maldini
 */
public class ZeroMQChannelStream<IN, OUT> extends ChannelStream<IN, OUT> {

	private final ZeroMQConsumerSpec          eventSpec     = new ZeroMQConsumerSpec();
	private final MutableList<Consumer<Void>> closeHandlers = SynchronizedMutableList.of(FastList
			.<Consumer<Void>>newList());

	private volatile String     connectionId;
	private volatile ZMQ.Socket socket;

	private ZMsg currentMsg;

	public ZeroMQChannelStream(@Nonnull Environment env,
	                           long prefetch,
	                           PeerStream<IN, OUT, ChannelStream<IN, OUT>> peer,
	                           @Nonnull Dispatcher eventsDispatcher,
	                           @Nonnull Dispatcher ioDispatcher,
	                           @Nullable Codec<Buffer, IN, OUT> codec) {
		super(env, codec, prefetch, peer, ioDispatcher, eventsDispatcher);
	}

	public ZeroMQChannelStream<IN, OUT> setConnectionId(String connectionId) {
		this.connectionId = connectionId;
		return this;
	}

	public ZeroMQChannelStream<IN, OUT> setSocket(ZMQ.Socket socket) {
		this.socket = socket;
		return this;
	}

	@Override
	public InetSocketAddress remoteAddress() {
		return null;
	}

	@Override
	protected void write(ByteBuffer data, final Subscriber<?> onComplete, boolean flush) {
		byte[] bytes = new byte[data.remaining()];
		data.get(bytes);
		boolean isNewMsg;
		ZMsg msg;
		synchronized (this) {
			msg = currentMsg;
			currentMsg = new ZMsg();
			if (msg == null) {
				msg = currentMsg;
				isNewMsg = true;
			}else{
				isNewMsg = false;
			}
		}
		if (isNewMsg) {
			switch (socket.getType()) {
				case ZMQ.ROUTER:
					msg.add(new ZFrame(connectionId));
					break;
				default:
			}
		}
		msg.add(new ZFrame(bytes));

		if (flush) {
			doFlush(onComplete);
		}
	}

	@Override
	protected void write(Buffer data, Subscriber<?> onComplete, boolean flush) {
		write(data.byteBuffer(), onComplete, flush);
	}

	@SuppressWarnings("unchecked")
	@Override
	protected void write(Object data, Subscriber<?> onComplete, boolean flush) {
		Buffer buff = getEncoder().apply((OUT) data);
		write(buff.byteBuffer(), onComplete, flush);
	}

	@Override
	protected synchronized void flush() {
		doFlush(null);
	}

	private void doFlush(final Subscriber<?> onComplete) {
		ZMsg msg;
		synchronized (this) {
			msg = currentMsg;
			currentMsg = null;
		}
		if (null != msg) {
			boolean success = msg.send(socket);
			if (null != onComplete) {
				if (success) {
					onComplete.onComplete();
				} else {
					onComplete.onError(new RuntimeException("ZeroMQ Message could not be sent"));
				}
			}
		}
	}

	public void close() {
		getDispatcher().dispatch(null, new Consumer<Void>() {
			@Override
			public void accept(Void v) {
				closeHandlers.removeIf(new CheckedPredicate<Consumer<Void>>() {
					@Override
					public boolean safeAccept(Consumer<Void> r) throws Exception {
						r.accept(null);
						return true;
					}
				});
			}
		}, null);
	}

	@Override
	public ConsumerSpec on() {
		return eventSpec;
	}


	@Override
	public ZMQ.Socket delegate() {
		return socket;
	}

	@Override
	public String toString() {
		return "ZeroMQNetChannel{" +
				"closeHandlers=" + closeHandlers +
				", connectionId='" + connectionId + '\'' +
				", socket=" + socket +
				'}';
	}

	private class ZeroMQConsumerSpec implements ConsumerSpec {
		@Override
		public ConsumerSpec close(Consumer<Void> onClose) {
			closeHandlers.add(onClose);
			return this;
		}

		@Override
		public ConsumerSpec readIdle(long idleTimeout, Consumer<Void> onReadIdle) {
			return this;
		}

		@Override
		public ConsumerSpec writeIdle(long idleTimeout, Consumer<Void> onWriteIdle) {
			return this;
		}
	}

}
