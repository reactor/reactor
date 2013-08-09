/*
 * Copyright (c) 2011-2013 GoPivotal, Inc. All Rights Reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *       http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package reactor.tcp.netty;

import io.netty.buffer.ByteBuf;
import io.netty.channel.ChannelFuture;
import io.netty.channel.ChannelFutureListener;
import io.netty.channel.socket.SocketChannel;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import reactor.core.Environment;
import reactor.core.Reactor;
import reactor.event.Event;
import reactor.event.dispatch.Dispatcher;
import reactor.function.Consumer;
import reactor.function.Supplier;
import reactor.io.Buffer;
import reactor.tcp.AbstractTcpConnection;
import reactor.tcp.Reconnect;
import reactor.tcp.encoding.Codec;

import java.net.InetSocketAddress;
import java.nio.ByteBuffer;
import java.util.TimerTask;
import java.util.concurrent.atomic.AtomicBoolean;

/**
 * A {@link reactor.tcp.TcpConnection} implementation that uses Netty.
 *
 * @author Jon Brisbin
 * @author Stephane Maldini
 */
class NettyTcpConnection<IN, OUT> extends AbstractTcpConnection<IN, OUT> {

	private final Logger log = LoggerFactory.getLogger(getClass());

	private volatile Supplier<Reconnect>         reconnectSupplier;
	private volatile Consumer<InetSocketAddress> addressSetter;
	private volatile Supplier<ChannelFuture>     connectionSupplier;
	private volatile SocketChannel               channel;
	private volatile InetSocketAddress           remoteAddress;
	private volatile AtomicBoolean               reconnecting;
	private volatile long                        disconnectedAt;

	NettyTcpConnection(final Environment env,
										 Codec<Buffer, IN, OUT> codec,
										 Dispatcher ioDispatcher,
										 Reactor eventsReactor,
										 SocketChannel channel) {
		this(env, codec, ioDispatcher, eventsReactor, channel, null, null, null, null, null);
	}

	NettyTcpConnection(final Environment env,
										 Codec<Buffer, IN, OUT> codec,
										 Dispatcher ioDispatcher,
										 Reactor eventsReactor,
										 SocketChannel channel,
										 InetSocketAddress remoteAddress,
										 Supplier<Reconnect> reconnectSupplier,
										 Consumer<InetSocketAddress> addressSetter,
										 Supplier<ChannelFuture> connectionSupplier,
										 AtomicBoolean reconnecting) {
		super(env, codec, ioDispatcher, eventsReactor);
		this.channel = channel;
		this.remoteAddress = remoteAddress;
		this.reconnectSupplier = reconnectSupplier;
		this.addressSetter = addressSetter;
		this.connectionSupplier = connectionSupplier;
		this.reconnecting = reconnecting;

		addReconnectListener(this.channel.closeFuture(), null);
	}

	@Override
	public void close() {
		super.close();
		reconnectSupplier = null;
		connectionSupplier = null;
		addressSetter = null;
		try {
			channel.close().await();
		} catch (InterruptedException e) {
			throw new IllegalStateException(e.getMessage(), e);
		}
	}

	@Override
	public boolean consumable() {
		return !channel.isInputShutdown();
	}

	@Override
	public boolean writable() {
		return !channel.isOutputShutdown();
	}

	@Override
	public InetSocketAddress remoteAddress() {
		return remoteAddress;
	}

	@Override
	protected void write(Buffer data, final Consumer<Boolean> onComplete) {
		write(data.byteBuffer(), onComplete);
	}

	protected void write(ByteBuffer data, final Consumer<Boolean> onComplete) {
		ByteBuf buf = channel.alloc().buffer(data.remaining());
		buf.writeBytes(data);

		write(buf, onComplete);
	}

	@Override
	protected void write(Object data, final Consumer<Boolean> onComplete) {
		ChannelFuture writeFuture = channel.writeAndFlush(data);
		writeFuture.addListener(new ChannelFutureListener() {
			@Override
			public void operationComplete(ChannelFuture future) throws Exception {
				boolean success = future.isSuccess();

				if (!success) {
					Throwable t = future.cause();
					eventsReactor.notify(t, Event.wrap(t));
				}

				if (null != onComplete) {
					onComplete.accept(success);
				}
			}
		});
	}

	private void addReconnectListener(ChannelFuture future, final Reconnect reconnect) {
		future.addListener(new ChannelFutureListener() {
			@Override
			public void operationComplete(ChannelFuture future) throws Exception {
				if (log.isInfoEnabled()) {
					log.info("CLOSE: Channel " + NettyTcpConnection.this.channel + " closed.");
				}
				if (null == reconnectSupplier) {
					return;
				}

				if (null == reconnect) {
					// only set disconnected time first time through
					disconnectedAt = System.currentTimeMillis();
				}

				final Reconnect r = (null == reconnect ? reconnectSupplier.get() : reconnect);
				reconnecting.set(true);

				reconnect(r, new Consumer<SocketChannel>() {
					@Override
					public void accept(SocketChannel socketChannel) {
						addReconnectListener(socketChannel.closeFuture(), r);
					}
				});
			}
		});
	}

	private void reconnect(final Reconnect reconnect, final Consumer<SocketChannel> onSuccess) {
		InetSocketAddress connectAddress = reconnect.reconnectTo(remoteAddress);
		if (null == connectAddress) {
			close();
			reconnect.close();
			return;
		}
		addressSetter.accept(connectAddress);

		TimerTask task = new TimerTask() {
			@Override
			public void run() {
				if (null == connectionSupplier) {
					return;
				}
				connectionSupplier.get().addListener(new ChannelFutureListener() {
					@Override
					public void operationComplete(ChannelFuture future) throws Exception {
						if (log.isInfoEnabled()) {
							log.info("RECONNECT: Reconnection attempt successful? " + future.isSuccess());
						}
						if (future.isSuccess()) {
							NettyTcpConnection.this.channel = (SocketChannel) future.channel();
							NettyTcpConnection.this.remoteAddress = NettyTcpConnection.this.channel.remoteAddress();
							onSuccess.accept(NettyTcpConnection.this.channel);
							reconnect.close();
							reconnecting.set(false);
							disconnectedAt = -1;
						} else {
							reconnect(reconnect, onSuccess);
						}
					}
				});
			}
		};

		long delay = reconnect.reconnectAfter(System.currentTimeMillis() - disconnectedAt);
		if (log.isInfoEnabled()) {
			log.info("Attempting reconnect to " + connectAddress + (delay > 0 ? " after " + delay + "ms" : ""));
		}
		if (delay > -1) {
			env.getRootTimer().schedule(task, delay);
		} else {
			task.run();
		}
	}

	@Override
	public String toString() {
		return "NettyTcpConnection{" +
				"channel=" + channel +
				", remoteAddress=" + remoteAddress +
				'}';
	}

}
