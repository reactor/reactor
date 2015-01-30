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
 *  WITHIN WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  See the License for the specific language governing permissions and
 *  limitations under the License.
 */

package reactor.io.net.netty;

import io.netty.buffer.ByteBuf;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.ChannelInboundHandlerAdapter;
import org.reactivestreams.Subscriber;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import reactor.io.buffer.Buffer;
import reactor.rx.action.Action;
import reactor.rx.subscription.PushSubscription;

import java.util.concurrent.atomic.AtomicIntegerFieldUpdater;
import java.util.concurrent.atomic.AtomicLongFieldUpdater;

/**
 * Netty {@link io.netty.channel.ChannelInboundHandler} implementation that passes data to a Reactor {@link
 * reactor.io.net.ChannelStream}.
 *
 * @author Jon Brisbin
 * @author Stephane Maldini
 * @author Andy Wilkinson
 */
public class NettyNetChannelInboundHandler<IN> extends ChannelInboundHandlerAdapter {

	private final Logger log = LoggerFactory.getLogger(getClass());

	private final    Subscriber<? super IN>    subscriber;
	private final    NettyChannelStream<IN, ?> channelStream;
	private volatile ByteBuf                   remainder;
	private volatile long pendingRequests = 0l;
	private volatile int  terminated      = 0;

	private final AtomicLongFieldUpdater<NettyNetChannelInboundHandler> READ_UPDATER =
			AtomicLongFieldUpdater.newUpdater(NettyNetChannelInboundHandler.class, "pendingRequests");

	private final AtomicIntegerFieldUpdater<NettyNetChannelInboundHandler> TERMINATED =
			AtomicIntegerFieldUpdater.newUpdater(NettyNetChannelInboundHandler.class, "terminated");

	public NettyNetChannelInboundHandler(
			Subscriber<? super IN> subscriber, NettyChannelStream<IN, ?> channelStream
	) {
		this.subscriber = subscriber;
		this.channelStream = channelStream;
	}

	public Subscriber<? super IN> subscriber() {
		return subscriber;
	}
	public NettyChannelStream<IN, ?> channelStream() {
		return channelStream;
	}

	@Override
	public void channelActive(final ChannelHandlerContext ctx) throws Exception {
		try {
			if (log.isDebugEnabled()) {
				log.debug("OPEN: " + ctx.channel());
			}
			subscriber.onSubscribe(new PushSubscription<IN>(null, subscriber) {
				@Override
				public void request(long n) {
					Action.checkRequest(n);

					if (NettyNetChannelInboundHandler.this.terminated == 1) return;

					if (READ_UPDATER.addAndGet(NettyNetChannelInboundHandler.this, n) < 0l) {
						READ_UPDATER.set(NettyNetChannelInboundHandler.this, Long.MAX_VALUE);
					}
					ctx.read();
				}

				@Override
				public void cancel() {
					if (TERMINATED.compareAndSet(NettyNetChannelInboundHandler.this, 0, 1)) {
						super.cancel();
						ctx.close();
					}
				}
			});
			super.channelActive(ctx);
		} catch (Throwable err) {
			subscriber.onError(err);
		}
	}

	@Override
	public void channelReadComplete(ChannelHandlerContext ctx) throws Exception {
		if (terminated == 1) {
			return;
		}

		try {
			if (pendingRequests == Long.MAX_VALUE || READ_UPDATER.decrementAndGet(this) > 0l) {
				super.channelReadComplete(ctx);
			}
		} catch (Throwable throwable) {
			subscriber.onError(throwable);
		}
	}

	@Override
	public void channelInactive(ChannelHandlerContext ctx) throws Exception {
		try {
			if (TERMINATED.compareAndSet(this, 0, 1)) {
				subscriber.onComplete();
			}
			if (log.isDebugEnabled()) {
				log.debug("CLOSE: " + ctx.channel());
			}
			super.channelInactive(ctx);
		} catch (Throwable err) {
			subscriber.onError(err);
		}
	}

	@Override
	@SuppressWarnings("unchecked")
	public void channelRead(ChannelHandlerContext ctx, Object msg) throws Exception {
		if (!ByteBuf.class.isInstance(msg) || null == channelStream.getDecoder()) {
			try {
				subscriber.onNext((IN) msg);
			} catch (Throwable t) {
				subscriber.onError(t);
			}
			return;
		}

		ByteBuf data = (ByteBuf) msg;
		if (remainder == null) {
			try {
				passToConnection(data);
			} finally {
				if (data.isReadable()) {
					remainder = data;
				} else {
					data.release();
				}
			}
			return;
		}

		if (!bufferHasSufficientCapacity(remainder, data)) {
			ByteBuf combined = createCombinedBuffer(remainder, data, ctx);
			remainder.release();
			remainder = combined;
		} else {
			remainder.writeBytes(data);
		}
		data.release();

		try {
			passToConnection(remainder);
		} finally {
			if (remainder.isReadable()) {
				remainder.discardSomeReadBytes();
			} else {
				remainder.release();
				remainder = null;
			}
		}
	}

	@Override
	public void exceptionCaught(ChannelHandlerContext ctx, Throwable cause) throws Exception {
		if ("Broken pipe".equals(cause.getMessage()) || "Connection reset by peer".equals(cause.getMessage())) {
			if (log.isDebugEnabled()) {
				log.debug(ctx.channel().toString() + " " + cause.getMessage());
			}
		}
		subscriber.onError(cause);
		ctx.close();
	}

	private boolean bufferHasSufficientCapacity(ByteBuf receiver, ByteBuf provider) {
		return receiver.writerIndex() <= receiver.maxCapacity() - provider.readableBytes();
	}

	private ByteBuf createCombinedBuffer(ByteBuf partOne, ByteBuf partTwo, ChannelHandlerContext ctx) {
		ByteBuf combined = ctx.alloc().buffer(partOne.readableBytes() + partTwo.readableBytes());
		combined.writeBytes(partOne);
		combined.writeBytes(partTwo);
		return combined;
	}

	private void passToConnection(ByteBuf data) {
		Buffer b = new Buffer(data.nioBuffer());
		int start = b.position();
		if (null != channelStream.getDecoder() && null != b.byteBuffer()) {
			channelStream.getDecoder().apply(b);
		}

		//data.remaining() > 0;
		data.skipBytes(b.position() - start);
	}

}
