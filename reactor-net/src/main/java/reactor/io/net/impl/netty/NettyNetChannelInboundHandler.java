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

package reactor.io.net.impl.netty;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.EmptyByteBuf;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.ChannelInboundHandlerAdapter;
import org.reactivestreams.Subscriber;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import reactor.io.buffer.Buffer;
import reactor.io.net.Spec;
import reactor.rx.subscription.PushSubscription;

/**
 * Netty {@link io.netty.channel.ChannelInboundHandler} implementation that passes data to a Reactor {@link
 * reactor.io.net.ChannelStream}.
 *
 * @author Jon Brisbin
 * @author Stephane Maldini
 */
public class NettyNetChannelInboundHandler<IN> extends ChannelInboundHandlerAdapter {

	private final Logger log = LoggerFactory.getLogger(getClass());

	private final Subscriber<? super IN>    subscriber;
	protected final NettyChannelStream<IN, ?> channelStream;

	private volatile ByteBuf              remainder;
	private volatile PushSubscription<IN> channelSubscription;

	public NettyNetChannelInboundHandler(
			Subscriber<? super IN> subscriber, NettyChannelStream<IN, ?> channelStream
	) {
		this.subscriber = subscriber;
		this.channelStream = channelStream;
	}

	public PushSubscription<IN> subscription() {
		return channelSubscription;
	}

	@Override
	public void channelActive(final ChannelHandlerContext ctx) throws Exception {
		try {
			if (this.channelSubscription != null) {
				super.channelActive(ctx);
				if (log.isDebugEnabled()) {
					log.debug("RESUME: " + ctx.channel());
				}
				return;
			}

			this.channelSubscription = new PushSubscription<IN>(null, subscriber) {
				@Override
				protected void onRequest(long n) {
					if (n == Long.MAX_VALUE) {
						ctx.channel().config().setAutoRead(true);
					}
					ctx.read();
				}

				@Override
				public void cancel() {
					super.cancel();
					if (ctx.channel().isOpen()) {
						ctx.close();
					}
				}
			};
			subscriber.onSubscribe(channelSubscription);
			channelStream.registerOnPeer();
			super.channelActive(ctx);
		} catch (Throwable err) {
			subscriber.onError(err);
		}
	}

	@Override
	public void channelReadComplete(ChannelHandlerContext ctx) throws Exception {
		if (channelSubscription.isComplete()) {
			return;
		}

		try {
			super.channelReadComplete(ctx);
			if (channelSubscription.pendingRequestSignals() != Long.MAX_VALUE
					&& channelSubscription.pendingRequestSignals() > 1l) {
				ctx.read();
			}
		} catch (Throwable throwable) {
			channelSubscription.onError(throwable);
		}
	}

	@Override
	public void channelInactive(ChannelHandlerContext ctx) throws Exception {
		try {
			channelSubscription.onComplete();
			super.channelInactive(ctx);
		} catch (Throwable err) {
			channelSubscription.onError(err);
		}
	}

	@Override
	@SuppressWarnings("unchecked")
	public void channelRead(ChannelHandlerContext ctx, Object msg) throws Exception {
		try {

			if (channelSubscription.isComplete() || msg.getClass() == EmptyByteBuf.class) {
				return;
			}

			if (channelStream.getDecoder() == Spec.NOOP_DECODER || !ByteBuf.class.isAssignableFrom(msg.getClass())) {
				channelSubscription.onNext((IN) msg);
				return;
			}else if(channelStream.getDecoder() == null){
				try {
					channelSubscription.onNext((IN) new Buffer(((ByteBuf) msg).nioBuffer()));
				} finally {
					((ByteBuf)msg).release();
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
		} catch (Throwable t) {
			channelSubscription.onError(t);
		}

	}

	@Override
	public void exceptionCaught(ChannelHandlerContext ctx, Throwable cause) throws Exception {
		if ("Broken pipe".equals(cause.getMessage()) || "Connection reset by peer".equals(cause.getMessage())) {
			if (log.isDebugEnabled()) {
				log.debug(ctx.channel().toString() + " " + cause.getMessage());
			}
		}
		channelSubscription.onError(cause);
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
			IN read = channelStream.getDecoder().apply(b);
			if (read != null) {
				channelSubscription.onNext(read);
			}
		}

		//data.remaining() > 0;
		data.skipBytes(b.position() - start);
	}

}
