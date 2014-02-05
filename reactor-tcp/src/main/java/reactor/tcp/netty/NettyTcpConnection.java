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
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.socket.SocketChannel;
import io.netty.handler.timeout.IdleState;
import io.netty.handler.timeout.IdleStateEvent;
import io.netty.handler.timeout.IdleStateHandler;
import reactor.core.Environment;
import reactor.core.Reactor;
import reactor.core.composable.Deferred;
import reactor.core.composable.Promise;
import reactor.event.Event;
import reactor.event.dispatch.Dispatcher;
import reactor.io.Buffer;
import reactor.tcp.AbstractTcpConnection;
import reactor.io.encoding.Codec;

import java.net.InetSocketAddress;
import java.nio.ByteBuffer;
import java.util.concurrent.TimeUnit;

/**
 * A {@link reactor.tcp.TcpConnection} implementation that uses Netty.
 *
 * @author Jon Brisbin
 * @author Stephane Maldini
 */
public class NettyTcpConnection<IN, OUT> extends AbstractTcpConnection<IN, OUT> {

  private final    Codec<Buffer, IN, OUT> codec;
  private volatile SocketChannel          channel;
  private volatile InetSocketAddress      remoteAddress;
  private volatile boolean closing = false;

  NettyTcpConnection(final Environment env,
                     Codec<Buffer, IN, OUT> codec,
                     Dispatcher ioDispatcher,
                     Reactor eventsReactor,
                     SocketChannel channel) {
    this(env, codec, ioDispatcher, eventsReactor, channel, null);
  }

  NettyTcpConnection(final Environment env,
                     Codec<Buffer, IN, OUT> codec,
                     Dispatcher ioDispatcher,
                     Reactor eventsReactor,
                     SocketChannel channel,
                     InetSocketAddress remoteAddress) {
    super(env, codec, ioDispatcher, eventsReactor);
    this.codec = codec;
    this.channel = channel;
    this.remoteAddress = remoteAddress;
  }

  /**
   * Return the {@link SocketChannel} in use by this connection.
   *
   * @return the {@link SocketChannel} in use
   */
  public SocketChannel channel() {
    return channel;
  }

  public Codec<Buffer, IN, OUT> codec() {
    return codec;
  }

  @Override
  public ConsumerSpec on() {
    return new NettyTcpConnectionConsumerSpec();
  }

  @Override
  public void close() {
    super.close();
    closing = true;
    try {
      channel.close().await();
    } catch(InterruptedException e) {
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

  boolean isClosing() {
    return closing;
  }

  void notifyRead(Object obj) {
    eventsReactor.notify(read.getObject(), (Event.class.isInstance(obj) ? (Event)obj : Event.wrap(obj)));
  }

  void notifyError(Throwable throwable) {
    eventsReactor.notify(throwable.getClass(), Event.wrap(throwable));
  }

  @Override
  protected void write(Buffer data, final Deferred<Void, Promise<Void>> onComplete, boolean flush) {
    write(data.byteBuffer(), onComplete, flush);
  }

  protected void write(ByteBuffer data, final Deferred<Void, Promise<Void>> onComplete, boolean flush) {
    ByteBuf buf = channel.alloc().buffer(data.remaining());
    buf.writeBytes(data);
    write(buf, onComplete, flush);
  }

  @Override
  protected void write(Object data, final Deferred<Void, Promise<Void>> onComplete, final boolean flush) {
    ChannelFuture writeFuture = (flush ? channel.writeAndFlush(data) : channel.write(data));
    writeFuture.addListener(new ChannelFutureListener() {
      @Override
      public void operationComplete(ChannelFuture future) throws Exception {
        boolean success = future.isSuccess();

        if(!success) {
          Throwable t = future.cause();
          eventsReactor.notify(t.getClass(), Event.wrap(t));
          if(null != onComplete) {
            onComplete.accept(t);
          }
        } else if(null != onComplete) {
          onComplete.accept((Void)null);
        }
      }
    });
  }

  @Override
  protected void flush() {
    channel.flush();
  }

  @Override
  public String toString() {
    return "NettyTcpConnection{" +
        "channel=" + channel +
        ", remoteAddress=" + remoteAddress +
        '}';
  }

  private class NettyTcpConnectionConsumerSpec<IN, OUT> implements ConsumerSpec<IN, OUT> {
    @Override
    public ConsumerSpec close(final Runnable onClose) {
      channel.closeFuture().addListener(new ChannelFutureListener() {
        @SuppressWarnings("unchecked")
        @Override
        public void operationComplete(ChannelFuture future) throws Exception {
          onClose.run();
        }
      });
      return this;
    }

    @Override
    public ConsumerSpec readIdle(long idleTimeout, final Runnable onReadIdle) {
      channel.pipeline().addFirst(new IdleStateHandler(idleTimeout, 0, 0, TimeUnit.MILLISECONDS) {
        @Override
        protected void channelIdle(ChannelHandlerContext ctx, IdleStateEvent evt) throws Exception {
          if(evt.state() == IdleState.READER_IDLE) {
            onReadIdle.run();
          }
          super.channelIdle(ctx, evt);
        }
      });
      return this;
    }

    @Override
    public ConsumerSpec writeIdle(long idleTimeout, final Runnable onWriteIdle) {
      channel.pipeline().addLast(new IdleStateHandler(0, idleTimeout, 0, TimeUnit.MILLISECONDS) {
        @Override
        protected void channelIdle(ChannelHandlerContext ctx, IdleStateEvent evt) throws Exception {
          if(evt.state() == IdleState.WRITER_IDLE) {
            onWriteIdle.run();
          }
          super.channelIdle(ctx, evt);
        }
      });
      return this;
    }
  }

}
