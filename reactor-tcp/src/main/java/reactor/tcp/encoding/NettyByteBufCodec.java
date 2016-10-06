package reactor.tcp.encoding;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;
import reactor.function.Consumer;
import reactor.function.Function;
import reactor.io.Buffer;

/**
 * A {@link Codec} implementation that turns a {@link Buffer} into Netty {@link ByteBuf} and
 * vice-versa.
 */
public class NettyByteBufCodec implements Codec<Buffer, ByteBuf, ByteBuf> {

  @Override
  public Function<Buffer, ByteBuf> decoder(final Consumer<ByteBuf> next) {
    return new Function<Buffer, ByteBuf>() {
      @Override
      public ByteBuf apply(Buffer buffer) {
        if (null != next) {
          next.accept(Unpooled.wrappedBuffer(buffer.asBytes()));
          return null;
        } else {
          return Unpooled.wrappedBuffer(buffer.asBytes());
        }
      }
    };
  }

  @Override
  public Function<ByteBuf, Buffer> encoder() {
    return new Function<ByteBuf, Buffer>() {
      @Override
      public Buffer apply(ByteBuf byteBuf) {
        return new Buffer(byteBuf.nioBuffer());
      }
    };
  }

}
