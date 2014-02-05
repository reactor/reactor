package reactor.io.encoding.compress;

import reactor.function.Consumer;
import reactor.function.Function;
import reactor.io.Buffer;
import reactor.io.encoding.Codec;

import java.io.*;

/**
 * @author Jon Brisbin
 */
public abstract class CompressionCodec<IN, OUT> implements Codec<Buffer, IN, OUT> {

	private final Codec<Buffer, IN, OUT> delegate;

	protected CompressionCodec(Codec<Buffer, IN, OUT> delegate) {
		this.delegate = delegate;
	}

	@Override
	public Function<Buffer, IN> decoder(final Consumer<IN> next) {
		return new Function<Buffer, IN>() {
			@Override
			public IN apply(Buffer buffer) {
				try {
					ByteArrayInputStream bin = new ByteArrayInputStream(buffer.asBytes());
					InputStream zin = createInputStream(bin);
					Buffer newBuff = new Buffer();
					while(zin.available() > 0) {
						newBuff.append((byte)zin.read());
					}
					zin.close();
					IN in = delegate.decoder(null).apply(newBuff.flip());
					if(null != next) {
						next.accept(in);
						return null;
					} else {
						return in;
					}
				} catch(IOException e) {
					throw new IllegalStateException(e.getMessage(), e);
				}
			}
		};
	}

	@Override
	public Function<OUT, Buffer> encoder() {
		return new Function<OUT, Buffer>() {
			@Override
			public Buffer apply(OUT out) {
				Buffer buff = delegate.encoder().apply(out);
				try {
					ByteArrayOutputStream bout = new ByteArrayOutputStream();
					OutputStream zout = createOutputStream(bout);
					zout.write(buff.asBytes());
					zout.flush();
					bout.flush();
					zout.close();
					return Buffer.wrap(bout.toByteArray());
				} catch(IOException e) {
					throw new IllegalStateException(e.getMessage(), e);
				}
			}
		};
	}

	protected abstract InputStream createInputStream(InputStream parent) throws IOException;

	protected abstract OutputStream createOutputStream(OutputStream parent) throws IOException;

}

