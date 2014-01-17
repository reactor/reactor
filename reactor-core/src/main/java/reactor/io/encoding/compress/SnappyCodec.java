package reactor.io.encoding.compress;

import org.xerial.snappy.SnappyInputStream;
import org.xerial.snappy.SnappyOutputStream;
import reactor.io.Buffer;
import reactor.io.encoding.Codec;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;

/**
 * @author Jon Brisbin
 */
public class SnappyCodec<IN, OUT> extends CompressionCodec<IN, OUT> {

	public SnappyCodec(Codec<Buffer, IN, OUT> delegate) {
		super(delegate);
	}

	@Override
	protected InputStream createInputStream(InputStream parent) throws IOException {
		return new SnappyInputStream(parent);
	}

	@Override
	protected OutputStream createOutputStream(OutputStream parent) throws IOException {
		return new SnappyOutputStream(parent);
	}

}
