package reactor.io.encoding.compress;

import reactor.io.Buffer;
import reactor.io.encoding.Codec;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.util.zip.GZIPInputStream;
import java.util.zip.GZIPOutputStream;

/**
 * @author Jon Brisbin
 */
public class GzipCodec<IN, OUT> extends CompressionCodec<IN, OUT> {

	public GzipCodec(Codec<Buffer, IN, OUT> delegate) {
		super(delegate);
	}

	@Override
	protected InputStream createInputStream(InputStream parent) throws IOException {
		return new GZIPInputStream(parent);
	}

	@Override
	protected OutputStream createOutputStream(OutputStream parent) throws IOException {
		return new GZIPOutputStream(parent);
	}

}
