package reactor.tcp.codec;

import reactor.fn.Supplier;

/**
 * A {@link Supplier} that supplies a single, shared {@link LineFeedCodec}. {@code LineFeedCodec} is stateless,
 * so can safely be shared between connections.
 *
 * @author Andy Wilkinson
 *
 */
public final class LineFeedCodecSupplier implements Supplier<Codec<String>> {

	private final LineFeedCodec codec = new LineFeedCodec();

	@Override
	public LineFeedCodec get() {
		return codec;
	}

}
