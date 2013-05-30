package reactor.tcp.encoding;

/**
 * @author Jon Brisbin
 */
public abstract class StandardCodecs {

	private StandardCodecs() {
	}

	public static final StringCodec                    STRING_CODEC    = new StringCodec();
	public static final DelimitedCodec<String, String> LINE_FEED_CODEC = new DelimitedCodec<String, String>(STRING_CODEC);

}
