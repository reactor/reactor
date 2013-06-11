package reactor.tcp.encoding;

import reactor.fn.Consumer;
import reactor.fn.Function;

/**
 * Implementations of a {@literal Codec} are responsible for decoding a {@code SRC} into an instance of {@code IN} and
 * passing that to the given {@link reactor.fn.Consumer}. A codec also provides an encoder to take an instance of {@code
 * OUT} and encode to an instance of {@code SRC}.
 *
 * @author Jon Brisbin
 */
public interface Codec<SRC, IN, OUT> {

	/**
	 * Provide the caller with a decoder to turn a source object into an instance of the input type.
	 *
	 * @param next The {@link Consumer} to call after the object has been decoded.
	 * @return The decoded object.
	 */
	Function<SRC, IN> decoder(Consumer<IN> next);

	/**
	 * Provide the caller with an encoder to turn an output object into an instance of the source type.
	 *
	 * @return The encoded source object.
	 */
	Function<OUT, SRC> encoder();

}
