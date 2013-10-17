package reactor.queue.encoding;

import reactor.function.Function;
import reactor.io.Buffer;

/**
 * @author Jon Brisbin
 */
public interface Codec<T> {

	Function<Buffer, T> decoder();

	Function<T, Buffer> encoder();

}
