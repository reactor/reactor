package reactor.tcp.encoding;

import reactor.fn.Function;

/**
 * @author Jon Brisbin
 */
public interface Codec<SRC, IN, OUT> {

	Function<SRC, IN> decoder();

	Function<OUT, SRC> encoder();

}
