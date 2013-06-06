package reactor.tcp.encoding;

import reactor.fn.Function;
import reactor.fn.Observable;

/**
 * @author Jon Brisbin
 */
public interface Codec<SRC, IN, OUT> {

	Function<SRC, IN> decoder(Object notifyKey, Observable observable);

	Function<OUT, SRC> encoder();

}
