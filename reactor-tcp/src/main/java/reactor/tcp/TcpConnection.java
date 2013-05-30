package reactor.tcp;

import reactor.core.Composable;
import reactor.fn.Consumer;

/**
 * @author Jon Brisbin
 */
public interface TcpConnection<IN, OUT> {

	void close();

	boolean consumable();

	boolean writable();

	TcpConnection<IN, OUT> consume(Consumer<IN> consumer);

	TcpConnection<IN, OUT> send(OUT data, Consumer<Boolean> onComplete);

	TcpConnection<IN, OUT> send(Composable<OUT> data);

}
