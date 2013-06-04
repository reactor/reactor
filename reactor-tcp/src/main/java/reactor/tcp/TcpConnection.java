package reactor.tcp;

import reactor.core.Stream;
import reactor.fn.Consumer;
import reactor.fn.Function;

import java.net.InetSocketAddress;

/**
 * @author Jon Brisbin
 */
public interface TcpConnection<IN, OUT> {

	void close();

	boolean consumable();

	boolean writable();

	InetSocketAddress remoteAddress();

	Stream<IN> in();

	Consumer<OUT> out();

	TcpConnection<IN, OUT> consume(Consumer<IN> consumer);

	Stream<OUT> receive(Function<IN, OUT> fn);

	TcpConnection<IN, OUT> send(Stream<OUT> data);

	TcpConnection<IN, OUT> send(OUT data);

	TcpConnection<IN, OUT> send(OUT data, Consumer<Boolean> onComplete);

}
