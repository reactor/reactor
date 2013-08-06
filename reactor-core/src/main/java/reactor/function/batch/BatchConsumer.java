package reactor.function.batch;

import reactor.function.Consumer;

/**
 * @author Jon Brisbin
 */
public interface BatchConsumer<T> extends Consumer<T> {

	void start();

	void end();

}
