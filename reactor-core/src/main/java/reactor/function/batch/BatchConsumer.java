package reactor.function.batch;

import reactor.function.Consumer;

/**
 * {@link reactor.function.Consumer} that is notified of the start and end of a batch.
 *
 * @author Jon Brisbin
 */
public interface BatchConsumer<T> extends Consumer<T> {

	/**
	 * Called when a batch begins.
	 */
	void start();

	/**
	 * Called when a batch ends.
	 */
	void end();

}
