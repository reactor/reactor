package reactor.event.lifecycle;

import java.util.concurrent.TimeUnit;

/**
 * @author Jon Brisbin
 */
public interface Lifecycle {

	boolean alive();

	void start();

	boolean awaitAndShutdown();

	boolean awaitAndShutdown(long timeout, TimeUnit timeUnit);

	void shutdown();

	void halt();

}
