package reactor.core.configuration;

import java.util.concurrent.ThreadPoolExecutor;

import reactor.fn.dispatch.Dispatcher;

import com.lmax.disruptor.RingBuffer;

/**
 * An enumeration of supported types of {@link Dispatcher}.
 *
 * @author Andy Wilkinson
 *
 */
public enum DispatcherType {

	/**
	 * A {@link Dispatcher} which uses an event loop for dispatching
	 */
	EVENT_LOOP,

	/**
	 * A {@link Dispatcher} which uses a {@link RingBuffer} for dispatching
	 */
	RING_BUFFER,

	/**
	 * A {@link Dispatcher} which uses the current thread for dispatching
	 */
	SYNCHRONOUS,

	/**
	 * A {@link Dispatcher} which uses a {@link ThreadPoolExecutor} for dispatching
	 */
	THREAD_POOL_EXECUTOR

}
