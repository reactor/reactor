package reactor.fn.dispatch;

import reactor.fn.support.ConverterAwareConsumerInvoker;

import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * @author Jon Brisbin
 */
public abstract class AbstractDispatcher implements Dispatcher {

	private final AtomicInteger   queuedTasks = new AtomicInteger();
	private final AtomicBoolean   alive       = new AtomicBoolean(true);
	private final ConsumerInvoker invoker     = new ConverterAwareConsumerInvoker();

	@Override
	public boolean alive() {
		return alive.get();
	}

	@Override
	public boolean shutdown() {
		alive.compareAndSet(true, false);
		return queuedTasks.get() > 0;
	}

	@Override
	public boolean halt() {
		alive.compareAndSet(true, false);
		return queuedTasks.get() > 0;
	}

	@Override
	public <T> Task<T> nextTask() {
		if (!alive()) {
			throw new IllegalStateException("This Dispatcher has been shutdown and cannot accept new tasks.");
		}
		incrementTaskCount();
		return createTask();
	}

	protected ConsumerInvoker getInvoker() {
		return invoker;
	}

	protected void incrementTaskCount() {
		queuedTasks.incrementAndGet();
	}

	protected void decrementTaskCount() {
		queuedTasks.decrementAndGet();
	}

	protected abstract <T> Task<T> createTask();

}
