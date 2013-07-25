package reactor.queue.spec;

import reactor.function.Supplier;
import reactor.queue.InMemoryQueuePersistor;
import reactor.queue.PersistentQueue;
import reactor.util.Assert;

/**
 * @author Jon Brisbin
 */
public class PersistentQueueSpec<T> implements Supplier<PersistentQueue<T>> {

	private PersistentQueue<T> queue;

	public PersistentQueueSpec<T> inMemory() {
		Assert.isNull(queue, "PersistentQueue type already set (" + queue.getClass().getName() + ")");
		this.queue = new PersistentQueue<T>(new InMemoryQueuePersistor<T>());
		return this;
	}

	@Override
	public PersistentQueue<T> get() {
		return null;
	}

}
