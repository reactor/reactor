package reactor.queue.spec;

import java.io.IOException;

import reactor.function.Supplier;
import reactor.queue.InMemoryQueuePersistor;
import reactor.queue.IndexedChronicleQueuePersistor;
import reactor.queue.PersistentQueue;
import reactor.util.Assert;

/**
 * Helper spec to create a {@link PersistentQueue} instance.
 *
 * @author Jon Brisbin
 */
public class PersistentQueueSpec<T> implements Supplier<PersistentQueue<T>> {

	private PersistentQueue<T> queue;

	public PersistentQueueSpec<T> inMemory() {
		Assert.isNull(queue, "PersistentQueue type already set (" + queue.getClass().getName() + ")");
		this.queue = new PersistentQueue<T>(new InMemoryQueuePersistor<T>());
		return this;
	}

	public PersistentQueueSpec<T> indexedChronicle(String basePath) {
		Assert.isNull(queue, "PersistentQueue type already set (" + queue.getClass().getName() + ")");
		try {
			this.queue = new PersistentQueue<T>(new IndexedChronicleQueuePersistor<T>(basePath));
		} catch(IOException e) {
			throw new IllegalArgumentException(e.getMessage(), e);
		}
		return this;
	}

	@Override
	public PersistentQueue<T> get() {
		return queue;
	}

}
