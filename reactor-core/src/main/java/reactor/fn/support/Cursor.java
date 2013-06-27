package reactor.fn.support;

import java.util.concurrent.atomic.AtomicReference;

/**
 * @author Stephane Maldini
 */
public class Cursor<T> {

	private final AtomicReference<T> reference = new AtomicReference<T>();
	private       long               timestamp = -1l;

	public T get() {
		return reference.get();
	}

	public void set(T v) {
		reference.set(v);
		timestamp = System.nanoTime();
	}

	public long lastUpdated() {
		return timestamp;
	}

	@Override
	public String toString() {
		return "Cursor{" +
				"reference=" + reference.get() +
				", timestamp=" + timestamp +
				'}';
	}
}
