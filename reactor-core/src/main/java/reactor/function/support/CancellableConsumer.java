package reactor.function.support;

import javax.annotation.Nonnull;

import reactor.function.Consumer;
import reactor.function.Predicate;
import reactor.util.Assert;

/**
 * @author Jon Brisbin
 */
public abstract class CancellableConsumer<T> implements Consumer<T> {

	private final Predicate<T> predicate;

	public CancellableConsumer(@Nonnull Predicate<T> predicate) {
		Assert.notNull(predicate, "Predicate cannot be null.");
		this.predicate = predicate;
	}

	@Override public void accept(T obj) {
		if(!predicate.test(obj)) {
			throw new CancelConsumerException(obj + " failed Predicate test " + predicate);
		} else {
			doAccept(obj);
		}
	}

	protected abstract void doAccept(T obj);

}
