package reactor.fn.support;

import reactor.fn.Consumer;

/**
 * A {@code CascadingConsumer} simply cascades the value received by this {@link Consumer} into a delegate {@link
 * Consumer}.
 *
 * @author Jon Brisbin
 */
public class CascadingConsumer<T> implements Consumer<T> {

	private final Consumer<T> delegate;

	public CascadingConsumer(Consumer<T> delegate) {
		this.delegate = delegate;
	}

	@Override
	public void accept(T value) {
		delegate.accept(value);
	}

}
