package reactor.fn.support;

import reactor.fn.Consumer;

/**
 * A {@code CascadingConsumer} simply cascades the value received by this {@link Consumer} into a delegate {@link
 * Consumer}.
 *
 * @param <T> the type of the values that the consumer can accept
 *
 * @author Jon Brisbin
 */
public class CascadingConsumer<T> implements Consumer<T> {

	private final Consumer<T> delegate;

	/**
	 * Creates a new {@code CascadingConsumer} that will cascade to the given {@code delegate}.
	 * @param delegate The delegate
	 */
	public CascadingConsumer(Consumer<T> delegate) {
		this.delegate = delegate;
	}

	@Override
	public void accept(T value) {
		delegate.accept(value);
	}

}
