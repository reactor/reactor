package reactor.fn;

/**
 * A {@link Consumer} implementation that allows the delegate {@link Consumer} to only be called once. Should be used in
 * combination with {@link reactor.fn.Registration#cancelAfterUse()} to ensure that this {@link Consumer Consumer's}
 * {@link Registration} is cancelled as soon after its use as possible.
 *
 * @author Jon Brisbin
 * @see {@link reactor.fn.Registration#cancelAfterUse()}
 */
public class SingleUseConsumer<T> implements Consumer<T> {

	private final Consumer<T> delegate;
	private final    Object  monitor = new Object();
	private volatile boolean called  = false;

	/**
	 * Create a single-use {@link Consumer} using the given delgate.
	 *
	 * @param delegate The {@link Consumer} to delegate accept calls to.
	 */
	public SingleUseConsumer(Consumer<T> delegate) {
		this.delegate = delegate;
	}

	@Override
	public void accept(T t) {
		synchronized (monitor) {
			if (called) {
				return;
			}
			called = true;
		}
		delegate.accept(t);
	}

}
