package reactor.event.registry;

import reactor.event.lifecycle.Pausable;
import reactor.event.selector.ObjectSelector;
import reactor.event.selector.Selector;

/**
 * @author Jon Brisbin
 */
public class CachableRegistration<T> implements Registration<T> {

	private static final Selector NO_MATCH = new ObjectSelector<Void>(null) {
		@Override
		public boolean matches(Object key) {
			return false;
		}
	};

	private final Selector selector;
	private final T        object;
	private final Runnable onCancel;
	private final boolean  lifecycle;

	private volatile boolean cancelled      = false;
	private volatile boolean cancelAfterUse = false;
	private volatile boolean paused         = false;

	public CachableRegistration(Selector selector, T object, Runnable onCancel) {
		this.selector = selector;
		this.object = object;
		this.onCancel = onCancel;
		this.lifecycle = Pausable.class.isAssignableFrom(object.getClass());
	}

	@Override
	public Selector getSelector() {
		return (!cancelled ? selector : NO_MATCH);
	}

	@Override
	public T getObject() {
		return (!cancelled && !paused ? object : null);
	}

	@Override
	public Registration<T> cancelAfterUse() {
		this.cancelAfterUse = true;
		return this;
	}

	@Override
	public boolean isCancelAfterUse() {
		return cancelAfterUse;
	}

	@Override
	public Registration<T> cancel() {
		if (!cancelled) {
			if (null != onCancel) {
				onCancel.run();
			}
			if (lifecycle) {
				((Pausable) object).cancel();
			}
			this.cancelled = true;
		}
		return this;
	}

	@Override
	public boolean isCancelled() {
		return cancelled;
	}

	@Override
	public Registration<T> pause() {
		this.paused = true;
		if (lifecycle) {
			((Pausable) object).pause();
		}
		return this;
	}

	@Override
	public boolean isPaused() {
		return paused;
	}

	@Override
	public Registration<T> resume() {
		paused = false;
		if (lifecycle) {
			((Pausable) object).resume();
		}
		return this;
	}

	@Override
	public String toString() {
		return "CachableRegistration{" +
				"\n\tselector=" + selector +
				",\n\tobject=" + object +
				",\n\tonCancel=" + onCancel +
				",\n\tlifecycle=" + lifecycle +
				",\n\tcancelled=" + cancelled +
				",\n\tcancelAfterUse=" + cancelAfterUse +
				",\n\tpaused=" + paused +
				"\n}";
	}

}
