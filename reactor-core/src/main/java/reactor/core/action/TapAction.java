package reactor.core.action;

import reactor.core.Observable;
import reactor.event.Event;

import java.util.concurrent.atomic.AtomicReference;

/**
 * @author Jon Brisbin
 */
public class TapAction<T> extends Action<T> {

	private final AtomicReference<Event<T>> ref = new AtomicReference<Event<T>>();

	public TapAction(Observable observable, Object successKey, Object failureKey) {
		super(observable, successKey, failureKey);
	}

	public TapAction(Observable observable, Object successKey) {
		super(observable, successKey);
	}

	@Override
	protected void doAccept(Event<T> ev) {
		ref.set(ev);
	}

	public Event<T> getValue() {
		return ref.get();
	}

}
