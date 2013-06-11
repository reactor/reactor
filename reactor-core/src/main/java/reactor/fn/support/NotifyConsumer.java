package reactor.fn.support;

import reactor.fn.Consumer;
import reactor.fn.Event;
import reactor.fn.Observable;
import reactor.util.Assert;

/**
 * @author Jon Brisbin
 */
public class NotifyConsumer<T> implements Consumer<T> {

	private final Object     notifyKey;
	private final Observable observable;

	public NotifyConsumer(Object notifyKey, Observable observable) {
		Assert.notNull(observable, "Observable cannot be null.");
		this.notifyKey = notifyKey;
		this.observable = observable;
	}

	@Override
	public void accept(T t) {
		Event<T> ev = Event.wrap(t);
		if (null == notifyKey) {
			observable.notify(ev);
		} else {
			observable.notify(notifyKey, ev);
		}
	}

}
