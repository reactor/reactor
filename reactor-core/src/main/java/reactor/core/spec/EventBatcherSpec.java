package reactor.core.spec;

import java.util.Queue;

import reactor.core.EventBatcher;
import reactor.event.Event;
import reactor.core.Observable;
import reactor.function.Predicate;
import reactor.function.Supplier;
import reactor.util.Assert;

/**
 * @author Jon Brisbin
 */
public class EventBatcherSpec<T> implements Supplier<EventBatcher<T>> {

	private Observable                 observable = null;
	private Object                     notifyKey  = null;
	private Queue<Event<T>>            eventQueue = null;
	private Predicate<Queue<Event<T>>> queueWhile = null;
	private Predicate<Queue<Event<T>>> flushWhen  = null;

	public EventBatcherSpec<T> observable(Observable observable) {
		this.observable = observable;
		return this;
	}

	public EventBatcherSpec<T> notifyKey(Object notifyKey) {
		this.notifyKey = notifyKey;
		return this;
	}

	public EventBatcherSpec<T> eventQueue(Queue<Event<T>> eventQueue) {
		Assert.isNull(this.eventQueue, "Event Queue is already set (" + this.eventQueue + ")");
		this.eventQueue = eventQueue;
		return this;
	}

	public EventBatcherSpec<T> queueWhile(Predicate<Queue<Event<T>>> queueWhile) {
		this.queueWhile = queueWhile;
		return this;
	}

	public EventBatcherSpec<T> flushWhen(Predicate<Queue<Event<T>>> flushWhen) {
		this.flushWhen = flushWhen;
		return this;
	}

	@Override public EventBatcher<T> get() {
		return new EventBatcher<T>(observable, notifyKey, eventQueue, queueWhile, flushWhen);
	}

}
