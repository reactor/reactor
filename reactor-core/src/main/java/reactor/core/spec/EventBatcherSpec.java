package reactor.core.spec;

import reactor.core.EventBatcher;
import reactor.core.Observable;
import reactor.event.Event;
import reactor.function.Predicate;
import reactor.function.Supplier;
import reactor.util.Assert;

import java.util.Queue;

/**
 * Helper to configure an {@link reactor.core.EventBatcher}.
 *
 * @author Jon Brisbin
 */
public class EventBatcherSpec<T> implements Supplier<EventBatcher<T>> {

	private Observable                 observable = null;
	private Object                     notifyKey  = null;
	private Queue<Event<T>>            eventQueue = null;
	private Predicate<Queue<Event<T>>> queueWhile = null;
	private Predicate<Queue<Event<T>>> flushWhen  = null;

	/**
	 * When flushing, notify the given {@link reactor.core.Observable}.
	 *
	 * @param observable
	 * 		the {@link reactor.core.Observable} to notify
	 *
	 * @return {@literal this}
	 */
	public EventBatcherSpec<T> observable(Observable observable) {
		this.observable = observable;
		return this;
	}

	/**
	 * When flushing, notify the {@link reactor.core.Observable} using the given {@code key}.
	 *
	 * @param notifyKey
	 * 		the key to notify with
	 *
	 * @return {@literal this}
	 */
	public EventBatcherSpec<T> notifyKey(Object notifyKey) {
		this.notifyKey = notifyKey;
		return this;
	}

	/**
	 * While queueing {@link Event Events}, use the given {@link java.util.Queue}.
	 *
	 * @param eventQueue
	 * 		the {@link java.util.Queue} to use to queue events
	 *
	 * @return {@literal this}
	 */
	public EventBatcherSpec<T> eventQueue(Queue<Event<T>> eventQueue) {
		Assert.isNull(this.eventQueue, "Event Queue is already set (" + this.eventQueue + ")");
		this.eventQueue = eventQueue;
		return this;
	}

	/**
	 * The {@link reactor.function.Predicate} which should return {@code true} to queue events.
	 *
	 * @param queueWhile
	 * 		the {@link reactor.function.Predicate} to denote whether to queue events or not
	 *
	 * @return {@literal this}
	 */
	public EventBatcherSpec<T> queueWhile(Predicate<Queue<Event<T>>> queueWhile) {
		this.queueWhile = queueWhile;
		return this;
	}

	/**
	 * The {@link reactor.function.Predicate} which should return {@code true} to flush queued events.
	 *
	 * @param flushWhen
	 * 		the {@link reactor.function.Predicate} to denote whether to flush events or not
	 *
	 * @return {@literal this}
	 */
	public EventBatcherSpec<T> flushWhen(Predicate<Queue<Event<T>>> flushWhen) {
		this.flushWhen = flushWhen;
		return this;
	}

	@Override
	public EventBatcher<T> get() {
		return new EventBatcher<T>(observable, notifyKey, eventQueue, queueWhile, flushWhen);
	}

}
