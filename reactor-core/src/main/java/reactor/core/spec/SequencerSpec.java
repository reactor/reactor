package reactor.core.spec;

import java.util.Queue;

import reactor.core.Sequencer;
import reactor.event.Event;
import reactor.core.Observable;
import reactor.function.Predicate;
import reactor.function.Supplier;
import reactor.util.Assert;

/**
 * @author Jon Brisbin
 */
public class SequencerSpec<T> implements Supplier<Sequencer<T>> {

	private Observable                 observable = null;
	private Object                     notifyKey  = null;
	private Queue<Event<T>>            eventQueue = null;
	private Predicate<Queue<Event<T>>> queueWhile = null;
	private Predicate<Queue<Event<T>>> flushWhen  = null;

	public SequencerSpec<T> observable(Observable observable) {
		this.observable = observable;
		return this;
	}

	public SequencerSpec<T> notifyKey(Object notifyKey) {
		this.notifyKey = notifyKey;
		return this;
	}

	public SequencerSpec<T> eventQueue(Queue<Event<T>> eventQueue) {
		Assert.isNull(this.eventQueue, "Event Queue is already set (" + this.eventQueue + ")");
		this.eventQueue = eventQueue;
		return this;
	}

	public SequencerSpec<T> queueWhile(Predicate<Queue<Event<T>>> queueWhile) {
		this.queueWhile = queueWhile;
		return this;
	}

	public SequencerSpec<T> flushWhen(Predicate<Queue<Event<T>>> flushWhen) {
		this.flushWhen = flushWhen;
		return this;
	}

	@Override public Sequencer<T> get() {
		return new Sequencer<T>(observable, notifyKey, eventQueue, queueWhile, flushWhen);
	}

}
