/*
 * Copyright (c) 2011-2013 the original author or authors.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *       http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package reactor.core;

import com.eaio.uuid.UUID;
import org.cliffc.high_scale_lib.NonBlockingHashMap;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import reactor.Fn;
import reactor.fn.Consumer;
import reactor.fn.Event;
import reactor.fn.Observable;
import reactor.fn.Selector;
import reactor.fn.dispatch.Dispatcher;
import reactor.fn.dispatch.SynchronousDispatcher;
import reactor.support.Assert;

import java.util.Map;
import java.util.Timer;
import java.util.TimerTask;
import java.util.concurrent.TimeUnit;

import static reactor.Fn.$;
import static reactor.Fn.T;

/**
 * Helper class to encapsulate commonly-used functionality around Reactors.
 *
 * @author Jon Brisbin
 * @author Stephane Maldini
 * @author Andy Wilkinson
 */
public class R {

	public static final Timer TIMER = new Timer("reactor-timer", true);

	private static final Logger     LOG             = LoggerFactory.getLogger(R.class);
	private static final Dispatcher SYNC_DISPATCHER = new SynchronousDispatcher();

	private final Reactor rootReactor;
	private final NonBlockingHashMap<String, ReactorEntry> reactors = new NonBlockingHashMap<String, ReactorEntry>();

	static R self;

	static void assignRx(R r) {
		self = r;
	}

	public R() {
		TIMER.schedule(new TimerTask() {
			@Override
			public void run() {
				for (Map.Entry<String, ReactorEntry> entry : reactors.entrySet()) {
					try {
						if (null != entry.getValue().validator
								&& entry.getValue().validator.isValid(entry.getKey(),
																											entry.getValue().reactor,
																											(System.nanoTime() - entry.getValue().created))) {
							if (LOG.isInfoEnabled()) {
								LOG.info("Removing invalid Reactor {} with id {}",
												 entry.getValue().reactor,
												 entry.getKey());
							}
							reactors.remove(entry.getKey());
						}
					} catch (Exception e) {
						throw new IllegalStateException(e);
					}
				}
				//todo Configuration DSL
			}
		}, 5000, 5000);

		//start root reactor
		rootReactor = new Reactor();

		//post hooks
		postInit();
	}

	static void updateRegistry(Reactor r) {
		self.reactors.put(r.getId().toString(), new ReactorEntry(r));
	}

	/**
	 * Create a {@link Reactor} and optionally set a synchronous {@link reactor.fn.dispatch.Dispatcher}.
	 *
	 * @param synchronous {@literal true} if the {@literal Reactor} needs a synchronous {@literal Dispatcher}, {@literal
	 *                    false} otherwise.
	 * @return The newly-created {@link Reactor}.
	 * @see {@link R#get(UUID)}
	 * @see {@link R#get(String)}
	 */
	public static Reactor create(boolean synchronous) {
		Reactor r = new Reactor();
		if (synchronous) {
			r.setDispatcher(SYNC_DISPATCHER);
		}
		updateRegistry(r);
		return r;
	}

	/**
	 * Create a {@link Reactor} with default behavior.
	 *
	 * @return The newly-created {@link Reactor}.
	 * @see {@link R#get(UUID)}
	 * @see {@link R#get(String)}
	 */
	public static Reactor create() {
		return create(false);
	}

	/**
	 * Create a new {@link Reactor} with the given id or, if it already exists, return the one already created.
	 *
	 * @param id The id of the {@literal Reactor}.
	 * @return The {@link Reactor}.
	 */
	public static Reactor createOrGet(String id) {
		if (self.reactors.containsKey(id)) {
			return self.reactors.get(id).reactor;
		}
		Reactor r = new Reactor();
		self.reactors.put(id, new ReactorEntry(r));
		return r;
	}

	/**
	 * Create a {@link Reactor} but use the given {@link Validator} implementation to determine whether or not the {@link
	 * Reactor} is valid or whether it should be removed from the registry.
	 *
	 * @param validator A {@link Validator} implementation that tells the system whether a {@link Reactor} is valid or
	 *                  not.
	 * @return The id of the newly-created {@link Reactor}.
	 * @see {@link R#get(String)}
	 */
	public static Reactor create(Validator validator) {
		Reactor r = new Reactor();
		self.reactors.put(r.getId().toString(), new ReactorEntry(r, validator));
		return r;
	}

	/**
	 * Create a {@link Reactor} that will cease to be valid after the given time period.
	 *
	 * @param period   The length of the time period for which this {@literal Reactor} will be valid.
	 * @param timeUnit The units of time this period is measured in.
	 * @return The id of the newly-created {@link Reactor}.
	 * @see {@link R#get(String)}
	 */
	public static Reactor createAndExpireAfter(long period, TimeUnit timeUnit) {
		return create(new IsExpiredValidator(timeUnit.toMillis(period)));
	}

	/**
	 * Whether a {@link Reactor} exists for this id or not.
	 *
	 * @param id The id to check.
	 * @return {@literal true} if the {@literal Reactor} exists, false otherwise.
	 */
	public static boolean exists(String id) {
		return (null != self.reactors.get(id));
	}

	/**
	 * Get a {@link Reactor} from the registry.
	 *
	 * @param id The literal {@link UUID} of the {@literal Reactor} to retrieve.
	 * @return The {@literal Reactor} instance if it exists, {@literal null} otherwise.
	 */
	public static Reactor get(UUID id) {
		return get(id.toString());
	}

	/**
	 * Get a {@link Reactor} from the registry.
	 *
	 * @param id The id of the {@literal Reactor} to retrieve.
	 * @return The {@literal Reactor} instance if it exists, {@literal null} otherwise.
	 */
	public static Reactor get(String id) {
		if (null == id) {
			return null;
		}
		ReactorEntry re;
		if (null == (re = self.reactors.get(id))) {
			return null;
		}

		if (null != re.validator
				&& !re.validator.isValid(id, re.reactor, (System.nanoTime() - re.created))) {
			if (LOG.isInfoEnabled()) {
				LOG.info("Removing invalid Reactor {} with id {}",
								 re.reactor,
								 id);
			}
			self.reactors.remove(id);
			return null;
		}

		return re.reactor;
	}

	/**
	 * Convenience method for doing event handling using a global singleton {@link Reactor}.
	 *
	 * @param selector
	 * @param consumer
	 */
	public static <T> void on(Selector selector, Consumer<Event<T>> consumer) {
		self.rootReactor.on(selector, consumer);
	}

	/**
	 * Convenience method for assigning event consumers to a {@link Reactor} by id reference.
	 *
	 * @param id
	 * @param selector
	 * @param consumer
	 */
	public static <T> void on(String id, Selector selector, Consumer<Event<T>> consumer) {
		Reactor r = get(id);
		if (null != r) {
			r.on(selector, consumer);
		}
	}

	/**
	 * Convenience method for assigning event consumers to the root {@link Reactor} using the default {@link Selector}.
	 *
	 * @param consumer
	 */
	public static <T> void on(Consumer<Event<T>> consumer) {
		self.rootReactor.on(consumer);
	}

	/**
	 * Convenience method for publishing events on the global {@link Reactor}.
	 *
	 * @param key   The notification key.
	 * @param event The event to publish.
	 */
	public static void notify(Object key, Event<?> event) {
		self.rootReactor.notify(key, event);
	}

	/**
	 * Convenience method for publishing events on a {@link Reactor} using its id reference.
	 *
	 * @param id       The id of the {@link Reactor}.
	 * @param key      The key.
	 * @param event    The event to publish.
	 */
	public static void notifyReactor(String id, Object key, Event<?> event) {
		Reactor r = get(id);
		if (null != r) {
			r.notify(key, event);
		}
	}

	/**
	 * Convenience method for publishing events on a {@link Reactor}'s default key using its id reference.
	 *
	 * @param id    The id of the {@link Reactor}.
	 * @param event The event to publish.
	 */
	public static void notifyReactor(String id, Event<?> event) {
		Reactor r = get(id);
		if (null != r) {
			r.notify(event);
		}
	}

	/**
	 * Convenience method for publishing events on the global {@link Reactor}'s default key.
	 *
	 * @param event The event to publish.
	 */
	public static void notify(Event<?> event) {
		self.rootReactor.notify(event);
	}

	/**
	 * Link the given {@link Reactor} to the root (global) {@literal Reactor}.
	 *
	 * @param reactor The {@literal Reactor}s to link to the root.
	 */
	public static void link(Reactor reactor) {
		self.rootReactor.link(reactor);
	}

	/**
	 * Reply to an {@link Event} by looking at the {@literal replyTo} property of the {@literal src} as well as the id of
	 * the {@link Reactor} as set in the {@literal headers.origin} property.
	 *
	 * @param src   The source {@link Event} to which a reply is being sent.
	 * @param reply The {@link Event} to reply with.
	 */
	public static void replyTo(Event<?> src, Event<?> reply) {
		Assert.notNull(src, "Source Event cannot be null.");
		Assert.notNull(reply, "Reply Event cannot be null.");

		Object replyToKey = src.getReplyTo();
		Reactor replyToReactor = R.get(src.getHeaders().getOrigin());

		if (null == replyToKey) {
			if (null == replyToReactor) {
				notify(reply);
			} else {
				replyToReactor.notify(reply);
			}
		} else {
			if (null == replyToReactor) {
				notify(replyToKey, reply);
			} else {
				replyToReactor.notify(replyToKey, reply);
			}
		}
	}

	/**
	 * Schedule an arbitrary {@link Consumer} to be executed on the given {@link Dispatcher}, passing the given {@link
	 * Event}.
	 *
	 * @param consumer The {@link Consumer} to invoke.
	 * @param data     The data to pass to the consumer.
	 * @param <T>      The type of the data.
	 */
	public static <T> void schedule(final Consumer<T> consumer, T data, Observable observable) {
		Object key = new Object();
		Selector sel = $(key);
		observable.on(sel, new Consumer<Event<T>>() {
			@Override
			public void accept(Event<T> event) {
				consumer.accept(event.getData());
			}
		}).cancelAfterUse();
		observable.notify(key, Fn.event(data));
	}

	/**
	 * Schedule an arbitrary {@link Consumer} to be executed on the global timer thread.
	 *
	 * @param consumer The {@link Consumer} to invoke.
	 * @param data     The data to pass to the consumer.
	 * @param <T>      The type of the data.
	 */
	public static <T> void schedule(final Consumer<T> consumer, final T data) {
		TIMER.schedule(
				new TimerTask() {
					@Override
					public void run() {
						consumer.accept(data);
					}
				},
				0
		);
	}

	/**
	 * Schedule an arbitrary repeating {@link Consumer} to be executed on the global timer thread using the given
	 * interval.
	 *
	 * @param consumer The {@link Consumer} to schedule.
	 * @param data     The data to pass to the consumer.
	 * @param interval The interval at which to schedule this repeating task.
	 * @param <T>      The type of the data.
	 */
	public static <T> void schedule(final Consumer<T> consumer, final T data, long interval) {
		TIMER.schedule(
				new TimerTask() {
					@Override
					public void run() {
						consumer.accept(data);
					}
				},
				0,
				interval
		);
	}

	protected void postInit() {
		rootReactor.on(T(Throwable.class), new Consumer<Event<Throwable>>() {
			public void accept(Event<Throwable> ev) {
				if (null != ev.getData()) {
					LOG.error(ev.getData().getMessage(), ev.getData());
				}
			}
		});
	}

	/**
	 * Implementations of this interface determine whether a {@link Reactor} instance is still valid or not.
	 */
	public static interface Validator {
		/**
		 * Given a id, a {@link Reactor}, and an age (in milliseconds), determine whether a {@literal Reactor} is still valid
		 * or not.
		 *
		 * @param id      The {@literal Reactor}'s id.
		 * @param reactor The {@literal Reactor}.
		 * @param age     The age of the {@literal Reactor} in milliseconds.
		 * @return {@literal true} if the {@literal Reactor} is still valid, {@literal false} otherwise.
		 */
		boolean isValid(String id, Reactor reactor, Long age);
	}

	private static class ReactorEntry {
		final Reactor reactor;
		final Long    created;
		Validator validator;

		private ReactorEntry(Reactor reactor) {
			this.reactor = reactor;
			this.created = System.nanoTime();
		}

		private ReactorEntry(Reactor reactor, Validator validator) {
			this.reactor = reactor;
			this.created = System.nanoTime();
			this.validator = validator;
		}

		@Override
		public boolean equals(Object o) {
			if (this == o) return true;
			if (o == null || getClass() != o.getClass()) return false;

			ReactorEntry that = (ReactorEntry) o;

			return reactor.equals(that.reactor);

		}

		@Override
		public int hashCode() {
			return reactor.hashCode();
		}
	}

	private static class IsExpiredValidator implements Validator {
		final Long period;

		private IsExpiredValidator(Long period) {
			this.period = period;
		}

		@Override
		public boolean isValid(String id, Reactor reactor, Long age) {
			return (age < period);
		}
	}

}
