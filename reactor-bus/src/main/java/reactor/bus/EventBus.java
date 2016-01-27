/*
 * Copyright (c) 2011-2016 Pivotal Software Inc, All Rights Reserved.
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

package reactor.bus;

import java.lang.reflect.ParameterizedType;
import java.lang.reflect.Type;
import java.util.List;
import java.util.Map;
import javax.annotation.Nonnull;
import javax.annotation.Nullable;

import org.reactivestreams.Processor;
import org.reactivestreams.Publisher;
import org.reactivestreams.Subscriber;
import org.reactivestreams.Subscription;
import reactor.bus.registry.CachingRegistry;
import reactor.bus.registry.Registration;
import reactor.bus.registry.Registries;
import reactor.bus.registry.Registry;
import reactor.bus.routing.Router;
import reactor.bus.selector.ClassSelector;
import reactor.bus.selector.Selector;
import reactor.bus.selector.Selectors;
import reactor.bus.spec.EventBusSpec;
import reactor.bus.stream.BusStream;
import reactor.core.flow.Loopback;
import reactor.core.flow.Producer;
import reactor.core.state.Introspectable;
import reactor.core.subscriber.Subscribers;
import reactor.core.subscriber.SubscriptionWithContext;
import reactor.core.util.Assert;
import reactor.core.util.BackpressureUtils;
import reactor.core.util.EmptySubscription;
import reactor.core.util.Logger;
import reactor.core.util.ReactiveStateUtils;
import reactor.fn.BiConsumer;
import reactor.fn.Consumer;
import reactor.fn.Function;
import reactor.fn.Supplier;
import reactor.rx.Stream;

/**
 * A reactor is an event gateway that allows other components to register {@link Event} {@link Consumer}s that can
 * subsequently be notified of events. A consumer is typically registered with a {@link Selector} which, by matching on
 * the notification key, governs which events the consumer will receive. </p> When a {@literal Reactor} is notified of
 * an {@link Event}, a task is dispatched using the reactor's {@link Processor} which causes it to be executed
 * on a
 * thread based on the implementation of the {@link Processor} being used.
 *
 * @author Jon Brisbin
 * @author Stephane Maldini
 * @author Andy Wilkinson
 */
@SuppressWarnings({"unchecked", "rawtypes"})
public class EventBus extends AbstractBus<Object, Event<?>> implements Consumer<Event<?>>,
                                                                       Loopback {

	private final Processor<Event<?>, Event<?>> processor;

	/**
	 * Create a new {@link reactor.bus.spec.EventBusSpec} to configure a Reactor.
	 *
	 * @return The Reactor spec
	 */
	public static EventBusSpec config() {
		return new EventBusSpec();
	}

	/**
	 * Create a new synchronous {@link EventBus}
	 *
	 * @return A new {@link EventBus}
	 */
	public static EventBus create() {
		return create(null);
	}

	/**
	 * Create a new {@link EventBus} using the given {@link Processor}.
	 *
	 * @param processor The {@link Processor} to use.
	 * @return A new {@link EventBus}
	 */
	public static EventBus create(Processor<Event<?>, Event<?>> processor) {
		return create(processor, 1);
	}

	/**
	 * Create a new {@link EventBus} using the given {@link Processor}.
	 *
	 * @param processor   The {@link Processor} to use.
	 * @param concurrency The allowed number of concurrent routing. This is highly dependent on the
	 *                    processor used. Only "Work" processors like {@link reactor.core.publisher
	 *                    .ProcessorWorkQueue}
	 *                    will be meaningful as they distribute their messages, default RS behavior is to broadcast
	 *                    resulting
	 * @return A new {@link EventBus}
	 */
	public static EventBus create(Processor<Event<?>, Event<?>> processor, int concurrency) {
		return new EventBus(processor, concurrency);
	}


	/**
	 * Create a new {@literal Reactor} that uses the given {@link Processor}. The reactor will use a default
	 * {@link
	 * reactor.bus.routing.Router} that broadcast events to all of the registered consumers that {@link
	 * Selector#matches(Object) match}
	 * the notification key and does not perform any type conversion.
	 *
	 * @param processor   The {@link Processor} to use. May be {@code null} in which case the bus will be synchronous
	 * @param concurrency The allowed number of concurrent routing. This is highly dependent on the
	 *                    processor used. Only "Work" processors like {@link reactor.core.publisher
	 *                    .ProcessorWorkQueue}
	 *                    will be meaningful as they distribute their messages, default RS behavior is to broadcast
	 *                    resulting
	 */
	public EventBus(@Nullable Processor<Event<?>, Event<?>> processor, int concurrency) {
		this(processor, concurrency, null);
	}

	/**
	 * Create a new {@literal Reactor} that uses the given {@link Processor}. The reactor will use a default
	 * {@link
	 * CachingRegistry}.
	 *
	 * @param processor   The {@link Processor} to use. May be {@code null} in which case a new synchronous
	 *                    processor
	 *                    is used.
	 * @param concurrency The allowed number of concurrent routing. This is highly dependent on the
	 *                    processor used. Only "Work" processors like {@link reactor.core.publisher
	 *                    .ProcessorWorkQueue}
	 *                    will be meaningful as they distribute their messages, default RS behavior is to broadcast
	 *                    resulting
	 * @param router      The {@link Router} used to route events to {@link Consumer Consumers}. May be {@code null} in
	 *                    which case the
	 *                    default event router that broadcasts events to all of the registered consumers that {@link
	 *                    Selector#matches(Object) match} the notification key and does not perform any type conversion
	 *                    will be used.
	 */
	public EventBus(@Nullable Processor<Event<?>, Event<?>> processor,
	                int concurrency,
	                @Nullable Router router) {
		this(processor, concurrency, router, null, null);
	}

	public EventBus(@Nullable Processor<Event<?>, Event<?>> processor,
	                int concurrency,
	                @Nullable Router router,
	                @Nullable Consumer<Throwable> processorErrorHandler,
	                @Nullable final Consumer<Throwable> uncaughtErrorHandler) {
		this(Registries.<Object, BiConsumer<Object, ? extends Event<?>>>create(),
		  processor,
		  concurrency,
		  router,
		  processorErrorHandler,
		  uncaughtErrorHandler);
	}

	/**
	 * Create a new {@literal Reactor} that uses the given {@code processor} and {@code eventRouter}.
	 *
	 * @param consumerRegistry      The {@link Registry} to be used to match {@link Selector} and dispatch to {@link
	 *                              Consumer}
	 * @param processor             The {@link Processor} to use. May be {@code null} in which case a new synchronous
	 *                              processor is used.
	 * @param concurrency           The allowed number of concurrent routing. This is highly dependent on the
	 *                              processor used. Only "Work" processors like {@link reactor.core.publisher
	 *                              .ProcessorWorkQueue} will be meaningful as they distribute their messages,
	 *                              default RS behavior is to broadcast resulting in a matching number of duplicate
	 *                              routing.
	 * @param router                The {@link Router} used to route events to {@link Consumer Consumers}. May be {@code
	 *                              null} in which case the default event router that broadcasts events to all of the
	 *                              registered consumers that {@link
	 *                              Selector#matches(Object) match} the notification key and does not perform any type
	 *                              conversion will be used.
	 * @param processorErrorHandler The {@link Consumer} to be used on {@link Processor} exceptions. May be {@code null}
	 *                              in which case exceptions will be routed to this {@link EventBus} by it's class.
	 * @param uncaughtErrorHandler  Default {@link Consumer} to be used on all uncaught exceptions. May be {@code null}
	 *                              in which case exceptions will be logged.
	 */
	@SuppressWarnings("unchecked")
	public EventBus(@Nonnull final Registry<Object, BiConsumer<Object, ? extends Event<?>>> consumerRegistry,
	                @Nullable Processor<Event<?>, Event<?>> processor,
	                int concurrency,
	                @Nullable final Router router,
					@Nullable Consumer<Throwable> processorErrorHandler,
	                @Nullable final Consumer<Throwable> uncaughtErrorHandler) {
		super(consumerRegistry,
					concurrency,
					router,
					processorErrorHandler != null ? processorErrorHandler : new UncaughtExceptionConsumer(consumerRegistry),
					uncaughtErrorHandler);

		Assert.notNull(consumerRegistry, "Consumer Registry cannot be null.");
		this.processor = processor;

		if(processor != null) {
			for (int i = 0; i < concurrency; i++) {
				processor.subscribe(Subscribers.unbounded(new DispatchEventSubscriber(), getProcessorErrorHandler()));
			}
			processor.onSubscribe(EmptySubscription.INSTANCE);
		}

		this.on(new ClassSelector(Throwable.class), new BusErrorConsumer(uncaughtErrorHandler));
	}

	/**
	 * Get the {@link Processor} currently in use.
	 *
	 * @return The {@link Processor}.
	 */
	public Processor<Event<?>, Event<?>> getProcessor() {
		return processor;
	}

	@Override
	public Object connectedInput() {
		return processor;
	}

	@Override
	public Object connectedOutput() {
		return processor;
	}

	@Override
	public <T extends Event<?>> Registration<Object, BiConsumer<Object, ? extends Event<?>>> on(final Selector selector,
																								final Consumer<T> consumer) {
		Assert.notNull(consumer, "Consumer cannot be null.");

		final Class<?> tClass = extractGeneric(consumer);

		Consumer<T> proxyConsumer = new EventBusConsumer<>(selector, tClass, consumer);

		return super.on(selector, proxyConsumer);
	}

	private Class<?> extractGeneric(Consumer<? extends Event<?>> consumer) {
		if (consumer.getClass().getGenericInterfaces().length == 0) return null;

		Type t = consumer.getClass().getGenericInterfaces()[0];
		if (ParameterizedType.class.isAssignableFrom(t.getClass())) {
			ParameterizedType pt = (ParameterizedType) t;

			if (pt.getActualTypeArguments().length == 0) return null;

			t = pt.getActualTypeArguments()[0];
			if (ParameterizedType.class.isAssignableFrom(t.getClass())) {
				pt = (ParameterizedType) t;

				if (pt.getActualTypeArguments().length == 0) return null;

				Type t1 = pt.getActualTypeArguments()[0];
				if (t1 instanceof ParameterizedType) {
					return (Class<?>) ((ParameterizedType) t1).getRawType();
				} else if (t1 instanceof Class) {
					return (Class<?>) t1;
				}
			}
		}
		return null;
	}


	/**
	 * Attach a Stream to the {@link Bus} with the specified {@link Selector}.
	 *
	 * @param broadcastSelector the {@link Selector}/{@literal Object} tuple to listen to
	 * @return a new {@link Stream}
	 * @since 2.0
	 */
	public Stream<? extends Event<?>> on(Selector broadcastSelector) {
		return new BusStream<>(this, broadcastSelector);
	}

	protected void accept(Object key, Event<?> ev) {
		ev.setKey(key);

		if (processor == null) {
			try {
				accept(ev);
			} catch (Throwable throwable) {
				errorHandlerOrThrow(throwable);
			}
		} else {
			processor.onNext(ev);
		}
	}

	/**
	 * Pass values accepted by this {@code Stream} into the given {@link Bus}, notifying with the given key.
	 *
	 * @param key    the key to notify on
	 * @param source the {@link Publisher} to consume
	 * @return {@literal new Stream}
	 * @since 1.1, 2.0
	 */
	public final EventBus notify(@Nonnull final Publisher<?> source, @Nonnull final Object key) {
		return notify(source, new Function<Object, Object>() {
			@Override
			public Object apply(Object o) {
				return key;
			}
		});
	}

	/**
	 * Pass values accepted by this {@code Stream} into the given {@link Bus}, notifying with the given key.
	 *
	 * @param source    the {@link Publisher} to consume
	 * @param keyMapper the key function mapping each incoming data to a key to notify on
	 * @return {@literal new Stream}
	 * @since 2.0
	 */
	public final <T> EventBus notify(@Nonnull final Publisher<? extends T> source, @Nonnull final Function<? super T,
			Object> keyMapper) {
		source.subscribe(new EventSubscriber<T>(keyMapper));
		return this;
	}

	/**
	 * Assign a {@link reactor.fn.Function} to receive an {@link Event} and produce a reply of the given type.
	 *
	 * @param sel The {@link Selector} to be used for matching
	 * @param fn  The transformative {@link reactor.fn.Function} to call to receive an {@link Event}
	 * @return A {@link Registration} object that allows the caller to interact with the given mapping
	 */
	public <T extends Event<?>, V> Registration<?, BiConsumer<Object, ? extends Event<?>>> receive(Selector sel,
			Function<T, V> fn) {
		return on(sel, new ReplyToConsumer<>(fn));
	}

	/**
	 * Notify this component that the given {@link reactor.fn.Supplier} can provide an event that's ready to be
	 * processed.
	 *
	 * @param key      The key to be matched by {@link Selector Selectors}
	 * @param supplier The {@link reactor.fn.Supplier} that will provide the actual {@link Event}
	 * @return {@literal this}
	 */
	public EventBus notify(Object key, Supplier<? extends Event<?>> supplier) {
		notify(key, supplier.get());
		return this;
	}

	/**
	 * Notify this component that the consumers registered with a {@link Selector} that matches the {@code key}
	 * should be
	 * triggered with a {@literal null} input argument.
	 *
	 * @param key The key to be matched by {@link Selector Selectors}
	 * @return {@literal this}
	 */
	public EventBus notify(Object key) {
		notify(key, new Event<>(Void.class));
		return this;
	}

	/**
	 * Notify this component of the given {@link Event} and register an internal {@link Consumer} that will take the
	 * output of a previously-registered {@link Function} and respond using the key set on the {@link Event}'s
	 * {@literal
	 * replyTo} property.
	 *
	 * @param key The key to be matched by {@link Selector Selectors}
	 * @param ev  The {@literal Event}
	 * @return {@literal this}
	 */
	public EventBus send(Object key, Event<?> ev) {
		notify(key, new ReplyToEvent(ev, this));
		return this;
	}


	/**
	 * Notify this component that the given {@link Supplier} will provide an {@link Event} and register an internal
	 * {@link
	 * Consumer} that will take the output of a previously-registered {@link Function} and respond using the key set on
	 * the {@link Event}'s {@literal replyTo} property.
	 *
	 * @param key      The key to be matched by {@link Selector Selectors}
	 * @param supplier The {@link Supplier} that will provide the actual {@link Event} instance
	 * @return {@literal this}
	 */
	public EventBus send(Object key, Supplier<? extends Event<?>> supplier) {
		notify(key, new ReplyToEvent(supplier.get(), this));
		return this;
	}

	/**
	 * Notify this component of the given {@link Event} and register an internal {@link Consumer} that will take the
	 * output of a previously-registered {@link Function} and respond to the key set on the {@link Event}'s {@literal
	 * replyTo} property and will call the {@code notify} method on the given {@link Bus}.
	 *
	 * @param key     The key to be matched by {@link Selector Selectors}
	 * @param ev      The {@literal Event}
	 * @param replyTo The {@link Bus} on which to invoke the notify method
	 * @return {@literal this}
	 */
	public EventBus send(Object key, Event<?> ev, Bus replyTo) {
		notify(key, new ReplyToEvent(ev, replyTo));
		return this;
	}


	/**
	 * Notify this component that the given {@link Supplier} will provide an {@link Event} and register an internal
	 * {@link
	 * Consumer} that will take the output of a previously-registered {@link Function} and respond to the key set on
	 * the
	 * {@link Event}'s {@literal replyTo} property and will call the {@code notify} method on the given {@link
	 * Bus}.
	 *
	 * @param key      The key to be matched by {@link Selector Selectors}
	 * @param supplier The {@link Supplier} that will provide the actual {@link Event} instance
	 * @param replyTo  The {@link Bus} on which to invoke the notify method
	 * @return {@literal this}
	 */
	public EventBus send(Object key, Supplier<? extends Event<?>> supplier, Bus replyTo) {
		notify(key, new ReplyToEvent(supplier.get(), replyTo));
		return this;
	}

	/**
	 * Register the given {@link reactor.fn.Consumer} on an anonymous {@link reactor.bus.selector.Selector} and
	 * set the given event's {@code replyTo} property to the corresponding anonymous key, then register the consumer to
	 * receive replies from the {@link reactor.fn.Function} assigned to handle the given key.
	 *
	 * @param key   The key to be matched by {@link Selector Selectors}
	 * @param event The event to notify.
	 * @param reply The consumer to register as a reply handler.
	 * @return {@literal this}
	 */
	public <T extends Event<?>> EventBus sendAndReceive(Object key, Event<?> event, Consumer<T> reply) {
		Selector sel = Selectors.anonymous();
		on(sel, reply).cancelAfterUse();
		notify(key, event.setReplyTo(sel.getObject()));
		return this;
	}

	/**
	 * Register the given {@link reactor.fn.Consumer} on an anonymous {@link reactor.bus.selector.Selector} and
	 * set the event's {@code replyTo} property to the corresponding anonymous key, then register the consumer to
	 * receive
	 * replies from the {@link reactor.fn.Function} assigned to handle the given key.
	 *
	 * @param key      The key to be matched by {@link Selector Selectors}
	 * @param supplier The supplier to supply the event.
	 * @param reply    The consumer to register as a reply handler.
	 * @return {@literal this}
	 */
	public <T extends Event<?>> EventBus sendAndReceive(Object key, Supplier<? extends Event<?>> supplier, Consumer<T>
	  reply) {
		return sendAndReceive(key, supplier.get(), reply);
	}

	/**
	 * Create an optimized path for publishing notifications to the given key.
	 *
	 * @param key The key to be matched by {@link Selector Selectors}
	 * @return a {@link Consumer} to invoke with the {@link Event Events} to publish
	 */
	public <T> Consumer<Event<T>> prepare(final Object key) {
		return new PreparedConsumer<>(key);
	}

	/**
	 * Schedule an arbitrary {@link reactor.fn.Consumer} to be executed on the current Reactor  {@link
	 * Processor}, passing the given {@param data}.
	 *
	 * @param consumer The {@link reactor.fn.Consumer} to invoke.
	 * @param data     The data to pass to the consumer.
	 * @param <T>      The type of the data.
	 */
	public <T> void schedule(final Consumer<T> consumer, final T data) {
		if (processor == null) {
			try {
				consumer.accept(data);
			} catch (Throwable t) {
				errorHandlerOrThrow(t);
			}
			return;
		}

		processor.onNext(new ConsumerEvent<>(consumer, data));
	}

	@Override
	public void accept(Event<?> event) {
		if (event.getClass() == ConsumerEvent.class) {
			((ConsumerEvent) event).run();
		} else {
			route(event.getKey(), event);
		}
	}

	public static class ReplyToEvent<T> extends Event<T> {
		private static final long serialVersionUID = 1937884784799135647L;
		private final Bus replyToObservable;

		private ReplyToEvent(Headers headers, T data, Object replyTo,
		                     Bus replyToObservable,
		                     Consumer<Throwable> errorConsumer) {
			super(headers, data, errorConsumer);
			setReplyTo(replyTo);
			this.replyToObservable = replyToObservable;
		}

		private ReplyToEvent(Event<T> delegate, Bus replyToObservable) {
			this(delegate.getHeaders(), delegate.getData(), delegate.getReplyTo(), replyToObservable,
			  delegate.getErrorConsumer());
		}

		@Override
		public <X> Event<X> copy(X data) {
			return new ReplyToEvent<X>(getHeaders(), data, getReplyTo(), replyToObservable, getErrorConsumer());
		}

		public Bus getReplyToObservable() {
			return replyToObservable;
		}
	}

	private static class ConsumerEvent<T> extends Event<Consumer<T>> {
		private final T data;

		public ConsumerEvent(Consumer<T> consumer, T data) {
			super(consumer);
			this.data = data;
		}

		void run() {
			getData().accept(data);
		}
	}

	private static class EventBusConsumer<T extends Event<?>> implements Consumer<T>, Introspectable, Producer {

		private final Selector selector;
		private final Class<?>    tClass;
		private final Consumer<T> consumer;

		public EventBusConsumer(Selector selector, Class<?> tClass, Consumer<T> consumer) {
			this.selector = selector;
			this.tClass = tClass;
			this.consumer = consumer;
		}

		@Override
		public void accept(T e) {
			if (null != selector.getHeaderResolver()) {
				Function<Object, Map<String,Object>> resolver = selector.getHeaderResolver();
				e.getHeaders().setAll(resolver.apply(e.getKey()));
			}
			if (tClass == null || e.getData() == null || tClass.isAssignableFrom(e.getData().getClass())) {
				consumer.accept(e);
			}
		}

		@Override
		public Object downstream() {
			return consumer;
		}

		@Override
		public int getMode() {
			return TRACE_ONLY;
		}

		@Override
		public String getName() {
			return EventBusConsumer.class.getSimpleName();
		}
	}

	private static class UncaughtExceptionConsumer implements Consumer<Throwable> {

		private final Registry<Object, BiConsumer<Object, ? extends Event<?>>> consumerRegistry;

		public UncaughtExceptionConsumer(Registry<Object, BiConsumer<Object, ? extends Event<?>>> consumerRegistry) {
			this.consumerRegistry = consumerRegistry;
		}

		@Override
		public void accept(Throwable t) {
			Class<? extends Throwable> type = t.getClass();
			DEFAULT_EVENT_ROUTER.route(type,
									   Event.wrap(t),
									   consumerRegistry.select(type),
									   null,
									   null);
		}
	}

	private static class BusErrorConsumer implements Consumer<Event<Throwable>> {

		final         Logger              log;
		private final Consumer<Throwable> uncaughtErrorHandler;

		public BusErrorConsumer(Consumer<Throwable> uncaughtErrorHandler) {
			this.uncaughtErrorHandler = uncaughtErrorHandler;
			log = Logger.getLogger(EventBus.class);
		}

		@Override
		public void accept(Event<Throwable> ev) {
			if (null == uncaughtErrorHandler) {
				log.error(ev.getData().getMessage(), ev.getData());
			} else {
				uncaughtErrorHandler.accept(ev.getData());
			}
		}
	}

	public class ReplyToConsumer<E extends Event<?>, V> implements Consumer<E> {
		private final Function<E, V> fn;

		private ReplyToConsumer(Function<E, V> fn) {
			this.fn = fn;
		}

		@Override
		public void accept(E ev) {
			Bus replyToObservable = EventBus.this;

			if (ReplyToEvent.class.isAssignableFrom(ev.getClass())) {
				Bus o = ((ReplyToEvent<?>) ev).getReplyToObservable();
				if (null != o) {
					replyToObservable = o;
				}
			}

			try {
				V reply = fn.apply(ev);

				Event<?> replyEv;
				if (null == reply) {
					replyEv = new Event<Void>(Void.class);
				} else {
					replyEv = (Event.class.isAssignableFrom(reply.getClass()) ? (Event<?>) reply : Event.wrap(reply));
				}

				replyToObservable.notify(ev.getReplyTo(), replyEv);
			} catch (Throwable x) {
				replyToObservable.notify(x.getClass(), Event.wrap(x));
			}
		}

		public Function<E, V> getDelegate() {
			return fn;
		}
	}

	/**
	 *
	 * @return
	 */
	public ReactiveStateUtils.Graph debug(){
		return ReactiveStateUtils.scan(this);
	}

	private final class PreparedConsumer<T> implements Consumer<Event<T>> {

		final         List<Registration<Object, ? extends BiConsumer<Object, ? extends Event<?>>>> regs;
		final         int                                                                          size;
		private final Object                                                                       key;

		public PreparedConsumer(Object key) {
			this.key = key;
			regs = getConsumerRegistry().select(key);
			size = regs.size();
		}

		@Override
		public void accept(Event<T> ev) {
			for (int i = 0; i < size; i++) {
				Registration<Object, ? extends Consumer<Event<T>>> reg =
				  (Registration<Object, ? extends Consumer<Event<T>>>) regs.get(i);
				ev.setKey(key);
				if (processor == null) {
					try {
						accept(ev);
					} catch (Throwable t) {
						errorHandlerOrThrow(t);
					}
				} else {
					schedule(reg.getObject(), ev);
				}
			}
		}
	}

	private final class EventSubscriber<T> implements Subscriber<T> {

		private final Function<? super T, Object> keyMapper;
		Subscription s;

		public EventSubscriber(Function<? super T, Object> keyMapper) {
			this.keyMapper = keyMapper;
		}

		@Override
		public void onSubscribe(Subscription s) {
			if(BackpressureUtils.validate(this.s, s)) {
				this.s = s;
				s.request(Long.MAX_VALUE);
			}
		}

		@Override
		public void onNext(T t) {
			EventBus.this.notify(keyMapper.apply(t), Event.wrap(t));
		}

		@Override
		public void onError(Throwable t) {
			s = null;
		}

		@Override
		public void onComplete() {
			s = null;
		}
	}

	private final class DispatchEventSubscriber implements BiConsumer<Event<?>, SubscriptionWithContext<Void>> {

		@Override
		public void accept(Event<?> event, SubscriptionWithContext<Void> voidSubscriptionWithContext) {
			EventBus.this.accept(event);
		}
	}
}
