/*
 * Copyright (c) 2011-2015 Pivotal Software Inc, All Rights Reserved.
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
package reactor.core.processor;

import org.reactivestreams.Processor;
import org.reactivestreams.Publisher;
import org.reactivestreams.Subscriber;
import org.reactivestreams.Subscription;
import reactor.core.error.CancelException;
import reactor.core.error.Exceptions;
import reactor.core.error.ReactorFatalException;
import reactor.core.error.SpecificationExceptions;
import reactor.core.processor.rb.MutableSignal;
import reactor.core.processor.rb.RingBufferSubscriberUtils;
import reactor.core.publisher.PublisherFactory;
import reactor.core.subscriber.BaseSubscriber;
import reactor.core.support.*;
import reactor.fn.BiConsumer;
import reactor.fn.Consumer;
import reactor.fn.Supplier;
import reactor.fn.timer.GlobalTimer;
import reactor.fn.timer.Timer;
import reactor.core.processor.rb.disruptor.RingBuffer;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.concurrent.Executor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicIntegerFieldUpdater;

/**
 * A Shared Processor Service is a {@link Processor} factory eventually sharing one or more internal {@link Processor}.
 * <p>
 * Its purpose is to mutualize some threading and event loop resources, thus creating an (a)sync gateway reproducing
 * the input sequence of signals to their attached subscriber context.
 * Its default behavior will be to request a fair share of the internal
 * {@link Processor} to allow many concurrent use of a single async resource.
 * <p>
 * Alongside building Processor, SharedProcessor can generate unbounded dispatchers as:
 * - a {@link BiConsumer} that schedules the data argument over the  {@link Consumer} task argument.
 * - a {@link Consumer} that schedules  {@link Consumer} task argument.
 * - a {@link Executor} that runs an arbitrary {@link Runnable} task.
 * <p>
 * SharedProcessor maintains a reference count on how many artefacts have been built. Therefore it will automatically
 * shutdown the internal async resource after all references have been released. Each reference (consumer, executor
 * or processor)
 * can be used in combination with {@link ProcessorGroup#release(Object...)} to cleanly unregister and
 * eventually
 * shutdown when no more references use that service.
 *
 * @param <T> the default type (not enforced at runtime)
 * @author Anatoly Kadyshev
 * @author Stephane Maldini
 */
public class ProcessorGroup<T> implements Supplier<Processor<T, T>>, Resource {

	/**
	 * @param <E>
	 * @return
	 */
	@SuppressWarnings("unchecked")
	public static <E> ProcessorGroup<E> sync() {
		return (ProcessorGroup<E>) SYNC_SERVICE;
	}

	/**
	 * @param p
	 * @param <E>
	 * @return
	 */
	public static <E> ProcessorGroup<E> create(Processor<Task, Task> p) {
		return create(p, null, null, true);
	}

	/**
	 * @param p
	 * @param <E>
	 * @return
	 */
	public static <E> ProcessorGroup<E> create(Processor<Task, Task> p, int concurrency) {
		return create(p, concurrency, null, null, true);
	}

	/**
	 * @param p
	 * @param concurrency
	 * @param <E>
	 * @return
	 */
	public static <E> ProcessorGroup<E> create(Supplier<? extends Processor<Task, Task>> p,
	                                             int concurrency) {
		return create(p, concurrency, null, null, true);
	}

	/**
	 * @param p
	 * @param autoShutdown
	 * @param <E>
	 * @return
	 */
	public static <E> ProcessorGroup<E> create(Processor<Task, Task> p,
	                                             boolean autoShutdown) {
		return create(p, null, null, autoShutdown);
	}

	/**
	 * @param p
	 * @param uncaughtExceptionHandler
	 * @param autoShutdown
	 * @param <E>
	 * @return
	 */
	public static <E> ProcessorGroup<E> create(Processor<Task, Task> p,
	                                             Consumer<Throwable> uncaughtExceptionHandler,
	                                             boolean autoShutdown) {
		return create(p, uncaughtExceptionHandler, null, autoShutdown);
	}

	/**
	 * @param p
	 * @param uncaughtExceptionHandler
	 * @param shutdownHandler
	 * @param autoShutdown
	 * @param <E>
	 * @return
	 */
	public static <E> ProcessorGroup<E> create(final Processor<Task, Task> p,
	                                             Consumer<Throwable> uncaughtExceptionHandler,
	                                             Consumer<Void> shutdownHandler,
	                                             boolean autoShutdown) {
		return create(p, 1, uncaughtExceptionHandler, shutdownHandler, autoShutdown);
	}

	/**
	 * @param p
	 * @param concurrency
	 * @param uncaughtExceptionHandler
	 * @param shutdownHandler
	 * @param autoShutdown
	 * @param <E>
	 * @return
	 */
	@SuppressWarnings("unchecked")
	public static <E> ProcessorGroup<E> create(Supplier<? extends Processor<Task, Task>> p,
	                                             int concurrency,
	                                             Consumer<Throwable> uncaughtExceptionHandler,
	                                             Consumer<Void> shutdownHandler,
	                                             boolean autoShutdown) {
		if (p != null && concurrency > 1) {
			return new PooledProcessorGroup<>(p, concurrency, uncaughtExceptionHandler, shutdownHandler,
			  autoShutdown);
		} else {
			return new SingleProcessorGroup<E>(p, concurrency, uncaughtExceptionHandler, shutdownHandler,
			  autoShutdown);
		}
	}

	/**
	 * @param p
	 * @param concurrency
	 * @param uncaughtExceptionHandler
	 * @param shutdownHandler
	 * @param autoShutdown
	 * @param <E>
	 * @return
	 */
	@SuppressWarnings("unchecked")
	public static <E> ProcessorGroup<E> create(final Processor<Task, Task> p,
	                                             int concurrency,
	                                             Consumer<Throwable> uncaughtExceptionHandler,
	                                             Consumer<Void> shutdownHandler,
	                                             boolean autoShutdown) {
		return new SingleProcessorGroup<E>(new Supplier<Processor<Task, Task>>() {
			@Override
			public Processor<Task, Task> get() {
				return p;
			}
		}, concurrency, uncaughtExceptionHandler, shutdownHandler,
		  autoShutdown);
	}

	/**
	 * @param sharedProcessorReferences
	 * @return
	 */
	public static void release(Object... sharedProcessorReferences) {
		if (sharedProcessorReferences == null) return;

		for (Object sharedProcessorReference : sharedProcessorReferences) {
			if (sharedProcessorReference != null &&
			  ProcessorBarrier.class.isAssignableFrom(sharedProcessorReference.getClass())) {
				((ProcessorBarrier) sharedProcessorReference).cancel();
			}
		}
	}


	/**
	 * INSTANCE STUFF *
	 */

	final private TailRecurser tailRecurser;


	final private   Processor<Task, Task>     processor;
	final protected boolean                   autoShutdown;
	final protected int                       concurrency;
	final           BaseProcessor<Task, Task> managedProcessor;

	@SuppressWarnings("unused")
	private volatile int refCount = 0;

	private static final AtomicIntegerFieldUpdater<ProcessorGroup> REF_COUNT =
	  AtomicIntegerFieldUpdater
		.newUpdater(ProcessorGroup.class, "refCount");

	@Override
	public Processor<T, T> get() {
		return observeOn();
	}

	/**
	 * @param clazz
	 * @param <V>
	 * @return
	 */
	@SuppressWarnings("unchecked")
	public <V> Processor<V, V> observeOn(Class<V> clazz) {
		return (Processor<V, V>) observeOn();
	}

	/**
	 * @return
	 */
	public Processor<T, T> observeOn() {
		return createBarrier(false);
	}

	/**
	 * @param clazz
	 * @param <V>
	 * @return
	 */
	@SuppressWarnings("unchecked")
	public <V> Processor<V, V> publishOn(Class<V> clazz) {
		return (Processor<V, V>) publishOn();
	}

	/**
	 * @return
	 */
	public Processor<T, T> publishOn() {
		return createBarrier(true);
	}

	/**
	 * @return
	 */
	public Consumer<Consumer<Void>> dispatcher() {
		if (processor == null) {
			return SYNC_DISPATCHER;
		}

		return createBarrier(false);
	}

	/**
	 * @param clazz
	 * @param <V>
	 * @return
	 */
	@SuppressWarnings("unchecked")
	public <V> BiConsumer<V, Consumer<? super V>> dataDispatcher(Class<V> clazz) {
		if (processor == null) {
			return (BiConsumer<V, Consumer<? super V>>) SYNC_DATA_DISPATCHER;
		}

		return (BiConsumer<V, Consumer<? super V>>) createBarrier(false);
	}

	/**
	 * @return
	 */
	@SuppressWarnings("unchecked")
	public BiConsumer<T, Consumer<? super T>> dataDispatcher() {
		if (processor == null) {
			return (BiConsumer<T, Consumer<? super T>>) SYNC_DATA_DISPATCHER;
		}

		return createBarrier(false);
	}

	/**
	 * @return
	 */
	public Executor executor() {
		if (processor == null) {
			return SYNC_EXECUTOR;
		}

		return createBarrier(false);
	}

	@Override
	public boolean awaitAndShutdown() {
		return awaitAndShutdown(-1, TimeUnit.SECONDS);
	}

	@Override
	public boolean awaitAndShutdown(long timeout, TimeUnit timeUnit) {
		if (processor == null) {
			return true;
		} else if (Resource.class.isAssignableFrom(processor.getClass())) {
			return ((Resource) processor).awaitAndShutdown(timeout, timeUnit);
		}
		throw new UnsupportedOperationException("Underlying Processor doesn't implement Resource");
	}

	@Override
	public void forceShutdown() {
		if (processor == null) {
			return;
		} else if (Resource.class.isAssignableFrom(processor.getClass())) {
			((Resource) processor).forceShutdown();
			return;
		}
		throw new UnsupportedOperationException("Underlying Processor doesn't implement Resource");
	}

	@Override
	public boolean alive() {
		if (processor == null) {
			return true;
		}
		if (Resource.class.isAssignableFrom(processor.getClass())) {
			return ((Resource) processor).alive();
		}
		throw new UnsupportedOperationException("Underlying Processor doesn't implement Resource");
	}

	@Override
	public void shutdown() {
		if (processor == null) {
			return;
		}
		try {
			processor.onComplete();
			if (Resource.class.isAssignableFrom(processor.getClass())) {
				((Resource) processor).shutdown();
			}
		} catch (Throwable t) {
			Exceptions.throwIfFatal(t);
			processor.onError(t);
		}
	}

	/**
	 * A mutable transport for materialized signal dispatching
	 */
	public static final class Task implements Recyclable, Serializable {
		Subscriber subscriber;
		Object     payload;
		SignalType type;

		@Override
		public void recycle() {
			type = null;
			payload = null;
			subscriber = null;
		}
	}

	/**
	 * Simple Task {@link Supplier} used for tail recursion or by a ring buffer
	 */
	public final static Supplier<Task> DEFAULT_TASK_PROVIDER = new Supplier<Task>() {
		@Override
		public Task get() {
			return new Task();
		}
	};


	/* INTERNAL */
	@SuppressWarnings("unchecked")
	static private void route(Object payload, Subscriber subscriber, SignalType type) {

		try {
			if (subscriber == null) return;

			if (type == SignalType.NEXT) {
				subscriber.onNext(payload);
			} else if (type == SignalType.COMPLETE) {
				subscriber.onComplete();
			} else if (type == SignalType.SUBSCRIPTION) {
				subscriber.onSubscribe((Subscription) payload);
			} else {
				subscriber.onError((Throwable) payload);
			}
		} catch (CancelException c) {
			//IGNORE
		} catch (Throwable t) {
			if (type != SignalType.ERROR) {
				Exceptions.throwIfFatal(t);
				subscriber.onError(t);
			} else {
				throw t;
			}
		}
	}


	static private void routeTask(Task task) {
		try {
			route(task.payload, task.subscriber, task.type);
		} finally {
			task.recycle();
		}
	}

	@SuppressWarnings("unchecked")
	private static final ProcessorGroup SYNC_SERVICE = new ProcessorGroup(null, -1, null, null, false);

	/**
	 * Singleton delegating consumer for synchronous data dispatchers
	 */
	private final static BiConsumer SYNC_DATA_DISPATCHER = new BiConsumer() {
		@Override
		@SuppressWarnings("unchecked")
		public void accept(Object o, Object callback) {
			((Consumer) callback).accept(o);
		}
	};

	/**
	 * Singleton delegating consumer for synchronous dispatchers
	 */
	private final static Consumer<Consumer<Void>> SYNC_DISPATCHER = new Consumer<Consumer<Void>>() {
		@Override
		public void accept(Consumer<Void> callback) {
			callback.accept(null);
		}
	};

	/**
	 * Singleton delegating executor for synchronous executor
	 */
	private final static Executor SYNC_EXECUTOR = new Executor() {
		@Override
		public void execute(Runnable command) {
			command.run();
		}
	};

	private final static Consumer<Task> DEFAULT_TASK_CONSUMER = new Consumer<Task>() {
		@Override
		public void accept(Task task) {
			routeTask(task);
		}
	};

	private final static int MAX_BUFFER_SIZE = 2 ^ 17;

	@SuppressWarnings("unchecked")
	protected ProcessorGroup(
	  Supplier<? extends Processor<Task, Task>> processor,
	  int concurrency,
	  Consumer<Throwable> uncaughtExceptionHandler,
	  Consumer<Void> shutdownHandler,
	  boolean autoShutdown
	) {
		this.autoShutdown = autoShutdown;
		this.concurrency = concurrency;

		if (processor != null) {
			this.processor = processor.get();
			Assert.isTrue(this.processor != null);

			// Managed Processor, providing for tail recursion,
			if (BaseProcessor.class.isAssignableFrom(this.processor.getClass())) {

				this.managedProcessor = (BaseProcessor<Task, Task>) this.processor;

				if (concurrency == 1) {
					int bufferSize = (int) Math.min(
					  this.managedProcessor.getCapacity(),
					  MAX_BUFFER_SIZE
					);

					this.tailRecurser = new TailRecurser(
					  bufferSize,
					  DEFAULT_TASK_PROVIDER,
					  DEFAULT_TASK_CONSUMER
					);
				} else {
					this.tailRecurser = null;
				}


			} else {
				this.managedProcessor = null;
				this.tailRecurser = null;
			}

			for (int i = 0; i < concurrency; i++) {
				this.processor.onSubscribe(SignalType.NOOP_SUBSCRIPTION);
				this.processor.subscribe(new TaskSubscriber(uncaughtExceptionHandler, shutdownHandler));
			}

		} else {
			this.processor = null;
			this.managedProcessor = null;
			this.tailRecurser = null;
		}
	}

	protected void decrementReference() {
		if ((processor != null || concurrency > 1) && REF_COUNT.decrementAndGet(this) <= 0
		  && autoShutdown) {

			if (BaseProcessor.CANCEL_TIMEOUT > 0) {
				final Timer timer = GlobalTimer.globalOrNew();
				timer.submit(new Consumer<Long>() {
					@Override
					public void accept(Long aLong) {
						if (refCount == 0) {
							shutdown();
						}
						timer.cancel();
					}
				}, BaseProcessor.CANCEL_TIMEOUT, TimeUnit.SECONDS);
			} else {
				shutdown();
			}
		}
	}

	protected void incrementReference() {
		REF_COUNT.incrementAndGet(this);
	}

	@SuppressWarnings("unchecked")
	private ProcessorBarrier<T> createBarrier(boolean forceWork) {

		if (processor == null) {
			return new SyncProcessorBarrier<>(null);
		}

		if (BaseProcessor.class.isAssignableFrom(processor.getClass())
		  && !((BaseProcessor) processor).alive()) {
			throw new IllegalStateException("Internal Processor is shutdown");
		}

		incrementReference();

		if (forceWork || concurrency > 1) {
			return new WorkProcessorBarrier<>(this);
		}

		if (RingBufferProcessor.class == processor.getClass()) {
			return new RingBufferProcessorBarrier<>(this, ((RingBufferProcessor) processor).ringBuffer());
		}

		return new ProcessorBarrier<>(this);
	}

	/**
	 *
	 */

	static class TailRecurser {

		private final ArrayList<Task> pile;

		private final int            pileSizeIncrement;
		private final Supplier<Task> taskSupplier;

		private final Consumer<Task> taskConsumer;

		private int next = 0;

		public TailRecurser(int backlogSize, Supplier<Task> taskSupplier, Consumer<Task> taskConsumer) {
			this.pileSizeIncrement = backlogSize * 2;
			this.taskSupplier = taskSupplier;
			this.taskConsumer = taskConsumer;
			this.pile = new ArrayList<Task>(pileSizeIncrement);
			ensureEnoughTasks();
		}

		private void ensureEnoughTasks() {
			if (next >= pile.size()) {
				pile.ensureCapacity(pile.size() + pileSizeIncrement);
				for (int i = 0; i < pileSizeIncrement; i++) {
					pile.add(taskSupplier.get());
				}
			}
		}

		public Task next() {
			ensureEnoughTasks();
			return pile.get(next++);
		}

		public void consumeTasks() {
			if (next > 0) {
				for (int i = 0; i < next; i++) {
					taskConsumer.accept(pile.get(i));
				}

				for (int i = next - 1; i >= pileSizeIncrement; i--) {
					pile.remove(i);
				}
				next = 0;
			}
		}

	}

	private static class ProcessorBarrier<V> extends BaseSubscriber<V> implements
	  Consumer<Consumer<Void>>,
	  BiConsumer<V, Consumer<? super V>>,
	  Processor<V, V>,
	  Executor,
	  Subscription,
	  Bounded,
	  Publishable<V>,
	  Subscribable<V> {


		protected final ProcessorGroup service;
		protected final AtomicBoolean  terminated;

		volatile Subscription subscription;
		Subscriber<? super V> subscriber;

		public ProcessorBarrier(ProcessorGroup service) {
			this.service = service;
			this.terminated = service != null && service.processor == null ? null : new AtomicBoolean(false);
		}

		@Override
		public Publisher<V> upstream() {
			return PublisherFactory.fromSubscription(subscription);
		}

		@Override
		public Subscriber<? super V> downstream() {
			return subscriber;
		}

		@Override
		public final void accept(V data, Consumer<? super V> consumer) {
			if (consumer == null) {
				throw SpecificationExceptions.spec_2_13_exception();
			}
			dispatch(data, new ConsumerSubscriber<>(consumer), SignalType.NEXT);
		}

		@Override
		public final void accept(Consumer<Void> consumer) {
			if (consumer == null) {
				throw SpecificationExceptions.spec_2_13_exception();
			}
			dispatch(null, new ConsumerSubscriber<>(consumer), SignalType.NEXT);
		}

		@Override
		public final void execute(Runnable command) {
			if (command == null) {
				throw SpecificationExceptions.spec_2_13_exception();
			}
			dispatch(null, new RunnableSubscriber(command), SignalType.NEXT);
		}

		@Override
		public final void subscribe(Subscriber<? super V> s) {
			if (s == null) {
				throw SpecificationExceptions.spec_2_13_exception();
			}
			final boolean set, subscribed;
			synchronized (this) {
				if (subscriber == null) {
					subscriber = s;
					set = true;
				} else {
					set = false;
				}
				subscribed = this.subscription != null;
			}

			if (!set) {
				Exceptions
				  .<V>publisher(new IllegalStateException("Shared Processors do not support multi-subscribe"))
				  .subscribe(s);
			} else if (subscribed) {
				dispatch(this, s, SignalType.SUBSCRIPTION);
			}

		}

		@Override
		public final void onSubscribe(Subscription s) {
			final Subscriber<? super V> subscriber;

			synchronized (this) {
				if (BackpressureUtils.checkSubscription(subscription, s)) {
					subscription = s;
				}
				subscriber = this.subscriber != null ? this.subscriber : null;
			}

			if (subscriber != null) {
				dispatch(this, subscriber, SignalType.SUBSCRIPTION);
			}
		}

		@Override
		public final void onNext(V o) {
			super.onNext(o);

			if (subscriber == null) {
				//cancelled
				if (subscription == null) return;

				throw CancelException.get();
			}

			dispatchProcessorSequence(o, subscriber, SignalType.NEXT);
		}

		@Override
		public final void onError(Throwable t) {
			super.onError(t);

			if (subscriber == null) {
				//cancelled
				if (subscription == null) return;

				throw ReactorFatalException.create(t);
			}

			dispatchProcessorSequence(t, subscriber, SignalType.ERROR);
			handleTerminalSignal();
		}

		@Override
		public final void onComplete() {
			dispatchProcessorSequence(null, subscriber, SignalType.COMPLETE);
			handleTerminalSignal();
		}

		@Override
		public void request(long n) {
			Subscription subscription = this.subscription;
			if (subscription != null) {
				subscription.request(n);
			}
		}

		@Override
		public void cancel() {
			Subscription subscription = this.subscription;
			if (subscription != null) {
				synchronized (this) {
					this.subscription = null;
					this.subscriber = null;
				}
				subscription.cancel();
				handleTerminalSignal();
			}
		}

		protected void handleTerminalSignal() {
			if (terminated != null && terminated.compareAndSet(false, true) &&
			  service != null) {
				service.decrementReference();
			}
		}

		protected void dispatchProcessorSequence(Object data, Subscriber subscriber, SignalType type) {
			dispatch(data, subscriber, type);
		}

		protected boolean shouldTailRecruse() {
			return service != null && service.tailRecurser != null &&
			  service.managedProcessor != null &&
			  service.managedProcessor.isInContext();
		}

		@SuppressWarnings("unchecked")
		protected void dispatch(Object data, Subscriber subscriber, SignalType type) {
			final Task task;
			if (shouldTailRecruse()) {
				task = service.tailRecurser.next();
				task.type = type;
				task.payload = data;
				task.subscriber = subscriber;
			} else {
				task = new Task();
				task.type = type;
				task.payload = data;
				task.subscriber = subscriber;
				service.processor.onNext(task);
			}
		}

		@Override
		public boolean isExposedToOverflow(Bounded parentPublisher) {
			Subscriber sub = subscriber;
			return sub != null && Bounded.class.isAssignableFrom(sub.getClass()) &&
			  ((Bounded) sub).isExposedToOverflow(parentPublisher);
		}

		@Override
		public long getCapacity() {
			return service != null && service.managedProcessor != null ? service.managedProcessor.getCapacity() : Long
			  .MAX_VALUE;
		}

		@Override
		public String toString() {
			return getClass().getSimpleName() + "{" +
			  "subscription=" + subscription +
			  '}';
		}
	}

	private static final class RingBufferProcessorBarrier<V> extends ProcessorBarrier<V> {

		private final RingBuffer<MutableSignal<Task>> ringBuffer;

		public RingBufferProcessorBarrier(ProcessorGroup service,
		                                  RingBuffer<MutableSignal<Task>> ringBuffer) {
			super(service);
			this.ringBuffer = ringBuffer;
		}

		@Override
		protected void dispatch(Object data, Subscriber subscriber, SignalType type) {
			final Task task;
			if (shouldTailRecruse()) {
				task = service.tailRecurser.next();
				task.type = type;
				task.payload = data;
				task.subscriber = subscriber;
			} else {
				MutableSignal<Task> signal = RingBufferSubscriberUtils.next(ringBuffer);
				task = signal.value != null ? signal.value : new Task(); //TODO should always assume supplied?
				task.type = type;
				task.payload = data;
				task.subscriber = subscriber;

				RingBufferSubscriberUtils.publish(ringBuffer, signal);
			}
		}
	}

	private static final class WorkProcessorBarrier<V> extends ProcessorBarrier<V> {

		private volatile     int                                             start   = 0;
		private static final AtomicIntegerFieldUpdater<WorkProcessorBarrier> STARTED =
		  AtomicIntegerFieldUpdater.newUpdater(WorkProcessorBarrier.class, "start");

		public WorkProcessorBarrier(ProcessorGroup service) {
			super(service);
		}

		@Override
		protected void dispatchProcessorSequence(Object data, Subscriber subscriber, SignalType type) {
			route(data, subscriber, type);
		}


		@Override
		public void request(final long n) {
			if (STARTED.compareAndSet(this, 0, 1) &&
			  service.managedProcessor != null &&
			  !service.managedProcessor.isInContext()) {
				dispatch(n, new BaseSubscriber<Long>() {
					@Override
					public void onNext(Long aLong) {
						WorkProcessorBarrier.super.request(n);
					}
				}, SignalType.NEXT);
			} else {
				super.request(n);
			}
		}

		@Override
		public boolean isExposedToOverflow(Bounded parentPublisher) {
			return false;
		}

		@Override
		public long getCapacity() {
			return Long.MAX_VALUE;
		}
	}

	private static final class SyncProcessorBarrier<V> extends ProcessorBarrier<V> {

		public SyncProcessorBarrier(ProcessorGroup service) {
			super(service);
		}

		@Override
		protected void dispatch(Object data, Subscriber subscriber, SignalType type) {
			route(data, subscriber, type);
		}

		@Override
		public boolean isExposedToOverflow(Bounded parentPublisher) {
			return false;
		}

		@Override
		public long getCapacity() {
			return Long.MAX_VALUE;
		}
	}

	private static abstract class SubscriberWrapper<T> extends BaseSubscriber<T> {

		@Override
		public void onError(Throwable t) {
			throw new UnsupportedOperationException("OnError has not been implemented", t);
		}
	}

	private static final class ConsumerSubscriber<T> extends SubscriberWrapper<T> {

		private final Consumer<? super T> consumer;

		public ConsumerSubscriber(Consumer<? super T> consumer) {
			this.consumer = consumer;
		}

		@Override
		public void onNext(T t) {
			consumer.accept(t);
		}

	}


	private static final class RunnableSubscriber extends SubscriberWrapper<Void> {

		private final Runnable runnable;

		public RunnableSubscriber(Runnable runnable) {
			this.runnable = runnable;
		}

		@Override
		public void onNext(Void t) {
			runnable.run();
		}

	}

	private class TaskSubscriber implements Subscriber<Task> {

		private final Consumer<Throwable> uncaughtExceptionHandler;
		private final Consumer<Void>      shutdownHandler;

		public TaskSubscriber(Consumer<Throwable> uncaughtExceptionHandler, Consumer<Void> shutdownHandler) {
			this.uncaughtExceptionHandler = uncaughtExceptionHandler;
			this.shutdownHandler = shutdownHandler;
		}

		@Override
		public void onSubscribe(Subscription s) {
			s.request(Long.MAX_VALUE);
		}

		@Override
		public void onNext(Task task) {
			routeTask(task);
			if (tailRecurser != null) {
				tailRecurser.consumeTasks();
			}
		}

		@Override
		public void onError(Throwable t) {
			Exceptions.throwIfFatal(t);
			if (uncaughtExceptionHandler != null) {
				uncaughtExceptionHandler.accept(t);
			}
			throw new UnsupportedOperationException("No error handler provided for this ProcessorGroup", t);
		}

		@Override
		public void onComplete() {
			if (shutdownHandler != null) {
				shutdownHandler.accept(null);
			}
		}

	}


	final static class SingleProcessorGroup<T> extends ProcessorGroup<T> {
		public SingleProcessorGroup(Supplier<? extends Processor<Task, Task>> processor, int concurrency,
		                            Consumer<Throwable>
		                              uncaughtExceptionHandler, Consumer<Void> shutdownHandler, boolean
		                              autoShutdown) {
			super(processor, concurrency, uncaughtExceptionHandler, shutdownHandler, autoShutdown);
		}
	}

	final static class PooledProcessorGroup<T> extends ProcessorGroup<T> {

		final ProcessorGroup[] processorGroups;

		volatile int index = 0;

		public PooledProcessorGroup(
		  Supplier<? extends Processor<Task, Task>> processor,
		  int concurrency, Consumer<Throwable>
			uncaughtExceptionHandler, Consumer<Void> shutdownHandler,
		  boolean autoShutdown) {
			super(null, concurrency, null, null, autoShutdown);

			processorGroups = new ProcessorGroup[concurrency];

			for (int i = 0; i < concurrency; i++) {
				processorGroups[i] = new ProcessorGroup<T>(processor, 1, uncaughtExceptionHandler,
				  shutdownHandler, autoShutdown) {
					@Override
					protected void decrementReference() {
						REF_COUNT.decrementAndGet(this);
						PooledProcessorGroup.this.decrementReference();
					}

					@Override
					protected void incrementReference() {
						REF_COUNT.incrementAndGet(this);
						PooledProcessorGroup.this.incrementReference();
					}
				};
			}
		}


		@Override
		public void shutdown() {
			for (ProcessorGroup processorGroup : processorGroups) {
				processorGroup.shutdown();
			}
		}

		@Override
		public boolean awaitAndShutdown(long timeout, TimeUnit timeUnit) {
			for (ProcessorGroup processorGroup : processorGroups) {
				if (!processorGroup.awaitAndShutdown(timeout, timeUnit)) return false;
			}
			return true;
		}

		@Override
		public void forceShutdown() {
			for (ProcessorGroup processorGroup : processorGroups) {
				processorGroup.forceShutdown();
			}
		}

		@Override
		public boolean alive() {
			for (ProcessorGroup processorGroup : processorGroups) {
				if (!processorGroup.alive()) return false;
			}
			return true;
		}

		@SuppressWarnings("unchecked")
		private ProcessorGroup<T> next() {
			int index = this.index++;
			if (index == Integer.MAX_VALUE) this.index -= Integer.MAX_VALUE;
			return (ProcessorGroup<T>) processorGroups[index % concurrency];
		}

		@Override
		public Executor executor() {
			return next().executor();
		}

		@Override
		public BiConsumer<T, Consumer<? super T>> dataDispatcher() {
			return next().dataDispatcher();
		}

		@Override
		public <V> BiConsumer<V, Consumer<? super V>> dataDispatcher(Class<V> clazz) {
			return next().dataDispatcher(clazz);
		}

		@Override
		public Processor<T, T> observeOn() {
			return next().observeOn();
		}

		@Override
		public Processor<T, T> publishOn() {
			return next().publishOn();
		}

		@Override
		public Consumer<Consumer<Void>> dispatcher() {
			return next().dispatcher();
		}

		@Override
		public Processor<T, T> get() {
			return next().get();
		}
	}
}
