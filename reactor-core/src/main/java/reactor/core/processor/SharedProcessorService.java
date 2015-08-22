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
import org.reactivestreams.Subscriber;
import org.reactivestreams.Subscription;
import reactor.core.error.CancelException;
import reactor.core.error.Exceptions;
import reactor.core.error.ReactorFatalException;
import reactor.core.error.SpecificationExceptions;
import reactor.core.processor.rb.MutableSignal;
import reactor.core.processor.rb.RingBufferSubscriberUtils;
import reactor.core.subscriber.BaseSubscriber;
import reactor.core.support.Recyclable;
import reactor.core.support.Resource;
import reactor.core.support.SignalType;
import reactor.core.support.internal.PlatformDependent;
import reactor.fn.BiConsumer;
import reactor.fn.Consumer;
import reactor.fn.Supplier;
import reactor.jarjar.com.lmax.disruptor.RingBuffer;

import java.util.ArrayList;
import java.util.concurrent.Executor;
import java.util.concurrent.TimeUnit;
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
 * can be used in combination with {@link SharedProcessorService#release(Object...)} to cleanly unregister and
 * eventually
 * shutdown when no more references use that service.
 *
 * @param <T> the default type (not enforced at runtime)
 * @author Anatoly Kadyshev
 * @author Stephane Maldini
 */
public final class SharedProcessorService<T> implements Supplier<Processor<T, T>>, Resource {

	/**
	 * @param name
	 * @param <E>
	 * @return
	 */
	public static <E> SharedProcessorService<E> async(String name) {
		return async(name, AsyncProcessor.MEDIUM_BUFFER_SIZE);
	}

	/**
	 * @param name
	 * @param bufferSize
	 * @param <E>
	 * @return
	 */
	public static <E> SharedProcessorService<E> async(String name,
	                                                  int bufferSize) {
		return async(name, bufferSize, null);
	}

	/**
	 * @param name
	 * @param bufferSize
	 * @param uncaughtExceptionHandler
	 * @param <E>
	 * @return
	 */
	public static <E> SharedProcessorService<E> async(String name,
	                                                  int bufferSize,
	                                                  Consumer<Throwable> uncaughtExceptionHandler) {
		return async(name, bufferSize, uncaughtExceptionHandler, null);
	}

	/**
	 * @param name
	 * @param bufferSize
	 * @param uncaughtExceptionHandler
	 * @param shutdownHandler
	 * @param <E>
	 * @return
	 */
	public static <E> SharedProcessorService<E> async(String name,
	                                                  int bufferSize,
	                                                  Consumer<Throwable> uncaughtExceptionHandler,
	                                                  Consumer<Void> shutdownHandler
	) {
		return async(name, bufferSize, uncaughtExceptionHandler, shutdownHandler, true);
	}

	/**
	 * @param name
	 * @param bufferSize
	 * @param uncaughtExceptionHandler
	 * @param shutdownHandler
	 * @param autoShutdown
	 * @param <E>
	 * @return
	 */
	public static <E> SharedProcessorService<E> async(String name,
	                                                  int bufferSize,
	                                                  Consumer<Throwable> uncaughtExceptionHandler,
	                                                  Consumer<Void> shutdownHandler,
	                                                  boolean autoShutdown) {
		final Processor<Task, Task> processor;

		if (PlatformDependent.hasUnsafe()) {
			processor = RingBufferProcessor.share(name, bufferSize, DEFAULT_TASK_PROVIDER);
		} else {
			processor = SimpleAsyncProcessor.create(name, bufferSize);
		}

		return wrap(processor, uncaughtExceptionHandler, shutdownHandler, autoShutdown);
	}


	/**
	 * @param name
	 * @param <E>
	 * @return
	 */
	public static <E> SharedProcessorService<E> work(String name) {
		return work(name, AsyncProcessor.MEDIUM_BUFFER_SIZE);
	}

	/**
	 * @param name
	 * @param bufferSize
	 * @param <E>
	 * @return
	 */
	public static <E> SharedProcessorService<E> work(String name,
	                                                 int bufferSize) {
		return work(name, bufferSize, Runtime.getRuntime().availableProcessors());
	}

	/**
	 * @param name
	 * @param bufferSize
	 * @param concurrency
	 * @param <E>
	 * @return
	 */
	public static <E> SharedProcessorService<E> work(String name,
	                                                 int bufferSize,
	                                                 int concurrency) {
		return work(name, bufferSize, concurrency, null, null, true);
	}

	/**
	 * @param name
	 * @param bufferSize
	 * @param concurrency
	 * @param uncaughtExceptionHandler
	 * @param <E>
	 * @return
	 */
	public static <E> SharedProcessorService<E> work(String name,
	                                                 int bufferSize,
	                                                 int concurrency,
	                                                 Consumer<Throwable> uncaughtExceptionHandler) {
		return work(name, bufferSize, concurrency, uncaughtExceptionHandler, null, true);
	}

	/**
	 * @param name
	 * @param bufferSize
	 * @param concurrency
	 * @param uncaughtExceptionHandler
	 * @param shutdownHandler
	 * @param <E>
	 * @return
	 */
	public static <E> SharedProcessorService<E> work(String name,
	                                                 int bufferSize,
	                                                 int concurrency,
	                                                 Consumer<Throwable> uncaughtExceptionHandler,
	                                                 Consumer<Void> shutdownHandler) {
		return work(name, bufferSize, concurrency, uncaughtExceptionHandler, shutdownHandler, true);
	}


	/**
	 * @param name
	 * @param bufferSize
	 * @param concurrency
	 * @param uncaughtExceptionHandler
	 * @param shutdownHandler
	 * @param autoShutdown
	 * @param <E>
	 * @return
	 */
	public static <E> SharedProcessorService<E> work(String name,
	                                                 int bufferSize,
	                                                 int concurrency,
	                                                 Consumer<Throwable> uncaughtExceptionHandler,
	                                                 Consumer<Void> shutdownHandler,
	                                                 boolean autoShutdown) {
		final Processor<Task, Task> processor;
		if (PlatformDependent.hasUnsafe()) {
			processor = RingBufferWorkProcessor.share(name, bufferSize);
		} else {
			processor = SimpleWorkProcessor.create(name, bufferSize);
		}

		SharedProcessorService<E> sharedProcessorService =
		  wrap(processor, uncaughtExceptionHandler, shutdownHandler, autoShutdown);

		//add extra concurrent subscribers
		for (int i = 1; i < concurrency; i++) {
			sharedProcessorService.createSubscriber(uncaughtExceptionHandler, shutdownHandler);
		}

		return sharedProcessorService;
	}

	/**
	 * @param <E>
	 * @return
	 */
	@SuppressWarnings("unchecked")
	public static <E> SharedProcessorService<E> sync() {
		return (SharedProcessorService<E>) SYNC_SERVICE;
	}

	/**
	 * @param p
	 * @param <E>
	 * @return
	 */
	public static <E> SharedProcessorService<E> wrap(Processor<Task, Task> p) {
		return wrap(p, null, null, true);
	}


	/**
	 * @param p
	 * @param autoShutdown
	 * @param <E>
	 * @return
	 */
	public static <E> SharedProcessorService<E> wrap(Processor<Task, Task> p,
	                                                 boolean autoShutdown) {
		return wrap(p, null, null, autoShutdown);
	}

	/**
	 * @param p
	 * @param uncaughtExceptionHandler
	 * @param autoShutdown
	 * @param <E>
	 * @return
	 */
	public static <E> SharedProcessorService<E> wrap(Processor<Task, Task> p,
	                                                 Consumer<Throwable> uncaughtExceptionHandler,
	                                                 boolean autoShutdown) {
		return wrap(p, uncaughtExceptionHandler, null, autoShutdown);
	}

	/**
	 * @param p
	 * @param uncaughtExceptionHandler
	 * @param shutdownHandler
	 * @param autoShutdown
	 * @param <E>
	 * @return
	 */
	public static <E> SharedProcessorService<E> wrap(Processor<Task, Task> p,
	                                                 Consumer<Throwable> uncaughtExceptionHandler,
	                                                 Consumer<Void> shutdownHandler,
	                                                 boolean autoShutdown) {
		return new SharedProcessorService<E>(p, uncaughtExceptionHandler, shutdownHandler, autoShutdown);
	}

	/**
	 * @param sharedProcessorReferences
	 * @return
	 */
	public static void release(Object... sharedProcessorReferences) {
		if (sharedProcessorReferences == null) return;

		for (Object sharedProcessorReference : sharedProcessorReferences) {
			if (sharedProcessorReference != null &&
			  Processor.class.isAssignableFrom(sharedProcessorReference.getClass())) {
				((Processor) sharedProcessorReference).onComplete();
			}
		}
	}


	/**
	 * INSTANCE STUFF *
	 */

	final private TailRecurser          tailRecurser;


	final private Processor<Task, Task> processor;
	final private boolean               autoShutdown;
	final ExecutorPoweredProcessor<Task, Task> managedProcessor;

	@SuppressWarnings("unused")
	private volatile int refCount = 0;

	private static final AtomicIntegerFieldUpdater<SharedProcessorService> REF_COUNT =
	  AtomicIntegerFieldUpdater
		.newUpdater(SharedProcessorService.class, "refCount");

	@Override
	public Processor<T, T> get() {
		return createBarrier();
	}

	/**
	 * @param clazz
	 * @param <V>
	 * @return
	 */
	@SuppressWarnings("unchecked")
	public <V extends T> Processor<V, V> directProcessor(Class<V> clazz) {
		return (Processor<V, V>) get();
	}

	/**
	 * @return
	 */
	public Consumer<Consumer<Void>> dispatcher() {
		if (processor == null) {
			return SYNC_DISPATCHER;
		}

		return createBarrier();
	}

	/**
	 * @param clazz
	 * @param <V>
	 * @return
	 */
	@SuppressWarnings("unchecked")
	public <V extends T> BiConsumer<V, Consumer<? super V>> dataDispatcher(Class<V> clazz) {
		if (processor == null) {
			return (BiConsumer<V, Consumer<? super V>>) SYNC_DATA_DISPATCHER;
		}

		return (BiConsumer<V, Consumer<? super V>>) createBarrier();
	}

	/**
	 * @return
	 */
	@SuppressWarnings("unchecked")
	public BiConsumer<T, Consumer<? super T>> dataDispatcher() {
		if (processor == null) {
			return (BiConsumer<T, Consumer<? super T>>) SYNC_DATA_DISPATCHER;
		}

		return createBarrier();
	}

	/**
	 * @return
	 */
	public Executor executor() {
		if (processor == null) {
			return SYNC_EXECUTOR;
		}

		return createBarrier();
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
	 * @return a Reference to the internal shared Processor
	 */
	public Processor<Task, Task> engine() {
		return processor;
	}

	/* INTERNAL */
	@SuppressWarnings("unchecked")
	static private void route(Object payload, Subscriber subscriber, SignalType type) {
		try {
			if (type == SignalType.NEXT) {
				subscriber.onNext(payload);
			} else if (type == SignalType.COMPLETE) {
				subscriber.onComplete();
			} else if (type == SignalType.SUBSCRIPTION) {
				subscriber.onSubscribe((Subscription) payload);
			} else {
				subscriber.onError((Throwable) payload);
			}
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
	private static final SharedProcessorService SYNC_SERVICE = new SharedProcessorService(null, null, null, false);

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

	private final static Supplier<Task> DEFAULT_TASK_PROVIDER = new Supplier<Task>() {
		@Override
		public Task get() {
			return new Task();
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
	private SharedProcessorService(
	  Processor<Task, Task> processor,
	  Consumer<Throwable> uncaughtExceptionHandler,
	  Consumer<Void> shutdownHandler,
	  boolean autoShutdown
	) {
		this.processor = processor;
		this.autoShutdown = autoShutdown;

		if (processor != null) {
			// Managed Processor, providing for tail recursion,
			if (ExecutorPoweredProcessor.class.isAssignableFrom(processor.getClass())) {
				int bufferSize = (int) Math.min(((ExecutorPoweredProcessor) processor).getCapacity(), MAX_BUFFER_SIZE);

				this.tailRecurser = new TailRecurser(
				  bufferSize,
				  DEFAULT_TASK_PROVIDER,
				  DEFAULT_TASK_CONSUMER
				);

				this.managedProcessor = (ExecutorPoweredProcessor<Task, Task>) processor;
			} else {
				this.managedProcessor = null;
				this.tailRecurser = null;
			}

			processor.subscribe(createSubscriber(uncaughtExceptionHandler, shutdownHandler));

		} else {
			this.managedProcessor = null;
			this.tailRecurser = null;
		}
	}

	private Subscriber<Task> createSubscriber(
	  final Consumer<Throwable> uncaughtExceptionHandler,
	  final Consumer<Void> shutdownHandler
	) {
		return new Subscriber<Task>() {

			@Override
			public void onSubscribe(Subscription s) {
				s.request(Long.MAX_VALUE);
			}

			@Override
			public void onNext(Task task) {
				routeTask(task);
				tailRecurser.consumeTasks();
			}

			@Override
			public void onError(Throwable t) {
				Exceptions.throwIfFatal(t);
				if (uncaughtExceptionHandler != null) {
					uncaughtExceptionHandler.accept(t);
				}
			}

			@Override
			public void onComplete() {
				if (shutdownHandler != null) {
					shutdownHandler.accept(null);
				}
			}

		};
	}

	@SuppressWarnings("unchecked")
	private ProcessorBarrier<T> createBarrier() {

		REF_COUNT.incrementAndGet(this);

		if (RingBufferProcessor.class == processor.getClass()) {
			return new RingBufferProcessorBarrier<>(((RingBufferProcessor) processor).ringBuffer());
		} else if (ExecutorPoweredProcessor.class.isAssignableFrom(processor.getClass())
		  && ((ExecutorPoweredProcessor) processor).isWork()) {
			return new WorkProcessorBarrier<>();
		}
		return new ProcessorBarrier<>();
	}

	/**
	 *
	 */
	static class TailRecurser {

		private final ArrayList<Task> pile;

		private final int pileSizeIncrement;

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

	static final class Task implements Recyclable {
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

	private class ProcessorBarrier<V> extends BaseSubscriber<V> implements
	  Consumer<Consumer<Void>>,
	  BiConsumer<V, Consumer<? super V>>,
	  Processor<V, V>,
	  Executor {

		Subscription          subscription;
		Subscriber<? super V> subscriber;

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
				s.onError(new IllegalStateException("Shared Processors do not support multi-subscribe"));
			} else if (subscribed) {
				dispatch(subscription, s, SignalType.SUBSCRIPTION);
			}

		}

		@Override
		public final void onSubscribe(Subscription s) {
			super.onSubscribe(s);

			final boolean set, subscribed;
			synchronized (this) {
				if (subscription == null) {
					subscription = s;
					set = true;
				} else {
					set = false;
				}
				subscribed = this.subscriber != null;
			}

			if (!set) {
				s.cancel();
			} else if (subscribed) {
				dispatch(s, subscriber, SignalType.SUBSCRIPTION);
			}
		}

		@Override
		public final void onNext(V o) {
			super.onNext(o);

			if (subscriber == null) {
				throw CancelException.get();
			}

			dispatchProcessorSequence(o, subscriber, SignalType.NEXT);
		}

		@Override
		public final void onError(Throwable t) {
			super.onError(t);

			if (subscriber == null) {
				throw ReactorFatalException.create(t);
			}

			dispatchProcessorSequence(t, subscriber, SignalType.ERROR);
		}

		@Override
		public final void onComplete() {
			REF_COUNT.decrementAndGet(SharedProcessorService.this);

			if (subscriber == null) {
				throw CancelException.get();
			}

			dispatchProcessorSequence(null, subscriber, SignalType.COMPLETE);
		}

		protected void dispatchProcessorSequence(Object data, Subscriber subscriber, SignalType type) {
			dispatch(data, subscriber, type);
		}

		protected void dispatch(Object data, Subscriber subscriber, SignalType type) {
			final Task task;
			if (managedProcessor != null && managedProcessor.isInContext()) {
				task = tailRecurser.next();
				task.type = type;
				task.payload = data;
				task.subscriber = subscriber;
			} else {
				task = new Task();
				task.type = type;
				task.payload = data;
				task.subscriber = subscriber;
				processor.onNext(task);
			}
		}
	}

	private final class RingBufferProcessorBarrier<V> extends ProcessorBarrier<V> {

		private final RingBuffer<MutableSignal<Task>> ringBuffer;

		public RingBufferProcessorBarrier(RingBuffer<MutableSignal<Task>> ringBuffer) {
			this.ringBuffer = ringBuffer;
		}

		@Override
		protected void dispatch(Object data, Subscriber subscriber, SignalType type) {
			final Task task;
			if (managedProcessor != null && managedProcessor.isInContext()) {
				task = tailRecurser.next();
				task.type = type;
				task.payload = data;
				task.subscriber = subscriber;
			} else {
				MutableSignal<Task> signal = RingBufferSubscriberUtils.next(ringBuffer);
				task = signal.value;
				task.type = type;
				task.payload = data;
				task.subscriber = subscriber;

				RingBufferSubscriberUtils.publish(ringBuffer, signal);
			}
		}

	}

	private final class WorkProcessorBarrier<V> extends ProcessorBarrier<V> {

		@Override
		protected void dispatchProcessorSequence(Object data, Subscriber subscriber, SignalType type) {
			route(data, subscriber, type);
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
}
