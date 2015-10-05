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
package reactor;

import org.reactivestreams.Processor;
import org.reactivestreams.Publisher;
import org.reactivestreams.Subscriber;
import org.reactivestreams.Subscription;
import reactor.core.error.Exceptions;
import reactor.core.processor.*;
import reactor.core.processor.simple.SimpleSignal;
import reactor.core.publisher.LogOperator;
import reactor.core.subscriber.BlockingQueueSubscriber;
import reactor.core.support.Assert;
import reactor.core.support.Bounded;
import reactor.core.support.Publishable;
import reactor.core.support.Subscribable;
import reactor.core.support.internal.MpscLinkedQueue;
import reactor.core.support.internal.PlatformDependent;
import reactor.fn.Consumer;
import reactor.fn.Function;
import reactor.fn.Supplier;

import java.util.Queue;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;

/**
 * Main gateway to build various asynchronous {@link Processor} or "pool" services that allow their reuse.
 * Reactor offers a few management API via the subclassed {@link BaseProcessor} for the underlying {@link
 * java.util.concurrent.Executor} in use.
 *
 * @author Stephane Maldini
 * @since 2.1
 */
public final class Processors {

	/**
	 * Default number of processors available to the runtime on init (min 2)
	 *
	 * @see Runtime#availableProcessors()
	 */
	public static final int DEFAULT_POOL_SIZE = Math.max(Runtime.getRuntime().availableProcessors(), 2);

	/**
	 * Create a new {@link BaseProcessor} using {@link BaseProcessor#SMALL_BUFFER_SIZE} backlog size,
	 * blockingWait Strategy
	 * and auto-cancel.
	 * <p>
	 * A Shared Processor authorizes concurrent onNext calls and is suited for multi-threaded publisher that
	 * will fan-in data.
	 * <p>
	 * A new Cached ThreadExecutorPool will be implicitely created.
	 *
	 * @param <E> Type of processed signals
	 * @return a fresh processor
	 */
	public static <E> BaseProcessor<E, E> topic() {
		return topic("async", BaseProcessor.SMALL_BUFFER_SIZE, true);
	}

	/**
	 * Create a new {@link BaseProcessor} using {@link BaseProcessor#SMALL_BUFFER_SIZE} backlog size,
	 * blockingWait Strategy
	 * and the passed auto-cancel setting.
	 * <p>
	 * A Shared Processor authorizes concurrent onNext calls and is suited for multi-threaded publisher that
	 * will fan-in data.
	 * <p>
	 * A new Cached ThreadExecutorPool will be implicitely created.
	 *
	 * @param autoCancel Should this propagate cancellation when unregistered by all subscribers ?
	 * @param <E>        Type of processed signals
	 * @return a fresh processor
	 */
	public static <E> BaseProcessor<E, E> topic(boolean autoCancel) {
		return topic(Processors.class.getSimpleName(), BaseProcessor.SMALL_BUFFER_SIZE, autoCancel);
	}

	/**
	 * Create a new {@link BaseProcessor} using {@link BaseProcessor#SMALL_BUFFER_SIZE} backlog size,
	 * blockingWait Strategy
	 * and the passed auto-cancel setting.
	 * <p>
	 * A Shared Processor authorizes concurrent onNext calls and is suited for multi-threaded publisher that
	 * will fan-in data.
	 * <p>
	 * A new Cached ThreadExecutorPool will be implicitely created and will use the passed name to qualify
	 * the created threads.
	 *
	 * @param name       Use a new Cached ExecutorService and assign this name to the created threads
	 * @param bufferSize A Backlog Size to mitigate slow subscribers
	 * @param <E>        Type of processed signals
	 * @return a fresh processor
	 */
	public static <E> BaseProcessor<E, E> topic(String name, int bufferSize) {
		return topic(name, bufferSize, true);
	}

	/**
	 * Create a new {@link BaseProcessor} using the blockingWait Strategy, passed backlog size,
	 * and auto-cancel settings.
	 * <p>
	 * A Shared Processor authorizes concurrent onNext calls and is suited for multi-threaded publisher that
	 * will fan-in data.
	 * <p>
	 * The passed {@link java.util.concurrent.ExecutorService} will execute as many event-loop
	 * consuming the ringbuffer as subscribers.
	 *
	 * @param name       Use a new Cached ExecutorService and assign this name to the created threads
	 * @param bufferSize A Backlog Size to mitigate slow subscribers
	 * @param autoCancel Should this propagate cancellation when unregistered by all subscribers ?
	 * @param <E>        Type of processed signals
	 * @return a fresh processor
	 */
	public static <E> BaseProcessor<E, E> topic(String name, int bufferSize, boolean autoCancel) {
		final BaseProcessor<E, E> processor;

		if (PlatformDependent.hasUnsafe()) {
			processor = RingBufferProcessor.create(name, bufferSize, autoCancel);
		} else {
			throw new UnsupportedOperationException("Pub-Sub async processor not yet supported without Unsafe");
			//			processor = SimpleWorkProcessor.create(name, bufferSize);
		}
		return processor;
	}

	/**
	 * Create a new {@link BaseProcessor} using {@link BaseProcessor#SMALL_BUFFER_SIZE} backlog size,
	 * blockingWait Strategy
	 * and auto-cancel.
	 * <p>
	 * A Shared Processor authorizes concurrent onNext calls and is suited for multi-threaded publisher that
	 * will fan-in data.
	 * <p>
	 * A new Cached ThreadExecutorPool will be implicitely created.
	 *
	 * @param <E> Type of processed signals
	 * @return a fresh processor
	 */
	public static <E> BaseProcessor<E, E> queue() {
		return queue("worker", BaseProcessor.SMALL_BUFFER_SIZE, true);
	}

	/**
	 * Create a new {@link BaseProcessor} using {@link BaseProcessor#SMALL_BUFFER_SIZE} backlog size,
	 * blockingWait Strategy
	 * and the passed auto-cancel setting.
	 * <p>
	 * A Shared Processor authorizes concurrent onNext calls and is suited for multi-threaded publisher that
	 * will fan-in data.
	 * <p>
	 * A new Cached ThreadExecutorPool will be implicitely created.
	 *
	 * @param autoCancel Should this propagate cancellation when unregistered by all subscribers ?
	 * @param <E>        Type of processed signals
	 * @return a fresh processor
	 */
	public static <E> BaseProcessor<E, E> queue(boolean autoCancel) {
		return queue(Processors.class.getSimpleName(), BaseProcessor.SMALL_BUFFER_SIZE, autoCancel);
	}

	/**
	 * Create a new {@link BaseProcessor} using {@link BaseProcessor#SMALL_BUFFER_SIZE} backlog size,
	 * blockingWait Strategy
	 * and the passed auto-cancel setting.
	 * <p>
	 * A Shared Processor authorizes concurrent onNext calls and is suited for multi-threaded publisher that
	 * will fan-in data.
	 * <p>
	 * A new Cached ThreadExecutorPool will be implicitely created and will use the passed name to qualify
	 * the created threads.
	 *
	 * @param name       Use a new Cached ExecutorService and assign this name to the created threads
	 * @param bufferSize A Backlog Size to mitigate slow subscribers
	 * @param <E>        Type of processed signals
	 * @return a fresh processor
	 */
	public static <E> BaseProcessor<E, E> queue(String name, int bufferSize) {
		return queue(name, bufferSize, true);
	}

	/**
	 * Create a new {@link BaseProcessor} using the passed buffer size
	 * and auto-cancel settings.
	 * <p>
	 * A new Cached ThreadExecutorPool will be implicitely created and will use the passed name to qualify
	 * the created threads.
	 *
	 * @param name       Use a new Cached ExecutorService and assign this name to the created threads
	 * @param bufferSize A Backlog Size to mitigate slow subscribers
	 * @param autoCancel Should this propagate cancellation when unregistered by all subscribers ?
	 * @param <E>        Type of processed signals
	 * @return a fresh processor
	 */
	public static <E> BaseProcessor<E, E> queue(String name, int bufferSize, boolean autoCancel) {
		final BaseProcessor<E, E> processor;

		if (PlatformDependent.hasUnsafe()) {
			processor = RingBufferWorkProcessor.create(name, bufferSize, autoCancel);
		} else {
			processor = SimpleWorkProcessor.create(name, bufferSize, autoCancel);
		}
		return processor;
	}

	/**
	 * @param <E>
	 * @return
	 */
	public static <E> ProcessorGroup<E> asyncGroup() {
		return asyncGroup(null, BaseProcessor.MEDIUM_BUFFER_SIZE);
	}

	/**
	 * @param name
	 * @param <E>
	 * @return
	 */
	public static <E> ProcessorGroup<E> asyncGroup(String name) {
		return asyncGroup(name, BaseProcessor.MEDIUM_BUFFER_SIZE);
	}

	/**
	 * @param name
	 * @param bufferSize
	 * @param <E>
	 * @return
	 */
	public static <E> ProcessorGroup<E> asyncGroup(String name,
	                                               int bufferSize) {
		return asyncGroup(name, bufferSize, null);
	}

	/**
	 * @param name
	 * @param bufferSize
	 * @param <E>
	 * @return
	 */
	public static <E> ProcessorGroup<E> asyncGroup(String name,
	                                               int bufferSize,
	                                               int concurrency) {
		return asyncGroup(name, bufferSize, concurrency, null);
	}

	/**
	 * @param name
	 * @param bufferSize
	 * @param uncaughtExceptionHandler
	 * @param <E>
	 * @return
	 */
	public static <E> ProcessorGroup<E> asyncGroup(String name,
	                                               int bufferSize,
	                                               Consumer<Throwable> uncaughtExceptionHandler) {
		return asyncGroup(name, bufferSize, uncaughtExceptionHandler, null);
	}


	/**
	 * @param name
	 * @param bufferSize
	 * @param uncaughtExceptionHandler
	 * @param <E>
	 * @return
	 */
	public static <E> ProcessorGroup<E> asyncGroup(String name,
	                                               int bufferSize,
	                                               int concurrency,
	                                               Consumer<Throwable> uncaughtExceptionHandler) {
		return asyncGroup(name, bufferSize, concurrency, uncaughtExceptionHandler, null);
	}

	/**
	 * @param name
	 * @param bufferSize
	 * @param uncaughtExceptionHandler
	 * @param shutdownHandler
	 * @param <E>
	 * @return
	 */
	public static <E> ProcessorGroup<E> asyncGroup(String name,
	                                               int bufferSize,
	                                               Consumer<Throwable> uncaughtExceptionHandler,
	                                               Consumer<Void> shutdownHandler
	) {
		return asyncGroup(name, bufferSize, uncaughtExceptionHandler, shutdownHandler, true);
	}


	/**
	 * @param name
	 * @param bufferSize
	 * @param uncaughtExceptionHandler
	 * @param shutdownHandler
	 * @param <E>
	 * @return
	 */
	public static <E> ProcessorGroup<E> asyncGroup(String name,
	                                               int bufferSize,
	                                               int concurrency,
	                                               Consumer<Throwable> uncaughtExceptionHandler,
	                                               Consumer<Void> shutdownHandler
	) {
		return asyncGroup(name, bufferSize, concurrency, uncaughtExceptionHandler, shutdownHandler, true);
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
	public static <E> ProcessorGroup<E> asyncGroup(String name,
	                                               int bufferSize,
	                                               Consumer<Throwable> uncaughtExceptionHandler,
	                                               Consumer<Void> shutdownHandler,
	                                               boolean autoShutdown) {
		return asyncGroup(name, bufferSize, 1, uncaughtExceptionHandler, shutdownHandler, autoShutdown);
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
	public static <E> ProcessorGroup<E> asyncGroup(final String name,
	                                               final int bufferSize,
	                                               int concurrency,
	                                               Consumer<Throwable> uncaughtExceptionHandler,
	                                               Consumer<Void> shutdownHandler,
	                                               boolean autoShutdown) {

		return ProcessorGroup.create(
		  new Supplier<Processor<ProcessorGroup.Task, ProcessorGroup.Task>>() {
			  @Override
			  public Processor<ProcessorGroup.Task, ProcessorGroup.Task> get() {
				  return PlatformDependent.hasUnsafe()
					? RingBufferProcessor.share(name, bufferSize, ProcessorGroup.DEFAULT_TASK_PROVIDER)
					: SimpleWorkProcessor.create(name, bufferSize, MpscLinkedQueue.<SimpleSignal<ProcessorGroup
					.Task>>create
					());
			  }
		  },
		  concurrency,
		  uncaughtExceptionHandler,
		  shutdownHandler,
		  autoShutdown
		);
	}

	/**
	 * @param name
	 * @param <E>
	 * @return
	 */
	public static <E> ProcessorGroup<E> ioGroup(String name) {
		return ioGroup(name, BaseProcessor.MEDIUM_BUFFER_SIZE);
	}

	/**
	 * @param name
	 * @param bufferSize
	 * @param <E>
	 * @return
	 */
	public static <E> ProcessorGroup<E> ioGroup(String name,
	                                            int bufferSize) {
		return ioGroup(name, bufferSize, DEFAULT_POOL_SIZE);
	}


	/**
	 * @param name
	 * @param bufferSize
	 * @param concurrency
	 * @param <E>
	 * @return
	 */
	public static <E> ProcessorGroup<E> ioGroup(String name,
	                                            int bufferSize,
	                                            int concurrency) {
		return ioGroup(name, bufferSize, concurrency, null, null, true);
	}


	/**
	 * @param name
	 * @param bufferSize
	 * @param concurrency
	 * @param uncaughtExceptionHandler
	 * @param <E>
	 * @return
	 */
	public static <E> ProcessorGroup<E> ioGroup(String name,
	                                            int bufferSize,
	                                            int concurrency,
	                                            Consumer<Throwable> uncaughtExceptionHandler) {
		return ioGroup(name, bufferSize, concurrency, uncaughtExceptionHandler, null, true);
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
	public static <E> ProcessorGroup<E> ioGroup(String name,
	                                            int bufferSize,
	                                            int concurrency,
	                                            Consumer<Throwable> uncaughtExceptionHandler,
	                                            Consumer<Void> shutdownHandler) {
		return ioGroup(name, bufferSize, concurrency, uncaughtExceptionHandler, shutdownHandler, true);
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
	public static <E> ProcessorGroup<E> ioGroup(final String name,
	                                            final int bufferSize,
	                                            int concurrency,
	                                            Consumer<Throwable> uncaughtExceptionHandler,
	                                            Consumer<Void> shutdownHandler,
	                                            boolean autoShutdown) {
		return ProcessorGroup.create(
		  PlatformDependent.hasUnsafe()
			? RingBufferWorkProcessor.<ProcessorGroup.Task>share(name, bufferSize)
			: SimpleWorkProcessor.<ProcessorGroup.Task>create(name, bufferSize),
		  concurrency,
		  uncaughtExceptionHandler,
		  shutdownHandler,
		  autoShutdown
		);
	}

	/**
	 *
	 * @param map
	 * @param <T>
	 * @param <V>
	 * @return
	 */
	public static <T, V> Processor<T, V> flatMap(Function<? super T, ? extends Publisher<? extends V>> map){
		return flatMap(map, BaseProcessor.SMALL_BUFFER_SIZE, BaseProcessor.SMALL_BUFFER_SIZE * 2);
	}


	/**
	 *
	 * @param map
	 * @param <T>
	 * @param <V>
	 * @return
	 */
	public static <T, V> Processor<T, V> flatMap(Function<? super T, ? extends Publisher<? extends V>> map,
	                                             int maxConcurrency){
		return flatMap(map, maxConcurrency, maxConcurrency);
	}


	/**
	 *
	 * @param map
	 * @param <T>
	 * @param <V>
	 * @return
	 */
	public static <T, V> Processor<T, V> flatMap(Function<? super T, ? extends Publisher<? extends V>> map,
	                                             int maxConcurrency, int bufferSize){
		return new FlatMapProcessor<>(map, maxConcurrency, bufferSize);
	}

	/**
	 *
	 * @param map
	 * @param <T>
	 * @param <V>
	 * @return
	 */
	public static <T, V> Processor<T, V> concatMap(Function<? super T, ? extends Publisher<? extends V>> map){
		return concatMap(map, BaseProcessor.SMALL_BUFFER_SIZE);
	}


	/**
	 *
	 * @param map
	 * @param <T>
	 * @param <V>
	 * @return
	 */
	public static <T, V> Processor<T, V> concatMap(Function<? super T, ? extends Publisher<? extends V>> map,
	                                             int bufferSize){
		return flatMap(map, 1, bufferSize);
	}

	/**
	 * @param processor
	 * @param <IN>
	 * @param <OUT>
	 * @return
	 */
	public static <IN, OUT> Processor<IN, OUT> log(Processor<IN, OUT> processor) {
		return log(processor, null);
	}

	/**
	 * @param processor
	 * @param category
	 * @param <IN>
	 * @param <OUT>
	 * @return
	 */
	public static <IN, OUT> Processor<IN, OUT> log(final Processor<IN, OUT> processor, final String category) {
		return lift(processor, new Function<Processor<IN, OUT>, Publisher<OUT>>() {
			@Override
			public Publisher<OUT> apply(Processor<IN, OUT> processor) {
				return Publishers.lift(processor, new LogOperator<OUT>(category));
			}
		});
	}

	/**
	 * @param <IN>
	 * @return
	 */
	public static <IN> BlockingQueue<IN> toQueue(Processor<IN, IN> source) {
		return toQueue(source, BaseProcessor.SMALL_BUFFER_SIZE);
	}

	/**
	 * @param <IN>
	 * @return
	 */
	public static <IN> BlockingQueue<IN> toQueue(Processor<IN, IN> source, int size) {
		return toQueue(source, size, new ArrayBlockingQueue<IN>(size));
	}

	/**
	 * @param <IN>
	 * @return
	 */
	public static <IN> BlockingQueue<IN> toQueue(Processor<IN, IN> source, int size, Queue<IN> store) {
		return new BlockingQueueSubscriber<>(source, source, store, size);
	}

	/**
	 * @param <IN>
	 * @param <OUT>
	 * @return
	 */
	public static <IN, OUT> Processor<IN, OUT> create(final Subscriber<IN> upstream, final Publisher<OUT> downstream) {
		Assert.notNull(upstream, "Upstream must not be null");
		Assert.notNull(downstream, "Downstream must not be null");
		return new DelegateProcessor<>(downstream, upstream);
	}


	/**
	 * @param processor
	 * @param liftTransformation
	 * @param <IN>
	 * @param <OUT>
	 * @return
	 */
	public static <IN, OUT> Processor<IN, OUT> lift(
	  final Processor<IN, OUT> processor,
	  final Function<? super Processor<IN, OUT>, ? extends Publisher<OUT>> liftTransformation) {
		return new LiftProcessor<>(liftTransformation, processor);

	}

	private static class LiftProcessor<IN, OUT> implements Processor<IN, OUT> {
		private final Function<? super Processor<IN, OUT>, ? extends Publisher<OUT>> liftTransformation;
		private final Processor<IN, OUT>                                             processor;

		public LiftProcessor(Function<? super Processor<IN, OUT>, ? extends Publisher<OUT>> liftTransformation,
		                     Processor<IN, OUT> processor) {
			this.liftTransformation = liftTransformation;
			this.processor = processor;
		}

		@Override
		public void subscribe(Subscriber<? super OUT> s) {
			try {
				liftTransformation.apply(processor).subscribe(s);
			} catch (Throwable t) {
				Exceptions.<OUT>publisher(t).subscribe(s);
			}
		}

		@Override
		public void onSubscribe(Subscription s) {
			processor.onSubscribe(s);
		}

		@Override
		public void onNext(IN in) {
			processor.onNext(in);
		}

		@Override
		public void onError(Throwable t) {
			processor.onError(t);
		}

		@Override
		public void onComplete() {
			processor.onComplete();
		}
	}

	private static class DelegateProcessor<IN, OUT> implements Processor<IN, OUT>, Publishable<OUT>, Subscribable<IN>, Bounded {
		private final Publisher<OUT> downstream;
		private final Subscriber<IN> upstream;

		public DelegateProcessor(Publisher<OUT> downstream, Subscriber<IN> upstream) {
			this.downstream = downstream;
			this.upstream = upstream;
		}

		@Override
		public Publisher<OUT> upstream() {
			return downstream;
		}

		@Override
		public Subscriber<? super IN> downstream() {
			return upstream;
		}

		@Override
		public void subscribe(Subscriber<? super OUT> s) {
			downstream.subscribe(s);
		}

		@Override
		public void onSubscribe(Subscription s) {
			upstream.onSubscribe(s);
		}

		@Override
		public void onNext(IN in) {
			upstream.onNext(in);
		}

		@Override
		public void onError(Throwable t) {
			upstream.onError(t);
		}

		@Override
		public void onComplete() {
			upstream.onComplete();
		}

		@Override
		public boolean isExposedToOverflow(Bounded parentPublisher) {
			return Bounded.class.isAssignableFrom(upstream.getClass()) && ((Bounded)upstream).isExposedToOverflow(parentPublisher);
		}

		@Override
		public long getCapacity() {
			return Bounded.class.isAssignableFrom(upstream.getClass()) ? ((Bounded)upstream).getCapacity() : Long.MAX_VALUE;
		}
	}

}
