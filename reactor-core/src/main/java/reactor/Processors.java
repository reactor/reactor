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

package reactor;

import java.util.Queue;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.TimeUnit;
import java.util.logging.Level;

import org.reactivestreams.Processor;
import org.reactivestreams.Publisher;
import org.reactivestreams.Subscriber;
import org.reactivestreams.Subscription;
import reactor.core.processor.BaseProcessor;
import reactor.core.processor.EmitterProcessor;
import reactor.core.processor.ExecutorProcessor;
import reactor.core.processor.ProcessorGroup;
import reactor.core.processor.RingBufferProcessor;
import reactor.core.processor.RingBufferWorkProcessor;
import reactor.core.publisher.PublisherLog;
import reactor.core.subscriber.BlockingQueueSubscriber;
import reactor.core.support.Assert;
import reactor.core.support.ReactiveState;
import reactor.core.support.WaitStrategy;
import reactor.fn.Consumer;
import reactor.fn.Function;
import reactor.fn.Supplier;

/**
 * Main gateway to build various asynchronous {@link Processor} or "pool" services that allow their reuse. Reactor
 * offers a few management API via the subclassed {@link BaseProcessor} for the underlying {@link
 * java.util.concurrent.Executor} in use.
 * @author Stephane Maldini
 * @since 2.5
 */
public final class Processors {

	private Processors() {
	}

	/**
	 * Default number of processors available to the runtime on init (min 4)
	 * @see Runtime#availableProcessors()
	 */
	public static final int DEFAULT_POOL_SIZE = Math.max(Runtime.getRuntime()
	                                                            .availableProcessors(), 4);

	/**
	 *
	 * Non-Blocking "Synchronous" Pub-Sub
	 *
	 *
	 */

	/**
	 * Create a new {@link BaseProcessor} using {@link BaseProcessor#SMALL_BUFFER_SIZE} backlog size, blockingWait
	 * Strategy and auto-cancel. <p>
	 * @param <E> Type of processed signals
	 * @return a fresh processor
	 */
	public static <E> BaseProcessor<E, E> emitter() {
		return emitter(true);
	}

	/**
	 * Create a new {@link BaseProcessor} using {@link BaseProcessor#SMALL_BUFFER_SIZE} backlog size, blockingWait
	 * Strategy and auto-cancel. <p>
	 * @param <E> Type of processed signals
	 * @return a fresh processor
	 */
	public static <E> BaseProcessor<E, E> emitter(boolean autoCancel) {
		return emitter(BaseProcessor.SMALL_BUFFER_SIZE, autoCancel);
	}

	/**
	 * Create a new {@link BaseProcessor} using {@link BaseProcessor#SMALL_BUFFER_SIZE} backlog size, blockingWait
	 * Strategy and auto-cancel. <p>
	 * @param <E> Type of processed signals
	 * @return a fresh processor
	 */
	public static <E> BaseProcessor<E, E> emitter(int bufferSize) {
		return emitter(bufferSize, Integer.MAX_VALUE);
	}

	/**
	 * Create a new {@link BaseProcessor} using {@link BaseProcessor#SMALL_BUFFER_SIZE} backlog size, blockingWait
	 * Strategy and auto-cancel. <p>
	 * @param <E> Type of processed signals
	 * @return a fresh processor
	 */
	public static <E> BaseProcessor<E, E> emitter(int bufferSize, int concurrency) {
		return emitter(bufferSize, concurrency, true);
	}

	/**
	 * Create a new {@link BaseProcessor} using {@link BaseProcessor#SMALL_BUFFER_SIZE} backlog size, blockingWait
	 * Strategy and auto-cancel. <p>
	 * @param <E> Type of processed signals
	 * @return a fresh processor
	 */
	public static <E> BaseProcessor<E, E> emitter(int bufferSize, boolean autoCancel) {
		return emitter(bufferSize, Integer.MAX_VALUE, autoCancel);
	}

	/**
	 * Create a new {@link BaseProcessor} using {@link BaseProcessor#SMALL_BUFFER_SIZE} backlog size, blockingWait
	 * Strategy and auto-cancel. <p>
	 * @param <E> Type of processed signals
	 * @return a fresh processor
	 */
	public static <E> BaseProcessor<E, E> emitter(int bufferSize, int concurrency, boolean autoCancel) {
		return new EmitterProcessor<>(autoCancel, concurrency, bufferSize, -1);
	}

	/**
	 * Create a new {@link BaseProcessor} using {@link BaseProcessor#SMALL_BUFFER_SIZE} backlog size, blockingWait
	 * Strategy and auto-cancel. <p>
	 * @param <E> Type of processed signals
	 * @return a fresh processor
	 */
	public static <E> BaseProcessor<E, E> replay() {
		return replay(BaseProcessor.SMALL_BUFFER_SIZE);
	}

	/**
	 * Create a new {@link BaseProcessor} using {@link BaseProcessor#SMALL_BUFFER_SIZE} backlog size, blockingWait
	 * Strategy and auto-cancel. <p>
	 * @param <E> Type of processed signals
	 * @return a fresh processor
	 */
	public static <E> BaseProcessor<E, E> replay(int historySize) {
		return replay(historySize, Integer.MAX_VALUE);
	}

	/**
	 * Create a new {@link BaseProcessor} using {@link BaseProcessor#SMALL_BUFFER_SIZE} backlog size, blockingWait
	 * Strategy and auto-cancel. <p>
	 * @param <E> Type of processed signals
	 * @return a fresh processor
	 */
	public static <E> BaseProcessor<E, E> replay(int historySize, int concurrency) {
		return replay(historySize, concurrency, false);
	}

	/**
	 * Create a new {@link BaseProcessor} using {@link BaseProcessor#SMALL_BUFFER_SIZE} backlog size, blockingWait
	 * Strategy and auto-cancel. <p>
	 * @param <E> Type of processed signals
	 * @return a fresh processor
	 */
	public static <E> BaseProcessor<E, E> replay(int historySize, int concurrency, boolean autoCancel) {
		return new EmitterProcessor<>(autoCancel, concurrency, historySize, historySize);
	}

	/**
	 *
	 * Non-Blocking "Asynchronous" Dedicated Pub-Sub (1 Thread by Sub)
	 *
	 *
	 */

	/**
	 * Create a new {@link BaseProcessor} using {@link BaseProcessor#SMALL_BUFFER_SIZE} backlog size, blockingWait
	 * Strategy and auto-cancel. <p> A new Cached ThreadExecutorPool will be implicitely created.
	 * @param <E> Type of processed signals
	 * @return a fresh processor
	 */
	public static <E> ExecutorProcessor<E, E> topic() {
		return topic("async", BaseProcessor.SMALL_BUFFER_SIZE, true);
	}

	/**
	 * Create a new {@link BaseProcessor} using {@link BaseProcessor#SMALL_BUFFER_SIZE} backlog size, blockingWait
	 * Strategy and auto-cancel. <p> A new Cached ThreadExecutorPool will be implicitely created.
	 * @param <E> Type of processed signals
	 * @return a fresh processor
	 */
	public static <E> ExecutorProcessor<E, E> topic(String name) {
		return topic(name, BaseProcessor.SMALL_BUFFER_SIZE, true);
	}

	/**
	 * Create a new {@link BaseProcessor} using {@link BaseProcessor#SMALL_BUFFER_SIZE} backlog size, blockingWait
	 * Strategy and the passed auto-cancel setting. <p> A Shared Processor authorizes concurrent onNext calls and is
	 * suited for multi-threaded publisher that will fan-in data. <p> A new Cached ThreadExecutorPool will be
	 * implicitely created.
	 * @param autoCancel Should this propagate cancellation when unregistered by all subscribers ?
	 * @param <E> Type of processed signals
	 * @return a fresh processor
	 */
	public static <E> ExecutorProcessor<E, E> topic(boolean autoCancel) {
		return topic(Processors.class.getSimpleName(), BaseProcessor.SMALL_BUFFER_SIZE, autoCancel);
	}

	/**
	 * Create a new {@link BaseProcessor} using {@link BaseProcessor#SMALL_BUFFER_SIZE} backlog size, blockingWait
	 * Strategy and the passed auto-cancel setting. <p> A Shared Processor authorizes concurrent onNext calls and is
	 * suited for multi-threaded publisher that will fan-in data. <p> A new Cached ThreadExecutorPool will be
	 * implicitely created and will use the passed name to qualify the created threads.
	 * @param name Use a new Cached ExecutorService and assign this name to the created threads
	 * @param bufferSize A Backlog Size to mitigate slow subscribers
	 * @param <E> Type of processed signals
	 * @return a fresh processor
	 */
	public static <E> ExecutorProcessor<E, E> topic(String name, int bufferSize) {
		return topic(name, bufferSize, true);
	}

	/**
	 * Create a new {@link BaseProcessor} using the blockingWait Strategy, passed backlog size, and auto-cancel
	 * settings. <p> A Shared Processor authorizes concurrent onNext calls and is suited for multi-threaded publisher
	 * that will fan-in data. <p> The passed {@link java.util.concurrent.ExecutorService} will execute as many
	 * event-loop consuming the ringbuffer as subscribers.
	 * @param name Use a new Cached ExecutorService and assign this name to the created threads
	 * @param bufferSize A Backlog Size to mitigate slow subscribers
	 * @param autoCancel Should this propagate cancellation when unregistered by all subscribers ?
	 * @param <E> Type of processed signals
	 * @return a fresh processor
	 */
	public static <E> ExecutorProcessor<E, E> topic(String name, int bufferSize, boolean autoCancel) {
		return RingBufferProcessor.create(name, bufferSize, autoCancel);
	}

	/**
	 *
	 * Non-Blocking "Asynchronous" Work Queue (akin to vanilla Java Executor)
	 *
	 *
	 */

	/**
	 * Create a new {@link BaseProcessor} using {@link BaseProcessor#SMALL_BUFFER_SIZE} backlog size, blockingWait
	 * Strategy and auto-cancel. <p> A Shared Processor authorizes concurrent onNext calls and is suited for
	 * multi-threaded publisher that will fan-in data. <p> A new Cached ThreadExecutorPool will be implicitely created.
	 * @param <E> Type of processed signals
	 * @return a fresh processor
	 */
	public static <E> ExecutorProcessor<E, E> queue() {
		return queue("worker", BaseProcessor.SMALL_BUFFER_SIZE, true);
	}

	/**
	 * Create a new {@link BaseProcessor} using {@link BaseProcessor#SMALL_BUFFER_SIZE} backlog size, blockingWait
	 * Strategy and auto-cancel. <p> A Shared Processor authorizes concurrent onNext calls and is suited for
	 * multi-threaded publisher that will fan-in data. <p> A new Cached ThreadExecutorPool will be implicitely created.
	 * @param <E> Type of processed signals
	 * @return a fresh processor
	 */
	public static <E> ExecutorProcessor<E, E> queue(String name) {
		return queue(name, BaseProcessor.SMALL_BUFFER_SIZE, true);
	}

	/**
	 * Create a new {@link BaseProcessor} using {@link BaseProcessor#SMALL_BUFFER_SIZE} backlog size, blockingWait
	 * Strategy and the passed auto-cancel setting. <p> A Shared Processor authorizes concurrent onNext calls and is
	 * suited for multi-threaded publisher that will fan-in data. <p> A new Cached ThreadExecutorPool will be
	 * implicitely created.
	 * @param autoCancel Should this propagate cancellation when unregistered by all subscribers ?
	 * @param <E> Type of processed signals
	 * @return a fresh processor
	 */
	public static <E> ExecutorProcessor<E, E> queue(boolean autoCancel) {
		return queue(Processors.class.getSimpleName(), BaseProcessor.SMALL_BUFFER_SIZE, autoCancel);
	}

	/**
	 * Create a new {@link BaseProcessor} using {@link BaseProcessor#SMALL_BUFFER_SIZE} backlog size, blockingWait
	 * Strategy and the passed auto-cancel setting. <p> A Shared Processor authorizes concurrent onNext calls and is
	 * suited for multi-threaded publisher that will fan-in data. <p> A new Cached ThreadExecutorPool will be
	 * implicitely created and will use the passed name to qualify the created threads.
	 * @param name Use a new Cached ExecutorService and assign this name to the created threads
	 * @param bufferSize A Backlog Size to mitigate slow subscribers
	 * @param <E> Type of processed signals
	 * @return a fresh processor
	 */
	public static <E> ExecutorProcessor<E, E> queue(String name, int bufferSize) {
		return queue(name, bufferSize, true);
	}

	/**
	 * Create a new {@link BaseProcessor} using the passed buffer size and auto-cancel settings. <p> A new Cached
	 * ThreadExecutorPool will be implicitely created and will use the passed name to qualify the created threads.
	 * @param name Use a new Cached ExecutorService and assign this name to the created threads
	 * @param bufferSize A Backlog Size to mitigate slow subscribers
	 * @param autoCancel Should this propagate cancellation when unregistered by all subscribers ?
	 * @param <E> Type of processed signals
	 * @return a fresh processor
	 */
	public static <E> ExecutorProcessor<E, E> queue(String name, int bufferSize, boolean autoCancel) {
		return RingBufferWorkProcessor.create(name, bufferSize, autoCancel);
	}

	/**
	 *
	 * Non-Blocking "Asynchronous" Pooled Processors or "ProcessorGroup" : reuse resources with virtual processor
	 * references delegating to a pool of asynchronous processors (e.g. Topic).
	 *
	 * Dispatching behavior will implicitly or explicitly adapt to the reference method used: dispatchOn()
	 * or publisherOn().
	 *
	 */

	/**
	 * @param <E>
	 * @return
	 */
	public static <E> ProcessorGroup<E> singleGroup() {
		return singleGroup("single", BaseProcessor.MEDIUM_BUFFER_SIZE);
	}

	/**
	 * @param name
	 * @param <E>
	 * @return
	 */
	public static <E> ProcessorGroup<E> singleGroup(String name) {
		return singleGroup(name, BaseProcessor.MEDIUM_BUFFER_SIZE);
	}

	/**
	 * @param name
	 * @param bufferSize
	 * @param <E>
	 * @return
	 */
	public static <E> ProcessorGroup<E> singleGroup(String name, int bufferSize) {
		return singleGroup(name, bufferSize, null);
	}

	/**
	 * @param name
	 * @param bufferSize
	 * @param <E>
	 * @return
	 */
	public static <E> ProcessorGroup<E> singleGroup(String name, int bufferSize, Consumer<Throwable> errorC) {
		return singleGroup(name, bufferSize, errorC, null);
	}


	/**
	 * @param name
	 * @param bufferSize
	 * @param <E>
	 * @return
	 */
	public static <E> ProcessorGroup<E> singleGroup(String name, int bufferSize, Consumer<Throwable> errorC,
			Consumer<Void> shutdownC) {
		return singleGroup(name, bufferSize, errorC, shutdownC, SINGLE_WAIT_STRATEGY);
	}

	/**
	 * @param name
	 * @param bufferSize
	 * @param <E>
	 * @return
	 */
	public static <E> ProcessorGroup<E> singleGroup(String name, int bufferSize, Consumer<Throwable> errorC,
			Consumer<Void> shutdownC, Supplier<? extends WaitStrategy> waitStrategy) {
		return asyncGroup(name, bufferSize, 1, errorC, shutdownC, true, waitStrategy);
	}

	/**
	 * @param <E>
	 * @return
	 */
	public static <E> ProcessorGroup<E> asyncGroup() {
		return asyncGroup("async", BaseProcessor.MEDIUM_BUFFER_SIZE);
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
	public static <E> ProcessorGroup<E> asyncGroup(String name, int bufferSize) {
		return asyncGroup(name, bufferSize, null);
	}

	/**
	 * @param name
	 * @param bufferSize
	 * @param <E>
	 * @return
	 */
	public static <E> ProcessorGroup<E> asyncGroup(String name, int bufferSize, int concurrency) {
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
			Consumer<Void> shutdownHandler) {
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
			Consumer<Void> shutdownHandler) {
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
		return asyncGroup(name, bufferSize, DEFAULT_POOL_SIZE, uncaughtExceptionHandler, shutdownHandler, autoShutdown);
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

		return asyncGroup(name, bufferSize, concurrency, uncaughtExceptionHandler, shutdownHandler, autoShutdown, DEFAULT_WAIT_STRATEGY);
	}

	/**
	 * @param name
	 * @param bufferSize
	 * @param uncaughtExceptionHandler
	 * @param shutdownHandler
	 * @param autoShutdown
	 * @param waitprovider
	 * @param <E>
	 * @return
	 */
	public static <E> ProcessorGroup<E> asyncGroup(final String name,
			final int bufferSize,
			final int concurrency,
			Consumer<Throwable> uncaughtExceptionHandler,
			Consumer<Void> shutdownHandler,
			boolean autoShutdown,
			final Supplier<? extends WaitStrategy> waitprovider) {

		return ProcessorGroup.create(new Supplier<Processor<Runnable, Runnable>>() {
			int i = 1;
			@Override
			public Processor<Runnable, Runnable> get() {
				return RingBufferProcessor.share(name+(concurrency > 1 ? "-"+(i++) : ""), bufferSize, waitprovider
						.get(), false);
			}
		}, concurrency, uncaughtExceptionHandler, shutdownHandler, autoShutdown);
	}

	/**
	 * @param <E>
	 * @return
	 */
	public static <E> ProcessorGroup<E> ioGroup() {
		return ioGroup("io", BaseProcessor.MEDIUM_BUFFER_SIZE);
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
	public static <E> ProcessorGroup<E> ioGroup(String name, int bufferSize) {
		return ioGroup(name, bufferSize, DEFAULT_POOL_SIZE);
	}

	/**
	 * @param name
	 * @param bufferSize
	 * @param concurrency
	 * @param <E>
	 * @return
	 */
	public static <E> ProcessorGroup<E> ioGroup(String name, int bufferSize, int concurrency) {
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
		return ioGroup(name, bufferSize, concurrency, uncaughtExceptionHandler, shutdownHandler, autoShutdown,
				DEFAULT_WAIT_STRATEGY.get());
	}

	/**
	 * @param name
	 * @param bufferSize
	 * @param concurrency
	 * @param uncaughtExceptionHandler
	 * @param shutdownHandler
	 * @param autoShutdown
	 * @param waitStrategy
	 * @param <E>
	 * @return
	 */
	public static <E> ProcessorGroup<E> ioGroup(final String name,
			final int bufferSize,
			int concurrency,
			Consumer<Throwable> uncaughtExceptionHandler,
			Consumer<Void> shutdownHandler,
			boolean autoShutdown,
			WaitStrategy waitStrategy) {
		return ProcessorGroup.create(RingBufferWorkProcessor.<Runnable>share(name, bufferSize,
				waitStrategy, false),
				concurrency, uncaughtExceptionHandler, shutdownHandler, autoShutdown);
	}

	/**
	 *
	 *
	 * Miscellaneous : Processor log/lift, Decoration as queue
	 *
	 *
	 */
	/**
	 * @param processor
	 * @param <IN>
	 * @param <OUT>
	 * @return
	 */
	public static <IN, OUT> Processor<IN, OUT> log(Processor<IN, OUT> processor) {
		return log(processor, null, PublisherLog.ALL);
	}

	/**
	 * @param processor
	 * @param <IN>
	 * @param <OUT>
	 * @return
	 */
	public static <IN, OUT> Processor<IN, OUT> log(Processor<IN, OUT> processor, String category) {
		return log(processor, category, PublisherLog.ALL);
	}

	/**
	 * @param processor
	 * @param category
	 * @param <IN>
	 * @param <OUT>
	 * @return
	 */
	public static <IN, OUT> Processor<IN, OUT> log(final Processor<IN, OUT> processor,
			final String category,
			final int options) {
		return log(processor, category, Level.INFO, options);
	}

	/**
	 * @param processor
	 * @param category
	 * @param <IN>
	 * @param <OUT>
	 * @return
	 */
	public static <IN, OUT> Processor<IN, OUT> log(final Processor<IN, OUT> processor,
			final String category,
			final Level level,
			final int options) {
		return lift(processor, new Function<Processor<IN, OUT>, Publisher<OUT>>() {
			@Override
			public Publisher<OUT> apply(Processor<IN, OUT> processor) {
				return Publishers.log(processor, category, level, options);
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
	public static <IN, OUT> BaseProcessor<IN, OUT> create(final Subscriber<IN> upstream, final Publisher<OUT> downstream) {
		Assert.notNull(upstream, "Upstream must not be null");
		Assert.notNull(downstream, "Downstream must not be null");
		return new DelegateProcessor<>(downstream, upstream);
	}

	/**
	 * @param processor
	 * @param liftTransformation
	 * @param <IN>
	 * @param <OUT>
	 * @param <NOUT>
	 * @return
	 */
	public static <IN, OUT, NOUT> BaseProcessor<IN, NOUT> lift(final Processor<IN, OUT> processor,
			final Function<? super Processor<IN, OUT>, ? extends Publisher<NOUT>> liftTransformation) {
		return new DelegateProcessor<>(liftTransformation.apply(processor), processor);

	}

	private static class DelegateProcessor<IN, OUT>
			extends BaseProcessor<IN, OUT> implements ReactiveState.Downstream, ReactiveState.Bounded {

		private final Publisher<OUT> downstream;
		private final Subscriber<IN> upstream;

		public DelegateProcessor(Publisher<OUT> downstream, Subscriber<IN> upstream) {
			super(false);
			this.downstream = downstream;
			this.upstream = upstream;
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
		public long getCapacity() {
			return ReactiveState.Bounded.class.isAssignableFrom(upstream.getClass()) ? ((ReactiveState.Bounded) upstream).getCapacity() :
					Long.MAX_VALUE;
		}
	}


	private static final Supplier<? extends WaitStrategy> DEFAULT_WAIT_STRATEGY = new Supplier<WaitStrategy>() {
		@Override
		public WaitStrategy get() {
			return WaitStrategy.PhasedOff.withLiteLock(200, 200, TimeUnit.MILLISECONDS);
		}
	};

	private static final Supplier<? extends WaitStrategy> SINGLE_WAIT_STRATEGY = new Supplier<WaitStrategy>() {
		@Override
		public WaitStrategy get() {
			return WaitStrategy.PhasedOff.withLiteLock(500, 50, TimeUnit.MILLISECONDS);
		}
	};

}
