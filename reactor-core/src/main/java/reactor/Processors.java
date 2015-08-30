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
	public static final int DEFAULT_POOL_SIZE = Math.min(Runtime.getRuntime().availableProcessors(), 2);

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
	public static <E> BaseProcessor<E, E> async() {
		return async("async", BaseProcessor.SMALL_BUFFER_SIZE, true);
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
	public static <E> BaseProcessor<E, E> async(boolean autoCancel) {
		return async(Processors.class.getSimpleName(), BaseProcessor.SMALL_BUFFER_SIZE, autoCancel);
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
	public static <E> BaseProcessor<E, E> async(String name, int bufferSize) {
		return async(name, bufferSize, true);
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
	public static <E> BaseProcessor<E, E> async(String name, int bufferSize, boolean autoCancel) {
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
	public static <E> BaseProcessor<E, E> work() {
		return work("worker", BaseProcessor.SMALL_BUFFER_SIZE, true);
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
	public static <E> BaseProcessor<E, E> work(boolean autoCancel) {
		return work(Processors.class.getSimpleName(), BaseProcessor.SMALL_BUFFER_SIZE, autoCancel);
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
	public static <E> BaseProcessor<E, E> work(String name, int bufferSize) {
		return work(name, bufferSize, true);
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
	public static <E> BaseProcessor<E, E> work(String name, int bufferSize, boolean autoCancel) {
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
	public static <E> ProcessorService<E> asyncService() {
		return asyncService(null, BaseProcessor.MEDIUM_BUFFER_SIZE);
	}

	/**
	 * @param name
	 * @param <E>
	 * @return
	 */
	public static <E> ProcessorService<E> asyncService(String name) {
		return asyncService(name, BaseProcessor.MEDIUM_BUFFER_SIZE);
	}

	/**
	 * @param name
	 * @param bufferSize
	 * @param <E>
	 * @return
	 */
	public static <E> ProcessorService<E> asyncService(String name,
	                                                   int bufferSize) {
		return asyncService(name, bufferSize, null);
	}

	/**
	 * @param name
	 * @param bufferSize
	 * @param <E>
	 * @return
	 */
	public static <E> ProcessorService<E> asyncService(String name,
	                                                   int bufferSize,
	                                                   int concurrency) {
		return asyncService(name, bufferSize, concurrency, null);
	}

	/**
	 * @param name
	 * @param bufferSize
	 * @param uncaughtExceptionHandler
	 * @param <E>
	 * @return
	 */
	public static <E> ProcessorService<E> asyncService(String name,
	                                                   int bufferSize,
	                                                   Consumer<Throwable> uncaughtExceptionHandler) {
		return asyncService(name, bufferSize, uncaughtExceptionHandler, null);
	}


	/**
	 * @param name
	 * @param bufferSize
	 * @param uncaughtExceptionHandler
	 * @param <E>
	 * @return
	 */
	public static <E> ProcessorService<E> asyncService(String name,
	                                                   int bufferSize,
	                                                   int concurrency,
	                                                   Consumer<Throwable> uncaughtExceptionHandler) {
		return asyncService(name, bufferSize, concurrency, uncaughtExceptionHandler, null);
	}

	/**
	 * @param name
	 * @param bufferSize
	 * @param uncaughtExceptionHandler
	 * @param shutdownHandler
	 * @param <E>
	 * @return
	 */
	public static <E> ProcessorService<E> asyncService(String name,
	                                                   int bufferSize,
	                                                   Consumer<Throwable> uncaughtExceptionHandler,
	                                                   Consumer<Void> shutdownHandler
	) {
		return asyncService(name, bufferSize, uncaughtExceptionHandler, shutdownHandler, true);
	}


	/**
	 * @param name
	 * @param bufferSize
	 * @param uncaughtExceptionHandler
	 * @param shutdownHandler
	 * @param <E>
	 * @return
	 */
	public static <E> ProcessorService<E> asyncService(String name,
	                                                   int bufferSize,
	                                                   int concurrency,
	                                                   Consumer<Throwable> uncaughtExceptionHandler,
	                                                   Consumer<Void> shutdownHandler
	) {
		return asyncService(name, bufferSize, concurrency, uncaughtExceptionHandler, shutdownHandler, true);
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
	public static <E> ProcessorService<E> asyncService(String name,
	                                                   int bufferSize,
	                                                   Consumer<Throwable> uncaughtExceptionHandler,
	                                                   Consumer<Void> shutdownHandler,
	                                                   boolean autoShutdown) {
		return asyncService(name, bufferSize, 1, uncaughtExceptionHandler, shutdownHandler, autoShutdown);
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
	public static <E> ProcessorService<E> asyncService(final String name,
	                                                   final int bufferSize,
	                                                   int concurrency,
	                                                   Consumer<Throwable> uncaughtExceptionHandler,
	                                                   Consumer<Void> shutdownHandler,
	                                                   boolean autoShutdown) {

		return ProcessorService.create(
		  new Supplier<Processor<ProcessorService.Task, ProcessorService.Task>>() {
			  @Override
			  public Processor<ProcessorService.Task, ProcessorService.Task> get() {
				  return  PlatformDependent.hasUnsafe()
				    ? RingBufferProcessor.share(name, bufferSize, ProcessorService.DEFAULT_TASK_PROVIDER)
				    : SimpleWorkProcessor.create(name, bufferSize, MpscLinkedQueue.<SimpleSignal<ProcessorService.Task>>create
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
	public static <E> ProcessorService<E> workService(String name) {
		return workService(name, BaseProcessor.MEDIUM_BUFFER_SIZE);
	}

	/**
	 * @param name
	 * @param bufferSize
	 * @param <E>
	 * @return
	 */
	public static <E> ProcessorService<E> workService(String name,
	                                                  int bufferSize) {
		return workService(name, bufferSize, DEFAULT_POOL_SIZE);
	}

	/**
	 * @param name
	 * @param bufferSize
	 * @param concurrency
	 * @param <E>
	 * @return
	 */
	public static <E> ProcessorService<E> workService(String name,
	                                                  int bufferSize,
	                                                  int concurrency) {
		return workService(name, bufferSize, concurrency, null, null, true);
	}

	/**
	 * @param name
	 * @param bufferSize
	 * @param concurrency
	 * @param uncaughtExceptionHandler
	 * @param <E>
	 * @return
	 */
	public static <E> ProcessorService<E> workService(String name,
	                                                  int bufferSize,
	                                                  int concurrency,
	                                                  Consumer<Throwable> uncaughtExceptionHandler) {
		return workService(name, bufferSize, concurrency, uncaughtExceptionHandler, null, true);
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
	public static <E> ProcessorService<E> workService(String name,
	                                                  int bufferSize,
	                                                  int concurrency,
	                                                  Consumer<Throwable> uncaughtExceptionHandler,
	                                                  Consumer<Void> shutdownHandler) {
		return workService(name, bufferSize, concurrency, uncaughtExceptionHandler, shutdownHandler, true);
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
	public static <E> ProcessorService<E> workService(final String name,
	                                                  final int bufferSize,
	                                                  int concurrency,
	                                                  Consumer<Throwable> uncaughtExceptionHandler,
	                                                  Consumer<Void> shutdownHandler,
	                                                  boolean autoShutdown) {
		return ProcessorService.create(
				  PlatformDependent.hasUnsafe()
				    ? RingBufferWorkProcessor.<ProcessorService.Task>share(name, bufferSize)
				    : SimpleWorkProcessor.<ProcessorService.Task>create(name, bufferSize),
		  concurrency,
		  uncaughtExceptionHandler,
		  shutdownHandler,
		  autoShutdown
		);
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
				return Publishers.lift(processor, new LogOperator<>(category));
			}
		});
	}

	/**
	 * @param <IN>
	 * @return
	 */
	public static <IN> BlockingQueue<IN> queue(Processor<IN, IN> source) {
		return queue(source, BaseProcessor.SMALL_BUFFER_SIZE);
	}

	/**
	 * @param <IN>
	 * @return
	 */
	public static <IN> BlockingQueue<IN> queue(Processor<IN, IN> source, int size) {
		return queue(source, size, new ArrayBlockingQueue<IN>(size));
	}

	/**
	 * @param <IN>
	 * @return
	 */
	public static <IN> BlockingQueue<IN> queue(Processor<IN, IN> source, int size, Queue<IN> store) {
		return new BlockingQueueSubscriber<>(source, source, store, size);
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
}
