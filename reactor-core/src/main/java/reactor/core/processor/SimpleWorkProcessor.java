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

import org.reactivestreams.Subscriber;
import org.reactivestreams.Subscription;
import reactor.core.error.CancelException;
import reactor.core.error.Exceptions;
import reactor.core.error.SpecificationExceptions;
import reactor.core.processor.simple.SimpleSignal;
import reactor.core.processor.simple.SimpleSubscriberUtils;
import reactor.core.support.Assert;
import reactor.core.support.SignalType;

import java.util.Queue;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.atomic.AtomicIntegerFieldUpdater;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.locks.LockSupport;

/**
 * A base processor used by executor backed processors to take care of their ExecutorService
 *
 * @author Stephane Maldini
 */
public final class SimpleWorkProcessor<IN> extends ExecutorPoweredProcessor<IN, IN> {

	/**
	 * Create a new SimpleWorkProcessor using {@link #SMALL_BUFFER_SIZE} backlog size, blockingWait Strategy
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
	public static <E> SimpleWorkProcessor<E> create() {
		return create(SimpleWorkProcessor.class.getSimpleName(), SMALL_BUFFER_SIZE, null, true);
	}

	/**
	 * Create a new SimpleWorkProcessor using {@link #SMALL_BUFFER_SIZE} backlog size, blockingWait Strategy
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
	public static <E> SimpleWorkProcessor<E> create(boolean autoCancel) {
		return create(SimpleWorkProcessor.class.getSimpleName(), SMALL_BUFFER_SIZE, null, autoCancel);
	}

	/**
	 * Create a new SimpleWorkProcessor using {@link #SMALL_BUFFER_SIZE} backlog size, blockingWait Strategy
	 * and auto-cancel.
	 * <p>
	 * A Shared Processor authorizes concurrent onNext calls and is suited for multi-threaded publisher that
	 * will fan-in data.
	 * <p>
	 * The passed {@link java.util.concurrent.ExecutorService} will execute as many event-loop
	 * consuming the ringbuffer as subscribers.
	 *
	 * @param service A provided ExecutorService to manage threading infrastructure
	 * @param <E>     Type of processed signals
	 * @return a fresh processor
	 */
	public static <E> SimpleWorkProcessor<E> create(ExecutorService service) {
		return create(service, SMALL_BUFFER_SIZE, null, true);
	}

	/**
	 * Create a new SimpleWorkProcessor using {@link #SMALL_BUFFER_SIZE} backlog size, blockingWait Strategy
	 * and the passed auto-cancel setting.
	 * <p>
	 * A Shared Processor authorizes concurrent onNext calls and is suited for multi-threaded publisher that
	 * will fan-in data.
	 * <p>
	 * The passed {@link java.util.concurrent.ExecutorService} will execute as many event-loop
	 * consuming the ringbuffer as subscribers.
	 *
	 * @param service    A provided ExecutorService to manage threading infrastructure
	 * @param autoCancel Should this propagate cancellation when unregistered by all subscribers ?
	 * @param <E>        Type of processed signals
	 * @return a fresh processor
	 */
	public static <E> SimpleWorkProcessor<E> create(ExecutorService service, boolean autoCancel) {
		return create(service, SMALL_BUFFER_SIZE, null, autoCancel);
	}

	/**
	 * Create a new SimpleWorkProcessor using {@link #SMALL_BUFFER_SIZE} backlog size, blockingWait Strategy
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
	public static <E> SimpleWorkProcessor<E> create(String name, int bufferSize) {
		return create(name, bufferSize, null, true);
	}

	/**
	 * Create a new SimpleWorkProcessor using the blockingWait Strategy, passed backlog size,
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
	public static <E> SimpleWorkProcessor<E> create(String name, int bufferSize, boolean autoCancel) {
		return create(name, bufferSize, null, autoCancel);
	}

	/**
	 * Create a new SimpleWorkProcessor using passed backlog size, blockingWait Strategy
	 * and will auto-cancel.
	 * <p>
	 * A Shared Processor authorizes concurrent onNext calls and is suited for multi-threaded publisher that
	 * will fan-in data.
	 * <p>
	 * The passed {@link java.util.concurrent.ExecutorService} will execute as many event-loop
	 * consuming the ringbuffer as subscribers.
	 *
	 * @param service    A provided ExecutorService to manage threading infrastructure
	 * @param bufferSize A Backlog Size to mitigate slow subscribers
	 * @param <E>        Type of processed signals
	 * @return a fresh processor
	 */
	public static <E> SimpleWorkProcessor<E> create(ExecutorService service, int bufferSize) {
		return create(service, bufferSize, null, true);
	}

	/**
	 * Create a new SimpleWorkProcessor using passed backlog size, blockingWait Strategy
	 * and the auto-cancel argument.
	 * <p>
	 * A Shared Processor authorizes concurrent onNext calls and is suited for multi-threaded publisher that
	 * will fan-in data.
	 * <p>
	 * The passed {@link java.util.concurrent.ExecutorService} will execute as many event-loop
	 * consuming the ringbuffer as subscribers.
	 *
	 * @param service    A provided ExecutorService to manage threading infrastructure
	 * @param bufferSize A Backlog Size to mitigate slow subscribers
	 * @param autoCancel Should this propagate cancellation when unregistered by all subscribers ?
	 * @param <E>        Type of processed signals
	 * @return a fresh processor
	 */
	public static <E> SimpleWorkProcessor<E> create(ExecutorService service, int bufferSize, boolean autoCancel) {
		return create(service, bufferSize, null, autoCancel);
	}


	/**
	 * Create a new SimpleWorkProcessor using passed backlog size, wait strategy
	 * and will auto-cancel.
	 * <p>
	 * A Shared Processor authorizes concurrent onNext calls and is suited for multi-threaded publisher that
	 * will fan-in data.
	 * <p>
	 * A new Cached ThreadExecutorPool will be implicitely created and will use the passed name to qualify
	 * the created threads.
	 *
	 * @param name       Use a new Cached ExecutorService and assign this name to the created threads
	 * @param bufferSize A Backlog Size to mitigate slow subscribers
	 * @param queue      A supplied queue to buffer incoming messages
	 * @param <E>        Type of processed signals
	 * @return a fresh processor
	 */
	public static <E> SimpleWorkProcessor<E> create(String name, int bufferSize, Queue<SimpleSignal<E>> queue) {
		return create(name, bufferSize, queue, true);
	}

	/**
	 * Create a new SimpleWorkProcessor using passed backlog size, wait strategy
	 * and auto-cancel settings.
	 * <p>
	 * A Shared Processor authorizes concurrent onNext calls and is suited for multi-threaded publisher that
	 * will fan-in data.
	 * <p>
	 * A new Cached ThreadExecutorPool will be implicitely created and will use the passed name to qualify
	 * the created threads.
	 *
	 * @param name       Use a new Cached ExecutorService and assign this name to the created threads
	 * @param bufferSize A Backlog Size to mitigate slow subscribers
	 * @param queue      A supplied queue to buffer incoming messages
	 * @param autoCancel Should this propagate cancellation when unregistered by all subscribers ?
	 * @param <E>        Type of processed signals
	 * @return a fresh processor
	 */
	public static <E> SimpleWorkProcessor<E> create(String name,
	                                                int bufferSize,
	                                                Queue<SimpleSignal<E>> queue,
	                                                boolean autoCancel) {
		return new SimpleWorkProcessor<>(name, null, bufferSize, queue, autoCancel);
	}

	/**
	 * Create a new SimpleWorkProcessor using passed backlog size, wait strategy
	 * and will auto-cancel.
	 * <p>
	 * A Shared Processor authorizes concurrent onNext calls and is suited for multi-threaded publisher that
	 * will fan-in data.
	 * <p>
	 * The passed {@link java.util.concurrent.ExecutorService} will execute as many event-loop
	 * consuming the ringbuffer as subscribers.
	 *
	 * @param service    A provided ExecutorService to manage threading infrastructure
	 * @param bufferSize A Backlog Size to mitigate slow subscribers
	 * @param queue      A supplied queue to buffer incoming messages
	 * @param <E>        Type of processed signals
	 * @return a fresh processor
	 */
	public static <E> SimpleWorkProcessor<E> create(ExecutorService service, int bufferSize, Queue<SimpleSignal<E>>
	  queue) {
		return create(service, bufferSize, queue, true);
	}

	/**
	 * Create a new SimpleWorkProcessor using passed backlog size, wait strategy
	 * and auto-cancel settings.
	 * <p>
	 * A Shared Processor authorizes concurrent onNext calls and is suited for multi-threaded publisher that
	 * will fan-in data.
	 * <p>
	 * The passed {@link java.util.concurrent.ExecutorService} will execute as many event-loop
	 * consuming the ringbuffer as subscribers.
	 *
	 * @param service    A provided ExecutorService to manage threading infrastructure
	 * @param bufferSize A Backlog Size to mitigate slow subscribers
	 * @param queue      A supplied queue to buffer incoming messages
	 * @param autoCancel Should this propagate cancellation when unregistered by all subscribers ?
	 * @param <E>        Type of processed signals
	 * @return a fresh processor
	 */
	public static <E> SimpleWorkProcessor<E> create(ExecutorService service,
	                                                int bufferSize,
	                                                Queue<SimpleSignal<E>> queue,
	                                                boolean autoCancel) {
		return new SimpleWorkProcessor<>(null, service, bufferSize, queue, autoCancel);
	}


	private final Queue<SimpleSignal<IN>> workQueue;
	private final int                     capacity;
	private final int                     prefetch;

	private static final AtomicIntegerFieldUpdater<SimpleWorkProcessor> AVAILABLE =
		AtomicIntegerFieldUpdater.newUpdater(SimpleWorkProcessor.class, "availableCapacity");

	private volatile int                     availableCapacity;


	protected SimpleWorkProcessor(String name, ExecutorService executor,
	                              int bufferSize, Queue<SimpleSignal<IN>> workQueue, boolean autoCancel) {
		super(name, executor, autoCancel);
		Assert.isTrue(bufferSize > 2, "Buffer size must be greater than 2");

		this.workQueue = workQueue == null ? new ConcurrentLinkedQueue<SimpleSignal<IN>>() : workQueue;
		this.capacity = bufferSize;
		this.availableCapacity = bufferSize - 1;
		this.prefetch = Math.min(bufferSize / 2 - 1, 1);
	}

	@Override
	public void subscribe(final Subscriber<? super IN> subscriber) {
		if (subscriber == null) {
			throw SpecificationExceptions.spec_2_13_exception();
		}

		try {
			final SubscriberWorker<IN> signalProcessor = new SubscriberWorker<>(
			  this,
			  subscriber
			);

			this.executor.execute(signalProcessor);
			incrementSubscribers();
		}catch(Throwable t){
			Exceptions.<IN>publisher(t).subscribe(subscriber);
		}
	}

	@Override
	public void onNext(IN in) {
		super.onNext(in);
		while(AVAILABLE.decrementAndGet(this) < 0) {
			AVAILABLE.incrementAndGet(this);
			if(!alive()) throw CancelException.get();
		}

		SimpleSubscriberUtils.onNext(in, workQueue);
	}

	@Override
	public void onError(Throwable t) {
		super.onError(t);
		SimpleSubscriberUtils.onError(t, workQueue);
	}

	@Override
	public void onComplete() {
		SimpleSubscriberUtils.onComplete(workQueue);
		super.onComplete();
	}

	@Override
	public long getCapacity() {
		return capacity;
	}

	@Override
	public long getAvailableCapacity() {
		return availableCapacity;
	}

	@Override
	public boolean isWork() {
		return true;
	}

	private static class SubscriberWorker<IN> extends AtomicLong implements Subscription, Runnable {

		private final Subscriber<? super IN>  subscriber;
		private final SimpleWorkProcessor<IN> processor;

		public SubscriberWorker(SimpleWorkProcessor<IN> workProcessor, Subscriber<? super IN> s) {
			super(Long.MIN_VALUE);
			this.subscriber = s;
			this.processor = workProcessor;
		}

		@Override
		public void run() {
			try {
				if (!compareAndSet(Long.MIN_VALUE, 0)) {
					subscriber.onError(new IllegalStateException("Thread is already running"));
					return;
				}

				try {
					subscriber.onSubscribe(this);
				} catch (Throwable t) {
					Exceptions.throwIfFatal(t);
					subscriber.onError(t);
					return;
				}

				if (!SimpleSubscriberUtils.waitRequestOrTerminalEvent(
				  this, processor.workQueue, subscriber
				)) {
					return;
				}

				final boolean unbounded = get() == Long.MAX_VALUE;

				SimpleSignal<IN> task;
				long availableBuffer;

				Subscription upstream = processor.upstreamSubscription;
				try {

					if(upstream != null) {
						if (unbounded) {
							upstream.request(Long.MAX_VALUE);
						} else {
							upstream.request(processor.capacity - 1);
						}
					}

					for (;;) {

						while (!unbounded && decrementAndGet() < 0L){
							incrementAndGet();
							if( !isRunning() ) throw CancelException.INSTANCE;
							LockSupport.parkNanos(1L);
						}

						task = processor.workQueue.poll();

						if (null != task) {

							availableBuffer = AVAILABLE.getAndIncrement(processor);

							if(!unbounded &&
							  task.type == SignalType.NEXT &&
							  (upstream = processor.upstreamSubscription) != null &&
							  availableBuffer == processor.prefetch){
								upstream.request(availableBuffer);
							}

							SimpleSubscriberUtils.route(task, subscriber);
						} else {

							if(!unbounded) {
								incrementAndGet();
							}

							if (!isRunning()) {
								throw CancelException.INSTANCE;
							}

							LockSupport.parkNanos(1l); //TODO expose?
						}
					}
				} catch (CancelException e) {
					//ignore
				} catch (Throwable e){
					Exceptions.throwIfFatal(e);
					subscriber.onError(e);
				}
			} finally {
				processor.decrementSubscribers();
			}
			halt();
		}

		@Override
		public void request(long n) {
			if (n <= 0l) {
				subscriber.onError(SpecificationExceptions.spec_3_09_exception(n));
				return;
			}

			if (!isRunning()) {
				return;
			}

			if (addAndGet(n) < 0) {
				set(Long.MAX_VALUE);
			}
		}


		@Override
		public void cancel() {
			halt();
		}

		private boolean isRunning(){
			return get() != Long.MIN_VALUE / 2;
		}

		private void halt(){
			set(Long.MIN_VALUE / 2);
		}
	}
}
