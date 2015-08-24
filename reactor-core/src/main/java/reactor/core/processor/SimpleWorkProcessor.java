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
import reactor.Publishers;
import reactor.core.error.CancelException;
import reactor.core.error.Exceptions;
import reactor.core.error.SpecificationExceptions;
import reactor.core.processor.simple.SimpleSignal;
import reactor.core.processor.simple.SimpleSubscriberUtils;

import java.util.Queue;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.atomic.AtomicBoolean;
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


	protected SimpleWorkProcessor(String name, ExecutorService executor,
	                              int bufferSize, Queue<SimpleSignal<IN>> workQueue, boolean autoCancel) {
		super(name, executor, autoCancel);

		this.workQueue = workQueue == null ? new ConcurrentLinkedQueue<SimpleSignal<IN>>() : workQueue;
		this.capacity = bufferSize;
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

			//prepare the subscriber subscription to this processor
			signalProcessor.subscription = new SimpleSubscription<>(signalProcessor, subscriber);

			this.executor.execute(signalProcessor);
			incrementSubscribers();
		}catch(Throwable t){
			Publishers.<IN>error(t).subscribe(subscriber);
		}
	}

	@Override
	public void onNext(IN in) {
		super.onNext(in);
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
		return workQueue.size();
	}

	@Override
	public boolean isWork() {
		return true;
	}

	private class SimpleSubscription<IN> implements Subscription {

		private final SubscriberWorker<IN>   runnable;
		private final Subscriber<? super IN> subscriber;

		public SimpleSubscription(SubscriberWorker<IN> runnable,
		                          Subscriber<? super IN> subscriber) {
			this.runnable = runnable;
			this.subscriber = subscriber;
		}

		@Override
		public void request(long n) {
			if (n <= 0l) {
				subscriber.onError(SpecificationExceptions.spec_3_09_exception(n));
				return;
			}

			if (!runnable.running.get()) {
				return;
			}

			if (runnable.pendingRequests.addAndGet(n) < 0) {
				runnable.pendingRequests.set(Long.MAX_VALUE);
			}

			long toRequest = n;

			if (toRequest > 0l) {
				Subscription parent = upstreamSubscription;
				if (parent != null) {
					parent.request(toRequest);
				}
			}
		}

		@Override
		public void cancel() {
			runnable.running.set(false);
		}
	}

	private static class SubscriberWorker<IN> implements Runnable {

		private final Subscriber<? super IN>  subscriber;
		private final SimpleWorkProcessor<IN> processor;
		private final AtomicBoolean           running;
		private final AtomicLong              pendingRequests;

		Subscription subscription;

		public SubscriberWorker(SimpleWorkProcessor<IN> workProcessor, Subscriber<? super IN> s) {
			this.subscriber = s;
			this.processor = workProcessor;
			this.running = new AtomicBoolean();
			this.pendingRequests = new AtomicLong(0);
		}

		@Override
		public void run() {
			try {
				if (!running.compareAndSet(false, true)) {
					subscriber.onError(new IllegalStateException("Thread is already running"));
					return;
				}

				try {
					subscriber.onSubscribe(subscription);
				} catch (Throwable t) {
					Exceptions.throwIfFatal(t);
					subscriber.onError(t);
					return;
				}

				if (!SimpleSubscriberUtils.waitRequestOrTerminalEvent(
				  pendingRequests, processor.workQueue, subscriber, running
				)) {
					return;
				}

				final boolean unbounded = pendingRequests.get() == Long.MAX_VALUE;

				SimpleSignal<IN> task;
				try {
					for (; ; ) {
						task = processor.workQueue.poll();
						if (null != task) {
							SimpleSubscriberUtils.route(task, subscriber);
						} else {
							LockSupport.parkNanos(1l); //TODO expose?
							if (!running.get()) throw CancelException.INSTANCE;
						}
					}
				} catch (CancelException e) {
					//ignore
				}
			} finally {
				processor.decrementSubscribers();
			}
			running.set(false);
		}
	}
}
