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

import java.util.Queue;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.ExecutorService;
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
	public static <E> SimpleWorkProcessor<E> create(String name, int bufferSize, Queue<E> queue) {
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
	                                                 Queue<E> queue,
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
	public static <E> SimpleWorkProcessor<E> create(ExecutorService service, int bufferSize, Queue<E> queue) {
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
	                                                 Queue<E> queue,
	                                                 boolean autoCancel) {
		return new SimpleWorkProcessor<>(null, service, bufferSize, queue, autoCancel);
	}


	private final Queue<IN> workQueue;
	private final int       capacity;


	protected SimpleWorkProcessor(String name, ExecutorService executor,
	                               int bufferSize, Queue<IN> workQueue, boolean autoCancel) {
		super(name, executor, autoCancel);

		this.workQueue = workQueue == null ? new ConcurrentLinkedQueue<>() : workQueue;
		this.capacity = bufferSize;
	}

	@Override
	public void subscribe(Subscriber<? super IN> s) {

		this.executor.execute(new Runnable() {
			@Override
			public void run() {
				Task task;
				try {
					for (; ; ) {
						task = workQueue.poll();
						if (null != task) {
							task.run();
						} else {
							LockSupport.parkNanos(1l); //TODO expose
						}
					}
				} catch (EndException e) {
					//ignore
				}
			}
		});
	}

	@Override
	public void onNext(IN in) {
		workQueue.offer(task);
	}

	@Override
	public void onError(Throwable t) {
		workQueue.add(task);
	}

	@Override
	public void onComplete() {
		workQueue.add(new EndMpscTask());
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

}
