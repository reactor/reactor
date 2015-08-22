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
import reactor.core.error.SpecificationExceptions;
import reactor.core.processor.simple.SimpleSignal;
import reactor.core.processor.simple.SimpleSubscriberUtils;
import reactor.core.support.internal.MpscLinkedQueue;

import java.util.Queue;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.locks.LockSupport;

/**
 * A simple async processor
 *
 * @author Stephane Maldini
 */
public final class SimpleAsyncProcessor<IN> extends ExecutorPoweredProcessor<IN, IN> {

	/**
	 * Create a new SimpleAsyncProcessor using {@link #SMALL_BUFFER_SIZE} backlog size, blockingWait Strategy
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
	public static <E> SimpleAsyncProcessor<E> create() {
		return create(SimpleAsyncProcessor.class.getSimpleName(), SMALL_BUFFER_SIZE, null, true);
	}

	/**
	 * Create a new SimpleAsyncProcessor using {@link #SMALL_BUFFER_SIZE} backlog size, blockingWait Strategy
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
	public static <E> SimpleAsyncProcessor<E> create(boolean autoCancel) {
		return create(SimpleAsyncProcessor.class.getSimpleName(), SMALL_BUFFER_SIZE, null, autoCancel);
	}

	/**
	 * Create a new SimpleAsyncProcessor using {@link #SMALL_BUFFER_SIZE} backlog size, blockingWait Strategy
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
	public static <E> SimpleAsyncProcessor<E> create(ExecutorService service) {
		return create(service, SMALL_BUFFER_SIZE, null, true);
	}

	/**
	 * Create a new SimpleAsyncProcessor using {@link #SMALL_BUFFER_SIZE} backlog size, blockingWait Strategy
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
	public static <E> SimpleAsyncProcessor<E> create(ExecutorService service, boolean autoCancel) {
		return create(service, SMALL_BUFFER_SIZE, null, autoCancel);
	}

	/**
	 * Create a new SimpleAsyncProcessor using {@link #SMALL_BUFFER_SIZE} backlog size, blockingWait Strategy
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
	public static <E> SimpleAsyncProcessor<E> create(String name, int bufferSize) {
		return create(name, bufferSize, null, true);
	}

	/**
	 * Create a new SimpleAsyncProcessor using the blockingWait Strategy, passed backlog size,
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
	public static <E> SimpleAsyncProcessor<E> create(String name, int bufferSize, boolean autoCancel) {
		return create(name, bufferSize, null, autoCancel);
	}

	/**
	 * Create a new SimpleAsyncProcessor using passed backlog size, blockingWait Strategy
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
	public static <E> SimpleAsyncProcessor<E> create(ExecutorService service, int bufferSize) {
		return create(service, bufferSize, null, true);
	}

	/**
	 * Create a new SimpleAsyncProcessor using passed backlog size, blockingWait Strategy
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
	public static <E> SimpleAsyncProcessor<E> create(ExecutorService service, int bufferSize, boolean autoCancel) {
		return create(service, bufferSize, null, autoCancel);
	}


	/**
	 * Create a new SimpleAsyncProcessor using passed backlog size, wait strategy
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
	public static <E> SimpleAsyncProcessor<E> create(String name, int bufferSize, Queue<SimpleSignal<E>> queue) {
		return create(name, bufferSize, queue, true);
	}

	/**
	 * Create a new SimpleAsyncProcessor using passed backlog size, wait strategy
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
	public static <E> SimpleAsyncProcessor<E> create(String name,
	                                                 int bufferSize,
	                                                 Queue<SimpleSignal<E>> queue,
	                                                 boolean autoCancel) {
		return new SimpleAsyncProcessor<>(name, null, bufferSize, queue, autoCancel);
	}

	/**
	 * Create a new SimpleAsyncProcessor using passed backlog size, wait strategy
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
	public static <E> SimpleAsyncProcessor<E> create(ExecutorService service, int bufferSize, Queue<SimpleSignal<E>> queue) {
		return create(service, bufferSize, queue, true);
	}

	/**
	 * Create a new SimpleAsyncProcessor using passed backlog size, wait strategy
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
	public static <E> SimpleAsyncProcessor<E> create(ExecutorService service,
	                                                 int bufferSize,
	                                                 Queue<SimpleSignal<E>> queue,
	                                                 boolean autoCancel) {
		return new SimpleAsyncProcessor<>(null, service, bufferSize, queue, autoCancel);
	}


	private final Queue<SimpleSignal<IN>> workQueue;
	private final int       capacity;


	protected SimpleAsyncProcessor(String name, ExecutorService executor,
	                               int bufferSize, Queue<SimpleSignal<IN>> workQueue, boolean autoCancel) {
		super(name, executor, autoCancel);

		this.workQueue = workQueue == null ? MpscLinkedQueue.<SimpleSignal<IN>>create() : workQueue;
		this.capacity = bufferSize;
	}

	@Override
	public void subscribe(final Subscriber<? super IN> s) {
		if(s == null) {
			throw SpecificationExceptions.spec_2_13_exception();
		}

		incrementSubscribers();
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
		return false;
	}

}
