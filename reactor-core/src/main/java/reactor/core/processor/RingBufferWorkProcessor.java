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

package reactor.core.processor;

import java.util.Arrays;
import java.util.Iterator;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.RejectedExecutionException;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicIntegerFieldUpdater;
import java.util.concurrent.atomic.AtomicReferenceFieldUpdater;
import java.util.concurrent.locks.LockSupport;

import org.reactivestreams.Publisher;
import org.reactivestreams.Subscriber;
import org.reactivestreams.Subscription;
import reactor.core.error.AlertException;
import reactor.core.error.CancelException;
import reactor.core.error.InsufficientCapacityException;
import reactor.core.publisher.FluxFactory;
import reactor.core.subscription.EmptySubscription;
import reactor.core.support.BackpressureUtils;
import reactor.core.support.NamedDaemonThreadFactory;
import reactor.core.support.ReactiveState;
import reactor.core.support.SignalType;
import reactor.core.support.WaitStrategy;
import reactor.core.support.internal.PlatformDependent;
import reactor.core.support.rb.MutableSignal;
import reactor.core.support.rb.RequestTask;
import reactor.core.support.rb.RingBufferSequencer;
import reactor.core.support.rb.RingBufferSubscriberUtils;
import reactor.core.support.rb.disruptor.RingBuffer;
import reactor.core.support.rb.disruptor.Sequence;
import reactor.core.support.rb.disruptor.SequenceBarrier;
import reactor.core.support.rb.disruptor.Sequencer;
import reactor.fn.LongSupplier;
import reactor.fn.Supplier;

/**
 * An implementation of a RingBuffer backed message-passing WorkProcessor. <p> The
 * processor is very similar to {@link reactor.core.processor.RingBufferProcessor} but
 * only partially respects the Reactive Streams contract. <p> The purpose of this
 * processor is to distribute the signals to only one of the subscribed subscribers and to
 * share the demand amongst all subscribers. The scenario is akin to Executor or
 * Round-Robin distribution. However there is no guarantee the distribution will be
 * respecting a round-robin distribution all the time. <p> The core use for this component
 * is to scale up easily without suffering the overhead of an Executor and without using
 * dedicated queues by subscriber, which is less used memory, less GC, more win.
 * @param <E> Type of dispatched signal
 * @author Stephane Maldini
 */
public final class RingBufferWorkProcessor<E> extends ExecutorProcessor<E, E>
		implements ReactiveState.Buffering, ReactiveState.LinkedDownstreams {

	/**
	 * Create a new RingBufferWorkProcessor using {@link ReactiveState#SMALL_BUFFER_SIZE} backlog size,
	 * blockingWait Strategy and auto-cancel. <p> A new Cached ThreadExecutorPool will be
	 * implicitely created.
	 * @param <E> Type of processed signals
	 * @return a fresh processor
	 */
	public static <E> RingBufferWorkProcessor<E> create() {
		return create(RingBufferWorkProcessor.class.getSimpleName(), SMALL_BUFFER_SIZE,
				null, true);
	}

	/**
	 * Create a new RingBufferWorkProcessor using {@link ReactiveState#SMALL_BUFFER_SIZE} backlog size,
	 * blockingWait Strategy and the passed auto-cancel setting. <p> A new Cached
	 * ThreadExecutorPool will be implicitely created.
	 * @param autoCancel Should this propagate cancellation when unregistered by all
	 * subscribers ?
	 * @param <E> Type of processed signals
	 * @return a fresh processor
	 */
	public static <E> RingBufferWorkProcessor<E> create(boolean autoCancel) {
		return create(RingBufferWorkProcessor.class.getSimpleName(), SMALL_BUFFER_SIZE,
				null, autoCancel);
	}

	/**
	 * Create a new RingBufferWorkProcessor using {@link ReactiveState#SMALL_BUFFER_SIZE} backlog size,
	 * blockingWait Strategy and auto-cancel. The passed {@link
	 * java.util.concurrent.ExecutorService} will execute as many event-loop consuming the
	 * ringbuffer as subscribers.
	 * @param service A provided ExecutorService to manage threading infrastructure
	 * @param <E> Type of processed signals
	 * @return a fresh processor
	 */
	public static <E> RingBufferWorkProcessor<E> create(ExecutorService service) {
		return create(service, SMALL_BUFFER_SIZE, null, true);
	}

	/**
	 * Create a new RingBufferWorkProcessor using {@link ReactiveState#SMALL_BUFFER_SIZE} backlog size,
	 * blockingWait Strategy and the passed auto-cancel setting. <p> The passed {@link
	 * java.util.concurrent.ExecutorService} will execute as many event-loop consuming the
	 * ringbuffer as subscribers.
	 * @param service A provided ExecutorService to manage threading infrastructure
	 * @param autoCancel Should this propagate cancellation when unregistered by all
	 * subscribers ?
	 * @param <E> Type of processed signals
	 * @return a fresh processor
	 */
	public static <E> RingBufferWorkProcessor<E> create(ExecutorService service,
			boolean autoCancel) {
		return create(service, SMALL_BUFFER_SIZE, null,
				autoCancel);
	}

	/**
	 * Create a new RingBufferProcessor using the default buffer size 32, blockingWait
	 * Strategy and auto-cancel. <p> A new Cached ThreadExecutorPool will be implicitely
	 * created and will use the passed name to qualify the created threads.
	 * @param name Use a new Cached ExecutorService and assign this name to the created
	 * threads
	 * @param <E> Type of processed signals
	 * @return a fresh processor
	 */
	public static <E> RingBufferWorkProcessor<E> create(String name) {
		return create(name, SMALL_BUFFER_SIZE);
	}

	/**
	 * Create a new RingBufferProcessor using the passed buffer size, blockingWait
	 * Strategy and auto-cancel. <p> A new Cached ThreadExecutorPool will be implicitely
	 * created and will use the passed name to qualify the created threads.
	 * @param name Use a new Cached ExecutorService and assign this name to the created
	 * threads
	 * @param bufferSize A Backlog Size to mitigate slow subscribers
	 * @param <E> Type of processed signals
	 * @return a fresh processor
	 */
	public static <E> RingBufferWorkProcessor<E> create(String name, int bufferSize) {
		return create(name, bufferSize, null, true);
	}

	/**
	 * Create a new RingBufferProcessor using the passed buffer size, blockingWait
	 * Strategy and the passed auto-cancel setting. <p> A new Cached ThreadExecutorPool
	 * will be implicitely created and will use the passed name to qualify the created
	 * threads.
	 * @param name Use a new Cached ExecutorService and assign this name to the created
	 * threads
	 * @param bufferSize A Backlog Size to mitigate slow subscribers
	 * @param autoCancel Should this propagate cancellation when unregistered by all
	 * subscribers ?
	 * @param <E> Type of processed signals
	 * @return a fresh processor
	 */
	public static <E> RingBufferWorkProcessor<E> create(String name, int bufferSize,
			boolean autoCancel) {
		return create(name, bufferSize, null, autoCancel);
	}

	/**
	 * Create a new RingBufferProcessor using the passed buffer size, blockingWait
	 * Strategy and auto-cancel. <p> The passed {@link java.util.concurrent.ExecutorService}
	 * will execute as many event-loop consuming the ringbuffer as subscribers.
	 * @param service A provided ExecutorService to manage threading infrastructure
	 * @param bufferSize A Backlog Size to mitigate slow subscribers
	 * @param <E> Type of processed signals
	 * @return a fresh processor
	 */
	public static <E> RingBufferWorkProcessor<E> create(ExecutorService service,
			int bufferSize) {
		return create(service, bufferSize, null, true);
	}

	/**
	 * Create a new RingBufferWorkProcessor using the passed buffer size, blockingWait
	 * Strategy and auto-cancel. <p> The passed {@link java.util.concurrent.ExecutorService}
	 * will execute as many event-loop consuming the ringbuffer as subscribers.
	 * @param service A provided ExecutorService to manage threading infrastructure
	 * @param bufferSize A Backlog Size to mitigate slow subscribers
	 * @param autoCancel Should this propagate cancellation when unregistered by all
	 * subscribers ?
	 * @param <E> Type of processed signals
	 * @return a fresh processor
	 */
	public static <E> RingBufferWorkProcessor<E> create(ExecutorService service,
			int bufferSize, boolean autoCancel) {
		return create(service, bufferSize, null, autoCancel);
	}

	/**
	 * Create a new RingBufferWorkProcessor using the passed buffer size, blockingWait
	 * Strategy and auto-cancel. <p> A new Cached ThreadExecutorPool will be implicitely
	 * created and will use the passed name to qualify the created threads.
	 * @param name Use a new Cached ExecutorService and assign this name to the created
	 * threads
	 * @param bufferSize A Backlog Size to mitigate slow subscribers
	 * @param strategy A RingBuffer WaitStrategy to use instead of the default
	 * smart blocking wait strategy.
	 * @param <E> Type of processed signals
	 * @return a fresh processor
	 */
	public static <E> RingBufferWorkProcessor<E> create(String name, int bufferSize,
			WaitStrategy strategy) {
		return create(name, bufferSize, strategy, true);
	}

	/**
	 * Create a new RingBufferWorkProcessor using the passed buffer size, blockingWait
	 * Strategy and auto-cancel settings. <p> A new Cached ThreadExecutorPool will be
	 * implicitely created and will use the passed name to qualify the created threads.
	 * @param name Use a new Cached ExecutorService and assign this name to the created
	 * threads
	 * @param bufferSize A Backlog Size to mitigate slow subscribers
	 * @param strategy A RingBuffer WaitStrategy to use instead of the default
	 * smart blocking wait strategy.
	 * @param autoCancel Should this propagate cancellation when unregistered by all
	 * subscribers ?
	 * @param <E> Type of processed signals
	 * @return a fresh processor
	 */
	public static <E> RingBufferWorkProcessor<E> create(String name, int bufferSize,
			WaitStrategy strategy, boolean autoCancel) {
		return new RingBufferWorkProcessor<E>(name, null, bufferSize, strategy, false,
				autoCancel);
	}

	/**
	 * Create a new RingBufferWorkProcessor using the passed buffer size and blockingWait
	 * Strategy settings but will auto-cancel. <p> The passed {@link
	 * java.util.concurrent.ExecutorService} will execute as many event-loop consuming the
	 * ringbuffer as subscribers.
	 * @param executor A provided ExecutorService to manage threading infrastructure
	 * @param bufferSize A Backlog Size to mitigate slow subscribers
	 * @param strategy A RingBuffer WaitStrategy to use instead of the default
	 * smart blocking wait strategy.
	 * @param <E> Type of processed signals
	 * @return a fresh processor
	 */
	public static <E> RingBufferWorkProcessor<E> create(ExecutorService executor,
			int bufferSize, WaitStrategy strategy) {
		return create(executor, bufferSize, strategy, true);
	}

	/**
	 * Create a new RingBufferWorkProcessor using the passed buffer size, wait strategy
	 * and auto-cancel settings. <p> The passed {@link java.util.concurrent.ExecutorService}
	 * will execute as many event-loop consuming the ringbuffer as subscribers.
	 * @param executor A provided ExecutorService to manage threading infrastructure
	 * @param bufferSize A Backlog Size to mitigate slow subscribers
	 * @param strategy A RingBuffer WaitStrategy to use instead of the default
	 * smart blocking wait strategy.
	 * @param autoCancel Should this propagate cancellation when unregistered by all
	 * subscribers ?
	 * @param <E> Type of processed signals
	 * @return a fresh processor
	 */
	public static <E> RingBufferWorkProcessor<E> create(ExecutorService executor,
			int bufferSize, WaitStrategy strategy, boolean autoCancel) {
		return new RingBufferWorkProcessor<E>(null, executor, bufferSize, strategy, false,
				autoCancel);
	}

	/**
	 * Create a new RingBufferWorkProcessor using {@link ReactiveState#SMALL_BUFFER_SIZE} backlog size,
	 * blockingWait Strategy and auto-cancel. <p> A Shared Processor authorizes concurrent
	 * onNext calls and is suited for multi-threaded publisher that will fan-in data. <p>
	 * A new Cached ThreadExecutorPool will be implicitely created.
	 * @param <E> Type of processed signals
	 * @return a fresh processor
	 */
	public static <E> RingBufferWorkProcessor<E> share() {
		return share(RingBufferWorkProcessor.class.getSimpleName(), SMALL_BUFFER_SIZE,
				null, true);
	}

	/**
	 * Create a new RingBufferWorkProcessor using {@link ReactiveState#SMALL_BUFFER_SIZE} backlog size,
	 * blockingWait Strategy and the passed auto-cancel setting. <p> A Shared Processor
	 * authorizes concurrent onNext calls and is suited for multi-threaded publisher that
	 * will fan-in data. <p> A new Cached ThreadExecutorPool will be implicitely created.
	 * @param autoCancel Should this propagate cancellation when unregistered by all
	 * subscribers ?
	 * @param <E> Type of processed signals
	 * @return a fresh processor
	 */
	public static <E> RingBufferWorkProcessor<E> share(boolean autoCancel) {
		return share(RingBufferWorkProcessor.class.getSimpleName(), SMALL_BUFFER_SIZE,
				null, autoCancel);
	}

	/**
	 * Create a new RingBufferWorkProcessor using {@link ReactiveState#SMALL_BUFFER_SIZE} backlog size,
	 * blockingWait Strategy and auto-cancel. The passed {@link
	 * java.util.concurrent.ExecutorService} will execute as many event-loop consuming the
	 * ringbuffer as subscribers.
	 * @param service A provided ExecutorService to manage threading infrastructure
	 * @param <E> Type of processed signals
	 * @return a fresh processor
	 */
	public static <E> RingBufferWorkProcessor<E> share(ExecutorService service) {
		return share(service, SMALL_BUFFER_SIZE, null, true);
	}

	/**
	 * Create a new RingBufferWorkProcessor using {@link ReactiveState#SMALL_BUFFER_SIZE} backlog size,
	 * blockingWait Strategy and the passed auto-cancel setting. <p> A Shared Processor
	 * authorizes concurrent onNext calls and is suited for multi-threaded publisher that
	 * will fan-in data. <p> The passed {@link java.util.concurrent.ExecutorService} will
	 * execute as many event-loop consuming the ringbuffer as subscribers.
	 * @param service A provided ExecutorService to manage threading infrastructure
	 * @param autoCancel Should this propagate cancellation when unregistered by all
	 * subscribers ?
	 * @param <E> Type of processed signals
	 * @return a fresh processor
	 */
	public static <E> RingBufferWorkProcessor<E> share(ExecutorService service,
			boolean autoCancel) {
		return share(service, SMALL_BUFFER_SIZE, null,
				autoCancel);
	}

	/**
	 * Create a new RingBufferProcessor using the passed buffer size, blockingWait
	 * Strategy and auto-cancel. <p> A Shared Processor authorizes concurrent onNext calls
	 * and is suited for multi-threaded publisher that will fan-in data. <p> A new Cached
	 * ThreadExecutorPool will be implicitely created and will use the passed name to
	 * qualify the created threads.
	 * @param name Use a new Cached ExecutorService and assign this name to the created
	 * threads
	 * @param bufferSize A Backlog Size to mitigate slow subscribers
	 * @param <E> Type of processed signals
	 * @return a fresh processor
	 */
	public static <E> RingBufferWorkProcessor<E> share(String name, int bufferSize) {
		return share(name, bufferSize, null, true);
	}

	/**
	 * Create a new RingBufferProcessor using the passed buffer size, blockingWait
	 * Strategy and the passed auto-cancel setting. <p> A Shared Processor authorizes
	 * concurrent onNext calls and is suited for multi-threaded publisher that will fan-in
	 * data. <p> A new Cached ThreadExecutorPool will be implicitely created and will use
	 * the passed name to qualify the created threads.
	 * @param name Use a new Cached ExecutorService and assign this name to the created
	 * threads
	 * @param bufferSize A Backlog Size to mitigate slow subscribers
	 * @param autoCancel Should this propagate cancellation when unregistered by all
	 * subscribers ?
	 * @param <E> Type of processed signals
	 * @return a fresh processor
	 */
	public static <E> RingBufferWorkProcessor<E> share(String name, int bufferSize,
			boolean autoCancel) {
		return share(name, bufferSize, null, autoCancel);
	}

	/**
	 * Create a new RingBufferProcessor using the passed buffer size, blockingWait
	 * Strategy and auto-cancel. <p> A Shared Processor authorizes concurrent onNext calls
	 * and is suited for multi-threaded publisher that will fan-in data. <p> The passed
	 * {@link java.util.concurrent.ExecutorService} will execute as many event-loop
	 * consuming the ringbuffer as subscribers.
	 * @param service A provided ExecutorService to manage threading infrastructure
	 * @param bufferSize A Backlog Size to mitigate slow subscribers
	 * @param <E> Type of processed signals
	 * @return a fresh processor
	 */
	public static <E> RingBufferWorkProcessor<E> share(ExecutorService service,
			int bufferSize) {
		return share(service, bufferSize, null, true);
	}

	/**
	 * Create a new RingBufferWorkProcessor using the passed buffer size, blockingWait
	 * Strategy and auto-cancel. <p> A Shared Processor authorizes concurrent onNext calls
	 * and is suited for multi-threaded publisher that will fan-in data. <p> The passed
	 * {@link java.util.concurrent.ExecutorService} will execute as many event-loop
	 * consuming the ringbuffer as subscribers.
	 * @param service A provided ExecutorService to manage threading infrastructure
	 * @param bufferSize A Backlog Size to mitigate slow subscribers
	 * @param autoCancel Should this propagate cancellation when unregistered by all
	 * subscribers ?
	 * @param <E> Type of processed signals
	 * @return a fresh processor
	 */
	public static <E> RingBufferWorkProcessor<E> share(ExecutorService service,
			int bufferSize, boolean autoCancel) {
		return share(service, bufferSize, null, autoCancel);
	}

	/**
	 * Create a new RingBufferWorkProcessor using the passed buffer size, blockingWait
	 * Strategy and auto-cancel. <p> A Shared Processor authorizes concurrent onNext calls
	 * and is suited for multi-threaded publisher that will fan-in data. <p> A new Cached
	 * ThreadExecutorPool will be implicitely created and will use the passed name to
	 * qualify the created threads.
	 * @param name Use a new Cached ExecutorService and assign this name to the created
	 * threads
	 * @param bufferSize A Backlog Size to mitigate slow subscribers
	 * @param strategy A RingBuffer WaitStrategy to use instead of the default
	 * smart blocking wait strategy.
	 * @param <E> Type of processed signals
	 * @return a fresh processor
	 */
	public static <E> RingBufferWorkProcessor<E> share(String name, int bufferSize,
			WaitStrategy strategy) {
		return share(name, bufferSize, strategy, true);
	}

	/**
	 * Create a new RingBufferWorkProcessor using the passed buffer size, blockingWait
	 * Strategy and auto-cancel settings. <p> A Shared Processor authorizes concurrent
	 * onNext calls and is suited for multi-threaded publisher that will fan-in data. <p>
	 * A new Cached ThreadExecutorPool will be implicitely created and will use the passed
	 * name to qualify the created threads.
	 * @param name Use a new Cached ExecutorService and assign this name to the created
	 * threads
	 * @param bufferSize A Backlog Size to mitigate slow subscribers
	 * @param strategy A RingBuffer WaitStrategy to use instead of the default
	 * smart blocking wait strategy.
	 * @param autoCancel Should this propagate cancellation when unregistered by all
	 * subscribers ?
	 * @param <E> Type of processed signals
	 * @return a fresh processor
	 */
	public static <E> RingBufferWorkProcessor<E> share(String name, int bufferSize,
			WaitStrategy strategy, boolean autoCancel) {
		return new RingBufferWorkProcessor<E>(name, null, bufferSize, strategy, true,
				autoCancel);
	}

	/**
	 * Create a new RingBufferWorkProcessor using the passed buffer size and blockingWait
	 * Strategy settings but will auto-cancel. <p> A Shared Processor authorizes
	 * concurrent onNext calls and is suited for multi-threaded publisher that will fan-in
	 * data. <p> The passed {@link java.util.concurrent.ExecutorService} will execute as
	 * many event-loop consuming the ringbuffer as subscribers.
	 * @param executor A provided ExecutorService to manage threading infrastructure
	 * @param bufferSize A Backlog Size to mitigate slow subscribers
	 * @param strategy A RingBuffer WaitStrategy to use instead of the default
	 * smart blocking wait strategy.
	 * @param <E> Type of processed signals
	 * @return a fresh processor
	 */
	public static <E> RingBufferWorkProcessor<E> share(ExecutorService executor,
			int bufferSize, WaitStrategy strategy) {
		return share(executor, bufferSize, strategy, true);
	}

	/**
	 * Create a new RingBufferWorkProcessor using the passed buffer size, wait strategy
	 * and auto-cancel settings. <p> A Shared Processor authorizes concurrent onNext calls
	 * and is suited for multi-threaded publisher that will fan-in data. <p> The passed
	 * {@link java.util.concurrent.ExecutorService} will execute as many event-loop
	 * consuming the ringbuffer as subscribers.
	 * @param executor A provided ExecutorService to manage threading infrastructure
	 * @param bufferSize A Backlog Size to mitigate slow subscribers
	 * @param strategy A RingBuffer WaitStrategy to use instead of the default
	 * smart blocking wait strategy.
	 * @param autoCancel Should this propagate cancellation when unregistered by all
	 * subscribers ?
	 * @param <E> Type of processed signals
	 * @return a fresh processor
	 */
	public static <E> RingBufferWorkProcessor<E> share(ExecutorService executor,
			int bufferSize, WaitStrategy strategy, boolean autoCancel) {
		return new RingBufferWorkProcessor<E>(null, executor, bufferSize, strategy, true,
				autoCancel);
	}

	private static final Supplier FACTORY = new Supplier<MutableSignal>() {
		@Override
		public MutableSignal get() {
			return new MutableSignal<>();
		}
	};

	/**
	 * Instance
	 */

	private final Sequence workSequence =
			Sequencer.newSequence(Sequencer.INITIAL_CURSOR_VALUE);

	private final Sequence retrySequence =
			Sequencer.newSequence(Sequencer.INITIAL_CURSOR_VALUE);

	private final RingBuffer<MutableSignal<E>> ringBuffer;

	private volatile RingBuffer<MutableSignal<E>> retryBuffer;

	private final static AtomicReferenceFieldUpdater<RingBufferWorkProcessor, RingBuffer>
			RETRY_REF = PlatformDependent
			.newAtomicReferenceFieldUpdater(RingBufferWorkProcessor.class, "retryBuffer");

	private final WaitStrategy readWait = new WaitStrategy.LiteBlocking();

	private volatile int replaying = 0;

	private static final AtomicIntegerFieldUpdater<RingBufferWorkProcessor> REPLAYING =
			AtomicIntegerFieldUpdater
					.newUpdater(RingBufferWorkProcessor.class, "replaying");

	@SuppressWarnings("unchecked")
	private RingBufferWorkProcessor(String name, ExecutorService executor, int bufferSize,
	                                WaitStrategy waitStrategy, boolean share,
	                                boolean autoCancel) {
		super(name, executor, autoCancel);

		Supplier<MutableSignal<E>> factory = (Supplier<MutableSignal<E>>) FACTORY;

		Runnable spinObserver = new Runnable() {
			@Override
			public void run() {
				if (!alive()) {
					throw CancelException.get();
				}
			}
		};

		WaitStrategy strategy = waitStrategy == null ?
				new WaitStrategy.LiteBlocking() :
				waitStrategy;

		if (share) {
			this.ringBuffer = RingBuffer
					.createMultiProducer(factory, bufferSize, strategy, spinObserver);
		}
		else {
			this.ringBuffer = RingBuffer
					.createSingleProducer(factory, bufferSize, strategy, spinObserver);
		}
		ringBuffer.addGatingSequence(workSequence);

	}

	@Override
	public void subscribe(final Subscriber<? super E> subscriber) {
		super.subscribe(subscriber);

		if (!alive()) {
			RingBufferSequencer<E> sequencer = new RingBufferSequencer<>(ringBuffer, workSequence.get());
			FluxFactory.create(sequencer, sequencer).subscribe(subscriber);
			return;
		}

		final QueueSubscriber<E> signalProcessor =
				new QueueSubscriber<E>(subscriber, this);
		try {

			incrementSubscribers();

			//bind eventProcessor sequence to observe the ringBuffer
			signalProcessor.sequence.set(workSequence.get());
			ringBuffer.addGatingSequence(signalProcessor.sequence);

			//start the subscriber thread
			executor.execute(signalProcessor);

		}
		catch (Throwable t) {
			decrementSubscribers();
			ringBuffer.removeGatingSequence(signalProcessor.sequence);
			if(RejectedExecutionException.class.isAssignableFrom(t.getClass())){
				RingBufferSequencer<E> sequencer = new RingBufferSequencer<>(ringBuffer, workSequence.get());
				FluxFactory.create(sequencer, sequencer).subscribe(subscriber);
			}
			else {
				EmptySubscription.error(subscriber, t);
			}
		}
	}

	@Override
	public void onNext(E o) {
		super.onNext(o);
		RingBufferSubscriberUtils.onNext(o, ringBuffer);
	}

	@Override
	protected void doError(Throwable t) {
		RingBufferSubscriberUtils.onError(t, ringBuffer);
		for (long n = ringBuffer.getCursor() - 1; n < workSequence.get(); n++) {
			RingBufferSubscriberUtils.onError(t, ringBuffer);
		}
		readWait.signalAllWhenBlocking();
	}

	@Override
	protected void doComplete() {
		try {
			boolean isInContext = isInContext();
			if(isInContext){
				RingBufferSubscriberUtils.tryOnComplete(ringBuffer);
			}
			else {
				RingBufferSubscriberUtils.onComplete(ringBuffer);
			}
			for (long n = ringBuffer.getCursor() - 1; n <= workSequence.get(); n++) {
				if(isInContext){
					RingBufferSubscriberUtils.tryOnComplete(ringBuffer);
				}
				else {
					RingBufferSubscriberUtils.onComplete(ringBuffer);
				}
			}
		}
		catch (CancelException | InsufficientCapacityException ce){
			//ignore
		}
		readWait.signalAllWhenBlocking();
	}

	public Publisher<Void> writeWith(final Publisher<? extends E> source) {
		return RingBufferSubscriberUtils.writeWith(source, ringBuffer);
	}

	@Override
	protected void requestTask(Subscription s) {
		new NamedDaemonThreadFactory(name+"[request-task]", null, null, false).newThread(new RequestTask(s, new Runnable() {
			@Override
			public void run() {
				if (!alive()) {
					if (cancelled) {
						throw CancelException.INSTANCE;
					}
					else {
						throw AlertException.INSTANCE;
					}
				}
			}
		}, null, new LongSupplier() {
			@Override
			public long get() {
				return ringBuffer.getMinimumGatingSequence();
			}
		}, readWait, this, ringBuffer)).start();
	}

	@Override
	protected void cancel(Subscription subscription) {
		super.cancel(subscription);
		readWait.signalAllWhenBlocking();
	}

	@Override
	public boolean isStarted() {
		return super.isStarted() || ringBuffer.get() != -1;
	}

	@Override
	public long getAvailableCapacity() {
		return ringBuffer.remainingCapacity();
	}

	@Override
	public String toString() {
		return "RingBufferWorkProcessor{" +
				", ringBuffer=" + ringBuffer +
				", executor=" + executor +
				", workSequence=" + workSequence +
				", retrySequence=" + retrySequence +
				'}';
	}

	@Override
	public long getCapacity() {
		return ringBuffer.getBufferSize();
	}

	@Override
	public boolean isWork() {
		return true;
	}

	@Override
	public long pending() {
		return ringBuffer.pending() + (retryBuffer != null ? retryBuffer.pending() : 0L);
	}

	@SuppressWarnings("unchecked")
	RingBuffer<MutableSignal<E>> retryBuffer() {
		RingBuffer<MutableSignal<E>> retry = retryBuffer;
		if (retry == null) {
			retry =
					RingBuffer.createMultiProducer((Supplier<MutableSignal<E>>) FACTORY, 32, RingBuffer.NO_WAIT);
			retry.addGatingSequence(retrySequence);
			if (!RETRY_REF.compareAndSet(this, null, retry)) {
				retry = retryBuffer;
			}
		}
		return retry;
	}


	@Override
	public Iterator<?> downstreams() {
		return Arrays.asList(ringBuffer.getSequencer().getGatingSequences()).iterator();
	}

	@Override
	public long downstreamsCount() {
		return ringBuffer.getSequencer().getGatingSequences().length - 1;
	}

	/**
	 * Disruptor WorkProcessor port that deals with pending demand. <p> Convenience class
	 * for handling the batching semantics of consuming entries from a {@link
	 * reactor.core.processor .rb.disruptor .RingBuffer} <p>
	 * @param <T> event implementation storing the data for sharing during exchange or
	 * parallel coordination of an event.
	 */
	private final static class QueueSubscriber<T>
			implements Runnable, Downstream, Buffering, ActiveUpstream,
			ActiveDownstream, Inner, DownstreamDemand, Subscription, Upstream {

		private final AtomicBoolean running = new AtomicBoolean(false);

		private final Sequence sequence =
				Sequencer.wrap(Sequencer.INITIAL_CURSOR_VALUE, this);

		private final Sequence pendingRequest = Sequencer.newSequence(0);

		private final SequenceBarrier barrier;

		private final RingBufferWorkProcessor<T> processor;

		private final Subscriber<? super T> subscriber;

		private final Runnable waiter = new Runnable() {
			@Override
			public void run() {
				if (barrier.isAlerted() || !isRunning() ||
						replay(pendingRequest.get() == Long.MAX_VALUE)) {
					throw AlertException.INSTANCE;
				}
			}
		};

		/**
		 * Construct a ringbuffer consumer that will automatically track the progress by
		 * updating its sequence
		 */
		public QueueSubscriber(Subscriber<? super T> subscriber,
				RingBufferWorkProcessor<T> processor) {
			this.processor = processor;
			this.subscriber = subscriber;

			this.barrier = processor.ringBuffer.newBarrier();
		}

		public Sequence getSequence() {
			return sequence;
		}

		public void halt() {
			running.set(false);
			barrier.alert();
		}

		public boolean isRunning() {
			return running.get();
		}


		/**
		 * It is ok to have another thread rerun this method after a halt().
		 */
		@Override
		public void run() {

			try {
				if (!running.compareAndSet(false, true)) {
					EmptySubscription.error(subscriber, new IllegalStateException("Thread is already running"));
					return;
				}

				//while(processor.alive() && processor.upstreamSubscription == null);
				if(!processor.startSubscriber(subscriber, this)){
					return;
				}

				boolean processedSequence = true;
				long cachedAvailableSequence = Long.MIN_VALUE;
				long nextSequence = sequence.get();
				MutableSignal<T> event = null;

				if (!RingBufferSubscriberUtils.waitRequestOrTerminalEvent(pendingRequest, processor.ringBuffer, barrier, subscriber, running, processor.workSequence, this)) {
					return;
				}

				final boolean unbounded = pendingRequest.get() == Long.MAX_VALUE;

				if (replay(unbounded)) {
					running.set(false);
					return;
				}

				while (true) {
					try {
						// if previous sequence was processed - fetch the next sequence and set
						// that we have successfully processed the previous sequence
						// typically, this will be true
						// this prevents the sequence getting too far forward if an error
						// is thrown from the WorkHandler
						if (processedSequence) {
							processedSequence = false;
							do {
								nextSequence = processor.workSequence.get() + 1L;

								if (!unbounded) {
									readNextEvent(processor.ringBuffer.get(nextSequence), false);
									pendingRequest.incrementAndGet();
								}

								sequence.set(nextSequence - 1L);
							}
							while (!processor.workSequence.compareAndSet(nextSequence - 1L, nextSequence));
						}

						if (cachedAvailableSequence >= nextSequence) {
							event = processor.ringBuffer.get(nextSequence);

							try {
								readNextEvent(event, unbounded);
							}
							catch (AlertException ce) {
								barrier.clearAlert();
								throw CancelException.INSTANCE;
							}

							RingBufferSubscriberUtils.route(event, subscriber);

							processedSequence = true;

						}
						else {
							processor.readWait.signalAllWhenBlocking();
							try {
								cachedAvailableSequence =
										barrier.waitFor(nextSequence, waiter);
							}
							catch (AlertException ce) {
								barrier.clearAlert();
								if (!isRunning()) {
									processor.decrementSubscribers();
								}
								else {
									throw ce;
								}
								try {
									for (; ; ) {
										try {
											cachedAvailableSequence =
													barrier.waitFor(nextSequence);
											event = processor.ringBuffer.get(nextSequence);
											break;
										}
										catch (AlertException cee) {
											barrier.clearAlert();
										}
									}
									reschedule(event);
								}
								catch (Exception c) {
									//IGNORE
								}
								processor.incrementSubscribers();
								throw ce;
							}
						}

					}
					catch (CancelException ce) {
						reschedule(event);
						break;
					}
					catch (AlertException ex) {
						barrier.clearAlert();
						if (!running.get()) {
							break;
						}
						//processedSequence = true;
						//continue event-loop

					}
					catch (final Throwable ex) {
						reschedule(event);
						subscriber.onError(ex);
						sequence.set(nextSequence);
						processedSequence = true;
					}
				}
			}
			finally {
				processor.decrementSubscribers();
				processor.ringBuffer.removeGatingSequence(sequence);
				/*if(processor.decrementSubscribers() == 0){
					long r = processor.ringBuffer.getCursor();
					long w = processor.workSequence.get();
					if ( w > r ){
						processor.workSequence.compareAndSet(w, r);
					}
				}*/
				running.set(false);
				barrier.alert();
			}
		}

		private boolean replay(final boolean unbounded) {

			if (REPLAYING.compareAndSet(processor, 0, 1)) {
				MutableSignal<T> signal;

				try {
					RingBuffer<MutableSignal<T>> q = processor.retryBuffer;
					if (q == null) {
						return false;
					}

					for (; ; ) {

						if (!isRunning()) {
							return true;
						}

						long cursor = processor.retrySequence.get() + 1;

						if (q.getCursor() >= cursor) {
							signal = q.get(cursor);
						}
						else {
							processor.readWait.signalAllWhenBlocking();
							return !processor.alive();
						}
						readNextEvent(signal, unbounded);
						RingBufferSubscriberUtils.route(signal, subscriber);
						processor.retrySequence.set(cursor);
					}

				}
				catch (CancelException ce) {
					running.set(false);
					return true;
				}
				finally {
					REPLAYING.compareAndSet(processor, 1, 0);
				}
			}
			else {
				return !processor.alive();
			}
		}

		private void reschedule(MutableSignal<T> event) {
			if (event != null &&
					event.type == SignalType.NEXT &&
					event.value != null &&
					event.value != RingBufferSubscriberUtils.CLEANED) {

				RingBuffer<MutableSignal<T>> retry = processor.retryBuffer();
				long seq = retry.next();
				retry.get(seq).value = event.value;
				retry.publish(seq);
				barrier.alert();
				processor.readWait.signalAllWhenBlocking();
			}
		}

		private void readNextEvent(MutableSignal<T> event, final boolean unbounded)
				throws AlertException {
			//if event is Next Signal we need to handle backpressure (pendingRequests)
			if (event.type == SignalType.NEXT) {
				if (event.value == null || event.value == RingBufferSubscriberUtils.CLEANED) {
					return;
				}

				//pause until request
				while (!unbounded && BackpressureUtils.getAndSub(pendingRequest, 1) == 0L) {
					if (!isRunning()) {
						throw AlertException.INSTANCE;
					}
					//Todo Use WaitStrategy?
					LockSupport.parkNanos(1l);
				}
			}
			else if (event.type != null) {
				//Complete or Error are terminal events, we shutdown the processor and process the signal
				running.set(false);
				RingBufferSubscriberUtils.route(event, subscriber);
				throw AlertException.INSTANCE;
			}
		}

		@Override
		public long requestedFromDownstream() {
			return pendingRequest.get();
		}

		@Override
		public boolean isCancelled() {
			return !running.get();
		}

		@Override
		public boolean isStarted() {
			return sequence.get() != -1L;
		}

		@Override
		public boolean isTerminated() {
			return !running.get();
		}

		@Override
		public long pending() {
			return processor.ringBuffer.pending();
		}

		@Override
		public long getCapacity() {
			return processor.getCapacity();
		}

		@Override
		public Object downstream() {
			return subscriber;
		}

		@Override
		public Object upstream() {
			return processor;
		}

		@Override
		public void request(long n) {
			if (BackpressureUtils.checkRequest(n, subscriber)) {
				if (!isRunning()) {
					return;
				}

				BackpressureUtils.getAndAdd(pendingRequest, n);
			}
		}

		@Override
		public void cancel() {
			halt();
		}
	}


}
