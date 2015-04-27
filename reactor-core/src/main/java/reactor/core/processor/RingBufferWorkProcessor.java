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
import reactor.core.processor.util.RingBufferSubscriberUtils;
import reactor.core.processor.util.SingleUseExecutor;
import reactor.core.support.SpecificationExceptions;
import reactor.jarjar.com.lmax.disruptor.*;
import reactor.jarjar.com.lmax.disruptor.dsl.ProducerType;

import java.util.Queue;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.locks.LockSupport;

/**
 * An implementation of a RingBuffer backed message-passing WorkProcessor.
 * <p>
 * The processor is very similar to {@link reactor.core.processor.RingBufferProcessor} but only partially respects the
 * Reactive Streams contract.
 * <p>
 * The purpose of this processor is to distribute the signals to only one of the subscribed subscribers and to share
 * the
 * demand amongst all subscribers. The scenario is akin to Executor or Round-Robin distribution. However there is
 * no guarantee the distribution will be respecting a round-robin distribution all the time.
 * <p>
 * The core use for this component is to scale up easily without suffering the overhead of an Executor and without
 * using
 * dedicated queues by subscriber, which is less used memory, less GC, more win.
 *
 * @param <E> Type of dispatched signal
 * @author Stephane Maldini
 */
public final class RingBufferWorkProcessor<E> extends ReactorProcessor<E> {

	/**
	 * Create a new RingBufferWorkProcessor using {@link #SMALL_BUFFER_SIZE} backlog size, blockingWait Strategy
	 * and auto-cancel.
	 * <p>
	 * A new Cached ThreadExecutorPool will be implicitely created.
	 *
	 * @param <E> Type of processed signals
	 * @return a fresh processor
	 */
	public static <E> RingBufferWorkProcessor<E> create() {
		return create(RingBufferWorkProcessor.class.getSimpleName(), SMALL_BUFFER_SIZE, new
				BlockingWaitStrategy(), true);
	}

	/**
	 * Create a new RingBufferWorkProcessor using {@link #SMALL_BUFFER_SIZE} backlog size, blockingWait Strategy
	 * and the passed auto-cancel setting.
	 * <p>
	 * A new Cached ThreadExecutorPool will be implicitely created.
	 *
	 * @param autoCancel Should this propagate cancellation when unregistered by all subscribers ?
	 * @param <E>        Type of processed signals
	 * @return a fresh processor
	 */
	public static <E> RingBufferWorkProcessor<E> create(boolean autoCancel) {
		return create(RingBufferWorkProcessor.class.getSimpleName(), SMALL_BUFFER_SIZE, new
				BlockingWaitStrategy(), autoCancel);
	}

	/**
	 * Create a new RingBufferWorkProcessor using {@link #SMALL_BUFFER_SIZE} backlog size, blockingWait Strategy
	 * and auto-cancel.
	 * The passed {@link java.util.concurrent.ExecutorService} will execute as many event-loop
	 * consuming the ringbuffer as subscribers.
	 *
	 * @param service A provided ExecutorService to manage threading infrastructure
	 * @param <E>     Type of processed signals
	 * @return a fresh processor
	 */
	public static <E> RingBufferWorkProcessor<E> create(ExecutorService service) {
		return create(service, SMALL_BUFFER_SIZE, new BlockingWaitStrategy(), true);
	}

	/**
	 * Create a new RingBufferWorkProcessor using {@link #SMALL_BUFFER_SIZE} backlog size, blockingWait Strategy
	 * and the passed auto-cancel setting.
	 * <p>
	 * The passed {@link java.util.concurrent.ExecutorService} will execute as many event-loop
	 * consuming the ringbuffer as subscribers.
	 *
	 * @param service    A provided ExecutorService to manage threading infrastructure
	 * @param autoCancel Should this propagate cancellation when unregistered by all subscribers ?
	 * @param <E>        Type of processed signals
	 * @return a fresh processor
	 */
	public static <E> RingBufferWorkProcessor<E> create(ExecutorService service, boolean autoCancel) {
		return create(service, SMALL_BUFFER_SIZE, new BlockingWaitStrategy(), autoCancel);
	}


	/**
	 * Create a new RingBufferProcessor using the passed buffer size, blockingWait Strategy
	 * and auto-cancel.
	 * <p>
	 * A new Cached ThreadExecutorPool will be implicitely created and will use the passed name to qualify
	 * the created threads.
	 *
	 * @param name       Use a new Cached ExecutorService and assign this name to the created threads
	 * @param bufferSize A Backlog Size to mitigate slow subscribers
	 * @param <E>        Type of processed signals
	 * @return a fresh processor
	 */
	public static <E> RingBufferWorkProcessor<E> create(String name, int bufferSize) {
		return create(name, bufferSize, new BlockingWaitStrategy(), true);
	}

	/**
	 * Create a new RingBufferProcessor using the passed buffer size, blockingWait Strategy
	 * and the passed auto-cancel setting.
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
	public static <E> RingBufferWorkProcessor<E> create(String name, int bufferSize, boolean autoCancel) {
		return create(name, bufferSize, new BlockingWaitStrategy(), autoCancel);
	}


	/**
	 * Create a new RingBufferProcessor using the passed buffer size, blockingWait Strategy
	 * and auto-cancel.
	 * <p>
	 * The passed {@link java.util.concurrent.ExecutorService} will execute as many event-loop
	 * consuming the ringbuffer as subscribers.
	 *
	 * @param service    A provided ExecutorService to manage threading infrastructure
	 * @param bufferSize A Backlog Size to mitigate slow subscribers
	 * @param <E>        Type of processed signals
	 * @return a fresh processor
	 */
	public static <E> RingBufferWorkProcessor<E> create(ExecutorService service, int bufferSize) {
		return create(service, bufferSize, new BlockingWaitStrategy(), true);
	}

	/**
	 * Create a new RingBufferWorkProcessor using the passed buffer size, blockingWait Strategy
	 * and auto-cancel.
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
	public static <E> RingBufferWorkProcessor<E> create(ExecutorService service, int bufferSize, boolean autoCancel) {
		return create(service, bufferSize, new BlockingWaitStrategy(), autoCancel);
	}


	/**
	 * Create a new RingBufferWorkProcessor using the passed buffer size, blockingWait Strategy
	 * and auto-cancel.
	 * <p>
	 * A new Cached ThreadExecutorPool will be implicitely created and will use the passed name to qualify
	 * the created threads.
	 *
	 * @param name       Use a new Cached ExecutorService and assign this name to the created threads
	 * @param bufferSize A Backlog Size to mitigate slow subscribers
	 * @param strategy   A RingBuffer WaitStrategy to use instead of the default BlockingWaitStrategy.
	 * @param <E>        Type of processed signals
	 * @return a fresh processor
	 */
	public static <E> RingBufferWorkProcessor<E> create(String name, int bufferSize, WaitStrategy
			strategy) {
		return create(name, bufferSize, strategy, true);
	}

	/**
	 * Create a new RingBufferWorkProcessor using the passed buffer size, blockingWait Strategy
	 * and auto-cancel settings.
	 * <p>
	 * A new Cached ThreadExecutorPool will be implicitely created and will use the passed name to qualify
	 * the created threads.
	 *
	 * @param name       Use a new Cached ExecutorService and assign this name to the created threads
	 * @param bufferSize A Backlog Size to mitigate slow subscribers
	 * @param strategy   A RingBuffer WaitStrategy to use instead of the default BlockingWaitStrategy.
	 * @param autoCancel Should this propagate cancellation when unregistered by all subscribers ?
	 * @param <E>        Type of processed signals
	 * @return a fresh processor
	 */
	public static <E> RingBufferWorkProcessor<E> create(String name, int bufferSize, WaitStrategy
			strategy, boolean autoCancel) {
		return new RingBufferWorkProcessor<E>(name, null, bufferSize, strategy, false, autoCancel);
	}

	/**
	 * Create a new RingBufferWorkProcessor using the passed buffer size and blockingWait Strategy settings
	 * but will auto-cancel.
	 * <p>
	 * The passed {@link java.util.concurrent.ExecutorService} will execute as many event-loop
	 * consuming the ringbuffer as subscribers.
	 *
	 * @param executor   A provided ExecutorService to manage threading infrastructure
	 * @param bufferSize A Backlog Size to mitigate slow subscribers
	 * @param strategy   A RingBuffer WaitStrategy to use instead of the default BlockingWaitStrategy.
	 * @param <E>        Type of processed signals
	 * @return a fresh processor
	 */
	public static <E> RingBufferWorkProcessor<E> create(ExecutorService executor, int bufferSize, WaitStrategy
			strategy) {
		return create(executor, bufferSize, strategy, true);
	}

	/**
	 * Create a new RingBufferWorkProcessor using the passed buffer size, wait strategy
	 * and auto-cancel settings.
	 * <p>
	 * The passed {@link java.util.concurrent.ExecutorService} will execute as many event-loop
	 * consuming the ringbuffer as subscribers.
	 *
	 * @param executor   A provided ExecutorService to manage threading infrastructure
	 * @param bufferSize A Backlog Size to mitigate slow subscribers
	 * @param strategy   A RingBuffer WaitStrategy to use instead of the default BlockingWaitStrategy.
	 * @param autoCancel Should this propagate cancellation when unregistered by all subscribers ?
	 * @param <E>        Type of processed signals
	 * @return a fresh processor
	 */
	public static <E> RingBufferWorkProcessor<E> create(ExecutorService executor, int bufferSize, WaitStrategy
			strategy, boolean autoCancel) {
		return new RingBufferWorkProcessor<E>(null, executor, bufferSize, strategy, false, autoCancel);
	}

	/**
	 * Create a new RingBufferWorkProcessor using {@link #SMALL_BUFFER_SIZE} backlog size, blockingWait Strategy
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
	public static <E> RingBufferWorkProcessor<E> share() {
		return share(RingBufferWorkProcessor.class.getSimpleName(), SMALL_BUFFER_SIZE, new
				BlockingWaitStrategy(), true);
	}

	/**
	 * Create a new RingBufferWorkProcessor using {@link #SMALL_BUFFER_SIZE} backlog size, blockingWait Strategy
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
	public static <E> RingBufferWorkProcessor<E> share(boolean autoCancel) {
		return share(RingBufferWorkProcessor.class.getSimpleName(), SMALL_BUFFER_SIZE, new
				BlockingWaitStrategy(), autoCancel);
	}

	/**
	 * Create a new RingBufferWorkProcessor using {@link #SMALL_BUFFER_SIZE} backlog size, blockingWait Strategy
	 * and auto-cancel.
	 * The passed {@link java.util.concurrent.ExecutorService} will execute as many event-loop
	 * consuming the ringbuffer as subscribers.
	 *
	 * @param service A provided ExecutorService to manage threading infrastructure
	 * @param <E>     Type of processed signals
	 * @return a fresh processor
	 */
	public static <E> RingBufferWorkProcessor<E> share(ExecutorService service) {
		return share(service, SMALL_BUFFER_SIZE, new BlockingWaitStrategy(), true);
	}

	/**
	 * Create a new RingBufferWorkProcessor using {@link #SMALL_BUFFER_SIZE} backlog size, blockingWait Strategy
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
	public static <E> RingBufferWorkProcessor<E> share(ExecutorService service, boolean autoCancel) {
		return share(service, SMALL_BUFFER_SIZE, new BlockingWaitStrategy(), autoCancel);
	}


	/**
	 * Create a new RingBufferProcessor using the passed buffer size, blockingWait Strategy
	 * and auto-cancel.
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
	public static <E> RingBufferWorkProcessor<E> share(String name, int bufferSize) {
		return share(name, bufferSize, new BlockingWaitStrategy(), true);
	}

	/**
	 * Create a new RingBufferProcessor using the passed buffer size, blockingWait Strategy
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
	 * @param autoCancel Should this propagate cancellation when unregistered by all subscribers ?
	 * @param <E>        Type of processed signals
	 * @return a fresh processor
	 */
	public static <E> RingBufferWorkProcessor<E> share(String name, int bufferSize, boolean autoCancel) {
		return share(name, bufferSize, new BlockingWaitStrategy(), autoCancel);
	}


	/**
	 * Create a new RingBufferProcessor using the passed buffer size, blockingWait Strategy
	 * and auto-cancel.
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
	public static <E> RingBufferWorkProcessor<E> share(ExecutorService service, int bufferSize) {
		return share(service, bufferSize, new BlockingWaitStrategy(), true);
	}

	/**
	 * Create a new RingBufferWorkProcessor using the passed buffer size, blockingWait Strategy
	 * and auto-cancel.
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
	public static <E> RingBufferWorkProcessor<E> share(ExecutorService service, int bufferSize, boolean autoCancel) {
		return share(service, bufferSize, new BlockingWaitStrategy(), autoCancel);
	}


	/**
	 * Create a new RingBufferWorkProcessor using the passed buffer size, blockingWait Strategy
	 * and auto-cancel.
	 * <p>
	 * A Shared Processor authorizes concurrent onNext calls and is suited for multi-threaded publisher that
	 * will fan-in data.
	 * <p>
	 * A new Cached ThreadExecutorPool will be implicitely created and will use the passed name to qualify
	 * the created threads.
	 *
	 * @param name       Use a new Cached ExecutorService and assign this name to the created threads
	 * @param bufferSize A Backlog Size to mitigate slow subscribers
	 * @param strategy   A RingBuffer WaitStrategy to use instead of the default BlockingWaitStrategy.
	 * @param <E>        Type of processed signals
	 * @return a fresh processor
	 */
	public static <E> RingBufferWorkProcessor<E> share(String name, int bufferSize, WaitStrategy
			strategy) {
		return share(name, bufferSize, strategy, true);
	}

	/**
	 * Create a new RingBufferWorkProcessor using the passed buffer size, blockingWait Strategy
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
	 * @param strategy   A RingBuffer WaitStrategy to use instead of the default BlockingWaitStrategy.
	 * @param autoCancel Should this propagate cancellation when unregistered by all subscribers ?
	 * @param <E>        Type of processed signals
	 * @return a fresh processor
	 */
	public static <E> RingBufferWorkProcessor<E> share(String name, int bufferSize, WaitStrategy
			strategy, boolean autoCancel) {
		return new RingBufferWorkProcessor<E>(name, null, bufferSize, strategy, true, autoCancel);
	}

	/**
	 * Create a new RingBufferWorkProcessor using the passed buffer size and blockingWait Strategy settings
	 * but will auto-cancel.
	 * <p>
	 * A Shared Processor authorizes concurrent onNext calls and is suited for multi-threaded publisher that
	 * will fan-in data.
	 * <p>
	 * The passed {@link java.util.concurrent.ExecutorService} will execute as many event-loop
	 * consuming the ringbuffer as subscribers.
	 *
	 * @param executor   A provided ExecutorService to manage threading infrastructure
	 * @param bufferSize A Backlog Size to mitigate slow subscribers
	 * @param strategy   A RingBuffer WaitStrategy to use instead of the default BlockingWaitStrategy.
	 * @param <E>        Type of processed signals
	 * @return a fresh processor
	 */
	public static <E> RingBufferWorkProcessor<E> share(ExecutorService executor, int bufferSize, WaitStrategy
			strategy) {
		return share(executor, bufferSize, strategy, true);
	}

	/**
	 * Create a new RingBufferWorkProcessor using the passed buffer size, wait strategy
	 * and auto-cancel settings.
	 * <p>
	 * A Shared Processor authorizes concurrent onNext calls and is suited for multi-threaded publisher that
	 * will fan-in data.
	 * <p>
	 * The passed {@link java.util.concurrent.ExecutorService} will execute as many event-loop
	 * consuming the ringbuffer as subscribers.
	 *
	 * @param executor   A provided ExecutorService to manage threading infrastructure
	 * @param bufferSize A Backlog Size to mitigate slow subscribers
	 * @param strategy   A RingBuffer WaitStrategy to use instead of the default BlockingWaitStrategy.
	 * @param autoCancel Should this propagate cancellation when unregistered by all subscribers ?
	 * @param <E>        Type of processed signals
	 * @return a fresh processor
	 */
	public static <E> RingBufferWorkProcessor<E> share(ExecutorService executor, int bufferSize, WaitStrategy
			strategy, boolean autoCancel) {
		return new RingBufferWorkProcessor<E>(null, executor, bufferSize, strategy, true, autoCancel);
	}


	/**
	 * Instance
	 */


	private final Sequence        workSequence       = new Sequence(Sequencer.INITIAL_CURSOR_VALUE);
	private final Sequence        pendingRequest     = new Sequence(0);
	private final Queue<Sequence> cancelledSequences = new ConcurrentLinkedQueue<>();

	private final SequenceBarrier              barrier;
	private final RingBuffer<MutableSignal<E>> ringBuffer;
	private final ExecutorService              executor;

	private RingBufferWorkProcessor(String name,
	                                ExecutorService executor,
	                                int bufferSize,
	                                WaitStrategy waitStrategy,
	                                boolean share,
	                                boolean autoCancel) {
		super(autoCancel);

		this.executor = executor == null
				? SingleUseExecutor.create(name)
				: executor;

		this.ringBuffer = RingBuffer.create(
				share ? ProducerType.MULTI : ProducerType.SINGLE,
				new EventFactory<MutableSignal<E>>() {
					@Override
					public MutableSignal<E> newInstance() {
						return new MutableSignal<E>();
					}
				},
				bufferSize,
				waitStrategy
		);

		ringBuffer.addGatingSequences(workSequence);

		this.barrier = ringBuffer.newBarrier();
	}

	@Override
	public void subscribe(final Subscriber<? super E> subscriber) {
		if (null == subscriber) {
			throw new NullPointerException("Cannot subscribe NULL subscriber");
		}
		try {
			final WorkSignalProcessor<E> signalProcessor = new WorkSignalProcessor<E>(
					subscriber,
					this
			);


			//set eventProcessor sequence to ringbuffer index
			signalProcessor.sequence.set(ringBuffer.getMinimumGatingSequence());

			//bind eventProcessor sequence to observe the ringBuffer
			ringBuffer.addGatingSequences(signalProcessor.sequence);

			//prepare the subscriber subscription to this processor
			signalProcessor.setSubscription(new RingBufferSubscription(subscriber, signalProcessor));

			//start the subscriber thread
			executor.execute(signalProcessor);

		} catch (Throwable t) {
			subscriber.onError(t);
		}
	}

	@Override
	public void onNext(E o) {
		RingBufferSubscriberUtils.onNext(o, ringBuffer);
	}

	@Override
	public void onError(Throwable t) {
		RingBufferSubscriberUtils.onError(t, ringBuffer);
	}

	@Override
	public void onComplete() {
		RingBufferSubscriberUtils.onComplete(ringBuffer);
		if (executor.getClass() == SingleUseExecutor.class) {
			executor.shutdown();
		}
	}

	@Override
	public String toString() {
		return "RingBufferWorkProcessor{" +
				"barrier=" + barrier +
				", ringBuffer=" + ringBuffer +
				", executor=" + executor +
				", workSequence=" + workSequence +
				", pendingRequest=" + pendingRequest +
				", cancelledSequence=" + cancelledSequences +
				'}';
	}

	private final class RingBufferSubscription implements Subscription {
		private final Subscriber<? super E> subscriber;
		private final WorkSignalProcessor   eventProcessor;

		public RingBufferSubscription(Subscriber<? super E> subscriber, WorkSignalProcessor eventProcessor) {
			this.subscriber = subscriber;
			this.eventProcessor = eventProcessor;
		}

		@Override
		public void request(long n) {
			if (n <= 0l) {
				subscriber.onError(SpecificationExceptions.spec_3_09_exception(n));
				return;
			}

			if (!eventProcessor.isRunning() || pendingRequest.get() == Long.MAX_VALUE) {
				return;
			}

			if (pendingRequest.addAndGet(n) < 0) {
				pendingRequest.set(Long.MAX_VALUE);
			}

			final Subscription parent = upstreamSubscription;
			if (parent != null) {
				parent.request(n);
			}
		}

		@Override
		public void cancel() {
			eventProcessor.halt();
			long current = workSequence.get();
			if (decrementSubscribers() && current != -1L) {
				long rewind = ringBuffer.getMinimumGatingSequence();
				workSequence.set(rewind);
			}
		}
	}

	@Override
	public long getCapacity() {
		return ringBuffer.getBufferSize();
	}

	/**
	 * Disruptor WorkProcessor port that deals with pending demand.
	 * <p>
	 * Convenience class for handling the batching semantics of consuming entries from a {@link com.lmax.disruptor
	 * .RingBuffer}
	 * and delegating the available events to an {@link com.lmax.disruptor.EventHandler}.
	 * <p>
	 * If the {@link com.lmax.disruptor.EventHandler} also implements {@link com.lmax.disruptor.LifecycleAware} it will
	 * be notified just after the thread
	 * is started and just before the thread is shutdown.
	 *
	 * @param <T> event implementation storing the data for sharing during exchange or parallel coordination of an event.
	 */
	private final static class WorkSignalProcessor<T> implements EventProcessor {

		private final AtomicBoolean running  = new AtomicBoolean(false);
		private final Sequence      sequence = new Sequence(Sequencer.INITIAL_CURSOR_VALUE);

		private final RingBufferWorkProcessor<T> processor;
		private final Subscriber<? super T>      subscriber;

		private Subscription subscription;

		/**
		 * Construct a {@link com.lmax.disruptor.EventProcessor} that will automatically track the progress by updating
		 * its
		 * sequence
		 */
		public WorkSignalProcessor(Subscriber<? super T> subscriber,
		                           RingBufferWorkProcessor<T> processor) {
			this.processor = processor;
			this.subscriber = subscriber;
		}

		public Subscription getSubscription() {
			return subscription;
		}

		public void setSubscription(Subscription subscription) {
			this.subscription = subscription;
		}

		@Override
		public Sequence getSequence() {
			return sequence;
		}

		@Override
		public void halt() {
			running.set(false);
			processor.barrier.alert();
		}

		@Override
		public boolean isRunning() {
			return running.get();
		}

		/**
		 * It is ok to have another thread rerun this method after a halt().
		 */
		@Override
		public void run() {
			if (!running.compareAndSet(false, true)) {
				subscriber.onError(new IllegalStateException("Thread is already running"));
				return;
			}

			processor.incrementSubscribers();

			subscriber.onSubscribe(subscription);


			boolean processedSequence = true;
			long cachedAvailableSequence = Long.MIN_VALUE;
			long nextSequence = sequence.get();
			MutableSignal<T> event;

			processor.barrier.clearAlert();

			if (replay()) {
				running.set(false);
				return;
			}

			while (true) {
				try {
					// if previous sequence was processed - fetch the next sequence and set
					// that we have successfully processed the previous sequence
					// typically, this will be true
					// this prevents the sequence getting too far forward if an exception
					// is thrown from the WorkHandler
					if (processedSequence) {
						processedSequence = false;
						do {
							nextSequence = processor.workSequence.get() + 1L;
							sequence.set(nextSequence - 1L);
						}
						while (!processor.workSequence.compareAndSet(nextSequence - 1L, nextSequence));
					}

					if (cachedAvailableSequence >= nextSequence) {
						event = processor.ringBuffer.get(nextSequence);

						readNextEvent(event);

						//It's an unbounded subscriber or there is enough capacity to process the signal
						RingBufferSubscriberUtils.routeOnce(event, subscriber);

						processedSequence = true;

					} else {
						cachedAvailableSequence = processor.barrier.waitFor(nextSequence);
					}

				} catch (CancelException ce) {
					sequence.set(nextSequence - 1L);
					processor.cancelledSequences.add(sequence);
					//processor.barrier.alert();
					break;
				} catch (AlertException ex) {

					if (!running.get()) {
						sequence.set(nextSequence - 1L);
						processor.cancelledSequences.add(sequence);
						break;

					} else {

						final long cursor = processor.barrier.getCursor();
						if (processor.ringBuffer.get(cursor).type == MutableSignal.Type.ERROR) {
							RingBufferSubscriberUtils.route(processor.ringBuffer.get(cursor), subscriber);
							break;
						} else {
							processor.barrier.clearAlert();
						}

						//continue event-loop
					}

				} catch (final Throwable ex) {
					subscriber.onError(ex);
					sequence.set(nextSequence);
					processedSequence = true;
				}
			}
			running.set(false);
		}

		private boolean replay() {
			Sequence replayedSequence;
			MutableSignal<T> signal;
			while ((replayedSequence = processor.cancelledSequences.poll()) != null) {
				signal = processor.ringBuffer.get(replayedSequence.get() + 1L);
				try {
					readNextEvent(signal);
					RingBufferSubscriberUtils.routeOnce(signal, subscriber);
					processor.ringBuffer.removeGatingSequence(replayedSequence);
				} catch (AlertException | CancelException ce) {
					processor.ringBuffer.removeGatingSequence(sequence);
					processor.cancelledSequences.add(replayedSequence);
					return true;
				}
			}
			return false;
		}

		private void readNextEvent(MutableSignal<T> event) throws AlertException {
			//if event is Next Signal we need to handle backpressure (pendingRequests)
			if (event.type == MutableSignal.Type.NEXT) {
				//if bounded and out of capacity
				if (processor.pendingRequest.get() != Long.MAX_VALUE && processor.pendingRequest.addAndGet(-1l) < 0l) {
					//re-add the retained capacity
					processor.pendingRequest.incrementAndGet();

					//if current sequence does not yet match the published one
					//if (nextSequence < cachedAvailableSequence) {
					//pause until request
					while (processor.pendingRequest.addAndGet(-1l) < 0l) {
						processor.pendingRequest.incrementAndGet();
						if (!running.get()) throw CancelException.INSTANCE;
						//Todo Use WaitStrategy?
						LockSupport.parkNanos(1l);
					}

				}
			} else if (event.type != null) {
				//Complete or Error are terminal events, we shutdown the processor and process the signal
				running.set(false);
				RingBufferSubscriberUtils.route(event, subscriber);
				if (event.type == MutableSignal.Type.ERROR) {
					processor.barrier.alert();
				}
				throw AlertException.INSTANCE;
			}
		}
	}

	@Override
	public long getAvailableCapacity() {
		return ringBuffer.remainingCapacity();
	}

}
