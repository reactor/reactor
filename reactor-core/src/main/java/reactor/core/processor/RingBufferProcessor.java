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

import java.util.concurrent.ExecutorService;
import java.util.concurrent.RejectedExecutionException;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.locks.LockSupport;

import org.reactivestreams.Publisher;
import org.reactivestreams.Subscriber;
import org.reactivestreams.Subscription;
import reactor.core.error.AlertException;
import reactor.core.error.CancelException;
import reactor.core.error.Exceptions;
import reactor.core.error.SpecificationExceptions;
import reactor.core.processor.rb.MutableSignal;
import reactor.core.processor.rb.RequestTask;
import reactor.core.processor.rb.RingBufferSequencer;
import reactor.core.processor.rb.RingBufferSubscriberUtils;
import reactor.core.processor.rb.disruptor.RingBuffer;
import reactor.core.processor.rb.disruptor.Sequence;
import reactor.core.processor.rb.disruptor.SequenceBarrier;
import reactor.core.processor.rb.disruptor.Sequencer;
import reactor.core.publisher.PublisherFactory;
import reactor.core.support.BackpressureUtils;
import reactor.core.support.NamedDaemonThreadFactory;
import reactor.core.support.Publishable;
import reactor.core.support.SignalType;
import reactor.core.support.wait.LiteBlockingWaitStrategy;
import reactor.core.support.wait.WaitStrategy;
import reactor.fn.Consumer;
import reactor.fn.LongSupplier;
import reactor.fn.Supplier;

/**
 * An implementation of a RingBuffer backed message-passing Processor. <p> The processor
 * respects the Reactive Streams contract and must not be signalled concurrently on any
 * onXXXX method. Each subscriber will be assigned a unique thread that will only stop on
 * terminal event: Complete, Error or Cancel. If Auto-Cancel is enabled, when all
 * subscribers are unregistered, a cancel signal is sent to the upstream Publisher if any.
 * Executor can be customized and will define how many concurrent subscribers are allowed
 * (fixed thread). When a Subscriber requests Long.MAX, there won't be any backpressure
 * applied and the producer will run at risk of being throttled if the subscribers don't
 * catch up. With any other strictly positive demand, a subscriber will stop reading new
 * Next signals (Complete and Error will still be read) as soon as the demand has been
 * fully consumed by the publisher. <p> When more than 1 subscriber listens to that
 * processor, they will all receive the exact same events if their respective demand is
 * still strictly positive, very much like a Fan-Out scenario. <p> When the backlog has
 * been completely booked and no subscribers is draining the signals, the publisher will
 * start throttling. In effect the smaller the backlog size is defined, the smaller the
 * difference in processing rate between subscribers must remain. Since the sequence for
 * each subscriber will point to various ringBuffer locations, the processor knows when a
 * backlog can't override the previously occupied slot.
 * @param <E> Type of dispatched signal
 * @author Stephane Maldini
 * @author Anatoly Kadyshev
 */
public final class RingBufferProcessor<E> extends ExecutorPoweredProcessor<E, E> {

	/**
	 * Create a new RingBufferProcessor using {@link #SMALL_BUFFER_SIZE} backlog size,
	 * blockingWait Strategy and auto-cancel. <p> A new Cached ThreadExecutorPool will be
	 * implicitely created.
	 * @param <E> Type of processed signals
	 * @return a fresh processor
	 */
	public static <E> RingBufferProcessor<E> create() {
		return create(RingBufferProcessor.class.getSimpleName(), SMALL_BUFFER_SIZE,
				new LiteBlockingWaitStrategy(), true);
	}

	/**
	 * Create a new RingBufferProcessor using {@link #SMALL_BUFFER_SIZE} backlog size,
	 * blockingWait Strategy and the passed auto-cancel setting. <p> A new Cached
	 * ThreadExecutorPool will be implicitely created.
	 * @param autoCancel Should this propagate cancellation when unregistered by all
	 * subscribers ?
	 * @param <E> Type of processed signals
	 * @return a fresh processor
	 */
	public static <E> RingBufferProcessor<E> create(boolean autoCancel) {
		return create(RingBufferProcessor.class.getSimpleName(), SMALL_BUFFER_SIZE,
				new LiteBlockingWaitStrategy(), autoCancel);
	}

	/**
	 * Create a new RingBufferProcessor using {@link #SMALL_BUFFER_SIZE} backlog size,
	 * blockingWait Strategy and auto-cancel. <p> The passed {@link
	 * java.util.concurrent.ExecutorService} will execute as many event-loop consuming the
	 * ringbuffer as subscribers.
	 * @param service A provided ExecutorService to manage threading infrastructure
	 * @param <E> Type of processed signals
	 * @return a fresh processor
	 */
	public static <E> RingBufferProcessor<E> create(ExecutorService service) {
		return create(service, SMALL_BUFFER_SIZE, new LiteBlockingWaitStrategy(), true);
	}

	/**
	 * Create a new RingBufferProcessor using {@link #SMALL_BUFFER_SIZE} backlog size,
	 * blockingWait Strategy and the passed auto-cancel setting. <p> The passed {@link
	 * java.util.concurrent.ExecutorService} will execute as many event-loop consuming the
	 * ringbuffer as subscribers.
	 * @param service A provided ExecutorService to manage threading infrastructure
	 * @param autoCancel Should this propagate cancellation when unregistered by all
	 * subscribers ?
	 * @param <E> Type of processed signals
	 * @return a fresh processor
	 */
	public static <E> RingBufferProcessor<E> create(ExecutorService service,
	                                                boolean autoCancel) {
		return create(service, SMALL_BUFFER_SIZE, new LiteBlockingWaitStrategy(),
				autoCancel);
	}

	/**
	 * Create a new RingBufferProcessor using {@link #SMALL_BUFFER_SIZE} backlog size,
	 * blockingWait Strategy and the passed auto-cancel setting. <p> A new Cached
	 * ThreadExecutorPool will be implicitely created and will use the passed name to
	 * qualify the created threads.
	 * @param name Use a new Cached ExecutorService and assign this name to the created
	 * threads
	 * @param bufferSize A Backlog Size to mitigate slow subscribers
	 * @param <E> Type of processed signals
	 */
	public static <E> RingBufferProcessor<E> create(String name, int bufferSize) {
		return create(name, bufferSize, new LiteBlockingWaitStrategy(), true);
	}

	/**
	 * Create a new RingBufferProcessor using the blockingWait Strategy, passed backlog
	 * size, and auto-cancel settings. <p> The passed {@link java.util.concurrent.ExecutorService}
	 * will execute as many event-loop consuming the ringbuffer as subscribers.
	 * @param name Use a new Cached ExecutorService and assign this name to the created
	 * threads
	 * @param bufferSize A Backlog Size to mitigate slow subscribers
	 * @param autoCancel Should this propagate cancellation when unregistered by all
	 * subscribers ?
	 * @param <E> Type of processed signals
	 * @return a fresh processor
	 */
	public static <E> RingBufferProcessor<E> create(String name, int bufferSize,
	                                                boolean autoCancel) {
		return create(name, bufferSize, new LiteBlockingWaitStrategy(), autoCancel);
	}

	/**
	 * Create a new RingBufferProcessor using passed backlog size, blockingWait Strategy
	 * and will auto-cancel. <p> The passed {@link java.util.concurrent.ExecutorService}
	 * will execute as many event-loop consuming the ringbuffer as subscribers.
	 * @param service A provided ExecutorService to manage threading infrastructure
	 * @param bufferSize A Backlog Size to mitigate slow subscribers
	 * @param <E> Type of processed signals
	 * @return a fresh processor
	 */
	public static <E> RingBufferProcessor<E> create(ExecutorService service,
	                                                int bufferSize) {
		return create(service, bufferSize, new LiteBlockingWaitStrategy(), true);
	}

	/**
	 * Create a new RingBufferProcessor using passed backlog size, blockingWait Strategy
	 * and the auto-cancel argument. <p> The passed {@link java.util.concurrent.ExecutorService}
	 * will execute as many event-loop consuming the ringbuffer as subscribers.
	 * @param service A provided ExecutorService to manage threading infrastructure
	 * @param bufferSize A Backlog Size to mitigate slow subscribers
	 * @param autoCancel Should this propagate cancellation when unregistered by all
	 * subscribers ?
	 * @param <E> Type of processed signals
	 * @return a fresh processor
	 */
	public static <E> RingBufferProcessor<E> create(ExecutorService service,
	                                                int bufferSize, boolean autoCancel) {
		return create(service, bufferSize, new LiteBlockingWaitStrategy(), autoCancel);
	}

	/**
	 * Create a new RingBufferProcessor using passed backlog size, wait strategy and will
	 * auto-cancel. <p> A new Cached ThreadExecutorPool will be implicitely created and
	 * will use the passed name to qualify the created threads.
	 * @param name Use a new Cached ExecutorService and assign this name to the created
	 * threads
	 * @param bufferSize A Backlog Size to mitigate slow subscribers
	 * @param strategy A RingBuffer WaitStrategy to use instead of the default
	 * BlockingWaitStrategy.
	 * @param <E> Type of processed signals
	 * @return a fresh processor
	 */
	public static <E> RingBufferProcessor<E> create(String name, int bufferSize,
	                                                WaitStrategy strategy) {
		return create(name, bufferSize, strategy, null);
	}

	/**
	 * Create a new RingBufferProcessor using passed backlog size, wait strategy, signal
	 * supplier. The created processor is not shared and will auto-cancel. <p> A new
	 * Cached ThreadExecutorPool will be implicitely created and will use the passed name
	 * to qualify the created threads.
	 * @param name Use a new Cached ExecutorService and assign this name to the created
	 * threads
	 * @param bufferSize A Backlog Size to mitigate slow subscribers
	 * @param strategy A RingBuffer WaitStrategy to use instead of the default
	 * BlockingWaitStrategy.
	 * @param signalSupplier A supplier of dispatched signals to preallocate in the ring
	 * buffer
	 * @param <E> Type of processed signals
	 * @return a fresh processor
	 */
	public static <E> RingBufferProcessor<E> create(String name, int bufferSize,
	                                                WaitStrategy strategy,
	                                                Supplier<E> signalSupplier) {
		return new RingBufferProcessor<E>(name, null, bufferSize, strategy, false, true,
				signalSupplier);
	}

	/**
	 * Create a new RingBufferProcessor using passed backlog size, wait strategy and
	 * auto-cancel settings. <p> A new Cached ThreadExecutorPool will be implicitely
	 * created and will use the passed name to qualify the created threads.
	 * @param name Use a new Cached ExecutorService and assign this name to the created
	 * threads
	 * @param bufferSize A Backlog Size to mitigate slow subscribers
	 * @param strategy A RingBuffer WaitStrategy to use instead of the default
	 * BlockingWaitStrategy.
	 * @param autoCancel Should this propagate cancellation when unregistered by all
	 * subscribers ?
	 * @param <E> Type of processed signals
	 * @return a fresh processor
	 */
	public static <E> RingBufferProcessor<E> create(String name, int bufferSize,
	                                                WaitStrategy strategy,
	                                                boolean autoCancel) {
		return new RingBufferProcessor<E>(name, null, bufferSize, strategy, false,
				autoCancel, null);
	}

	/**
	 * Create a new RingBufferProcessor using passed backlog size, wait strategy and will
	 * auto-cancel. <p> The passed {@link java.util.concurrent.ExecutorService} will
	 * execute as many event-loop consuming the ringbuffer as subscribers.
	 * @param service A provided ExecutorService to manage threading infrastructure
	 * @param bufferSize A Backlog Size to mitigate slow subscribers
	 * @param strategy A RingBuffer WaitStrategy to use instead of the default
	 * BlockingWaitStrategy.
	 * @param <E> Type of processed signals
	 * @return a fresh processor
	 */
	public static <E> RingBufferProcessor<E> create(ExecutorService service,
	                                                int bufferSize,
	                                                WaitStrategy strategy) {
		return create(service, bufferSize, strategy, true);
	}

	/**
	 * Create a new RingBufferProcessor using passed backlog size, wait strategy and
	 * auto-cancel settings. <p> The passed {@link java.util.concurrent.ExecutorService}
	 * will execute as many event-loop consuming the ringbuffer as subscribers.
	 * @param service A provided ExecutorService to manage threading infrastructure
	 * @param bufferSize A Backlog Size to mitigate slow subscribers
	 * @param strategy A RingBuffer WaitStrategy to use instead of the default
	 * BlockingWaitStrategy.
	 * @param autoCancel Should this propagate cancellation when unregistered by all
	 * subscribers ?
	 * @param <E> Type of processed signals
	 * @return a fresh processor
	 */
	public static <E> RingBufferProcessor<E> create(ExecutorService service,
	                                                int bufferSize, WaitStrategy strategy,
	                                                boolean autoCancel) {
		return new RingBufferProcessor<E>(null, service, bufferSize, strategy, false,
				autoCancel, null);
	}

	/**
	 * Create a new RingBufferProcessor using {@link #SMALL_BUFFER_SIZE} backlog size,
	 * blockingWait Strategy and auto-cancel. <p> A Shared Processor authorizes concurrent
	 * onNext calls and is suited for multi-threaded publisher that will fan-in data. <p>
	 * A new Cached ThreadExecutorPool will be implicitely created.
	 * @param <E> Type of processed signals
	 * @return a fresh processor
	 */
	public static <E> RingBufferProcessor<E> share() {
		return share(RingBufferProcessor.class.getSimpleName(), SMALL_BUFFER_SIZE,
				new LiteBlockingWaitStrategy(), true);
	}

	/**
	 * Create a new RingBufferProcessor using {@link #SMALL_BUFFER_SIZE} backlog size,
	 * blockingWait Strategy and the passed auto-cancel setting. <p> A Shared Processor
	 * authorizes concurrent onNext calls and is suited for multi-threaded publisher that
	 * will fan-in data. <p> A new Cached ThreadExecutorPool will be implicitely created.
	 * @param autoCancel Should this propagate cancellation when unregistered by all
	 * subscribers ?
	 * @param <E> Type of processed signals
	 * @return a fresh processor
	 */
	public static <E> RingBufferProcessor<E> share(boolean autoCancel) {
		return share(RingBufferProcessor.class.getSimpleName(), SMALL_BUFFER_SIZE,
				new LiteBlockingWaitStrategy(), autoCancel);
	}

	/**
	 * Create a new RingBufferProcessor using {@link #SMALL_BUFFER_SIZE} backlog size,
	 * blockingWait Strategy and auto-cancel. <p> A Shared Processor authorizes concurrent
	 * onNext calls and is suited for multi-threaded publisher that will fan-in data. <p>
	 * The passed {@link java.util.concurrent.ExecutorService} will execute as many
	 * event-loop consuming the ringbuffer as subscribers.
	 * @param service A provided ExecutorService to manage threading infrastructure
	 * @param <E> Type of processed signals
	 * @return a fresh processor
	 */
	public static <E> RingBufferProcessor<E> share(ExecutorService service) {
		return share(service, SMALL_BUFFER_SIZE, new LiteBlockingWaitStrategy(), true);
	}

	/**
	 * Create a new RingBufferProcessor using {@link #SMALL_BUFFER_SIZE} backlog size,
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
	public static <E> RingBufferProcessor<E> share(ExecutorService service,
	                                               boolean autoCancel) {
		return share(service, SMALL_BUFFER_SIZE, new LiteBlockingWaitStrategy(),
				autoCancel);
	}

	/**
	 * Create a new RingBufferProcessor using {@link #SMALL_BUFFER_SIZE} backlog size,
	 * blockingWait Strategy and the passed auto-cancel setting. <p> A Shared Processor
	 * authorizes concurrent onNext calls and is suited for multi-threaded publisher that
	 * will fan-in data. <p> A new Cached ThreadExecutorPool will be implicitely created
	 * and will use the passed name to qualify the created threads.
	 * @param name Use a new Cached ExecutorService and assign this name to the created
	 * threads
	 * @param bufferSize A Backlog Size to mitigate slow subscribers
	 * @param <E> Type of processed signals
	 * @return a fresh processor
	 */
	public static <E> RingBufferProcessor<E> share(String name, int bufferSize) {
		return share(name, bufferSize, new LiteBlockingWaitStrategy(), true);
	}

	/**
	 * Create a new RingBufferProcessor using the blockingWait Strategy, passed backlog
	 * size, and auto-cancel settings. <p> A Shared Processor authorizes concurrent onNext
	 * calls and is suited for multi-threaded publisher that will fan-in data. <p> The
	 * passed {@link java.util.concurrent.ExecutorService} will execute as many event-loop
	 * consuming the ringbuffer as subscribers.
	 * @param name Use a new Cached ExecutorService and assign this name to the created
	 * threads
	 * @param bufferSize A Backlog Size to mitigate slow subscribers
	 * @param autoCancel Should this propagate cancellation when unregistered by all
	 * subscribers ?
	 * @param <E> Type of processed signals
	 * @return a fresh processor
	 */
	public static <E> RingBufferProcessor<E> share(String name, int bufferSize,
	                                               boolean autoCancel) {
		return share(name, bufferSize, new LiteBlockingWaitStrategy(), autoCancel);
	}

	/**
	 * Create a new RingBufferProcessor using passed backlog size, blockingWait Strategy
	 * and will auto-cancel. <p> A Shared Processor authorizes concurrent onNext calls and
	 * is suited for multi-threaded publisher that will fan-in data. <p> The passed {@link
	 * java.util.concurrent.ExecutorService} will execute as many event-loop consuming the
	 * ringbuffer as subscribers.
	 * @param service A provided ExecutorService to manage threading infrastructure
	 * @param bufferSize A Backlog Size to mitigate slow subscribers
	 * @param <E> Type of processed signals
	 * @return a fresh processor
	 */
	public static <E> RingBufferProcessor<E> share(ExecutorService service,
	                                               int bufferSize) {
		return share(service, bufferSize, new LiteBlockingWaitStrategy(), true);
	}

	/**
	 * Create a new RingBufferProcessor using passed backlog size, blockingWait Strategy
	 * and the auto-cancel argument. <p> A Shared Processor authorizes concurrent onNext
	 * calls and is suited for multi-threaded publisher that will fan-in data. <p> The
	 * passed {@link java.util.concurrent.ExecutorService} will execute as many event-loop
	 * consuming the ringbuffer as subscribers.
	 * @param service A provided ExecutorService to manage threading infrastructure
	 * @param bufferSize A Backlog Size to mitigate slow subscribers
	 * @param autoCancel Should this propagate cancellation when unregistered by all
	 * subscribers ?
	 * @param <E> Type of processed signals
	 * @return a fresh processor
	 */
	public static <E> RingBufferProcessor<E> share(ExecutorService service,
	                                               int bufferSize, boolean autoCancel) {
		return share(service, bufferSize, new LiteBlockingWaitStrategy(), autoCancel);
	}

	/**
	 * Create a new RingBufferProcessor using passed backlog size, wait strategy and will
	 * auto-cancel. <p> A Shared Processor authorizes concurrent onNext calls and is
	 * suited for multi-threaded publisher that will fan-in data. <p> A new Cached
	 * ThreadExecutorPool will be implicitely created and will use the passed name to
	 * qualify the created threads.
	 * @param name Use a new Cached ExecutorService and assign this name to the created
	 * threads
	 * @param bufferSize A Backlog Size to mitigate slow subscribers
	 * @param strategy A RingBuffer WaitStrategy to use instead of the default
	 * BlockingWaitStrategy.
	 * @param <E> Type of processed signals
	 * @return a fresh processor
	 */
	public static <E> RingBufferProcessor<E> share(String name, int bufferSize,
	                                               WaitStrategy strategy) {
		return new RingBufferProcessor<E>(name, null, bufferSize, strategy, true, true,
				null);
	}

	/**
	 * Create a new RingBufferProcessor using passed backlog size, wait strategy and
	 * signal supplier. The created processor will auto-cancel and is shared. <p> A Shared
	 * Processor authorizes concurrent onNext calls and is suited for multi-threaded
	 * publisher that will fan-in data. <p> A new Cached ThreadExecutorPool will be
	 * implicitely created and will use the passed name to qualify the created threads.
	 * @param name Use a new Cached ExecutorService and assign this name to the created
	 * threads
	 * @param bufferSize A Backlog Size to mitigate slow subscribers
	 * @param signalSupplier A supplier of dispatched signals to preallocate in the ring
	 * buffer
	 * @param <E> Type of processed signals
	 * @return a fresh processor
	 */
	public static <E> RingBufferProcessor<E> share(String name, int bufferSize,
	                                               Supplier<E> signalSupplier) {
		return new RingBufferProcessor<E>(name, null, bufferSize,
				new LiteBlockingWaitStrategy(), true, true, signalSupplier);
	}

	/**
	 * Create a new RingBufferProcessor using passed backlog size, wait strategy and
	 * auto-cancel settings. <p> A Shared Processor authorizes concurrent onNext calls and
	 * is suited for multi-threaded publisher that will fan-in data. <p> A new Cached
	 * ThreadExecutorPool will be implicitely created and will use the passed name to
	 * qualify the created threads.
	 * @param name Use a new Cached ExecutorService and assign this name to the created
	 * threads
	 * @param bufferSize A Backlog Size to mitigate slow subscribers
	 * @param strategy A RingBuffer WaitStrategy to use instead of the default
	 * BlockingWaitStrategy.
	 * @param autoCancel Should this propagate cancellation when unregistered by all
	 * subscribers ?
	 * @param <E> Type of processed signals
	 * @return a fresh processor
	 */
	public static <E> RingBufferProcessor<E> share(String name, int bufferSize,
	                                               WaitStrategy strategy,
	                                               boolean autoCancel) {
		return new RingBufferProcessor<E>(name, null, bufferSize, strategy, true,
				autoCancel, null);
	}

	/**
	 * Create a new RingBufferProcessor using passed backlog size, wait strategy and will
	 * auto-cancel. <p> A Shared Processor authorizes concurrent onNext calls and is
	 * suited for multi-threaded publisher that will fan-in data. <p> The passed {@link
	 * java.util.concurrent.ExecutorService} will execute as many event-loop consuming the
	 * ringbuffer as subscribers.
	 * @param service A provided ExecutorService to manage threading infrastructure
	 * @param bufferSize A Backlog Size to mitigate slow subscribers
	 * @param strategy A RingBuffer WaitStrategy to use instead of the default
	 * BlockingWaitStrategy.
	 * @param <E> Type of processed signals
	 * @return a fresh processor
	 */
	public static <E> RingBufferProcessor<E> share(ExecutorService service,
	                                               int bufferSize,
	                                               WaitStrategy strategy) {
		return share(service, bufferSize, strategy, true);
	}

	/**
	 * Create a new RingBufferProcessor using passed backlog size, wait strategy and
	 * auto-cancel settings. <p> A Shared Processor authorizes concurrent onNext calls and
	 * is suited for multi-threaded publisher that will fan-in data. <p> The passed {@link
	 * java.util.concurrent.ExecutorService} will execute as many event-loop consuming the
	 * ringbuffer as subscribers.
	 * @param service A provided ExecutorService to manage threading infrastructure
	 * @param bufferSize A Backlog Size to mitigate slow subscribers
	 * @param strategy A RingBuffer WaitStrategy to use instead of the default
	 * BlockingWaitStrategy.
	 * @param autoCancel Should this propagate cancellation when unregistered by all
	 * subscribers ?
	 * @param <E> Type of processed signals
	 * @return a fresh processor
	 */
	public static <E> RingBufferProcessor<E> share(ExecutorService service,
	                                               int bufferSize, WaitStrategy strategy,
	                                               boolean autoCancel) {
		return new RingBufferProcessor<E>(null, service, bufferSize, strategy, true,
				autoCancel, null);
	}

	private final SequenceBarrier barrier;

	private final RingBuffer<MutableSignal<E>> ringBuffer;

	private final Sequence minimum;

	private final WaitStrategy readWait = new LiteBlockingWaitStrategy();

	private RingBufferProcessor(String name, ExecutorService executor, int bufferSize,
	                            WaitStrategy waitStrategy, boolean shared,
	                            boolean autoCancel, final Supplier<E> signalSupplier) {
		super(name, executor, autoCancel);

		Supplier<MutableSignal<E>> factory = new Supplier<MutableSignal<E>>() {
			@Override
			public MutableSignal<E> get() {
				MutableSignal<E> signal = new MutableSignal<>();
				if (signalSupplier != null) {
					signal.value = signalSupplier.get();
				}
				return signal;
			}
		};

		Consumer<Void> spinObserver = new Consumer<Void>() {
			@Override
			public void accept(Void aVoid) {
				if (!alive() && SUBSCRIBER_COUNT.get(RingBufferProcessor.this) == 0) {
					throw CancelException.get();
				}
			}
		};

		if (shared) {
			this.ringBuffer = RingBuffer
					.createMultiProducer(factory, bufferSize, waitStrategy, spinObserver);
		}
		else {
			this.ringBuffer = RingBuffer
					.createSingleProducer(factory, bufferSize, waitStrategy,
							spinObserver);
		}

		this.minimum = Sequencer.newSequence(-1);
		this.barrier = ringBuffer.newBarrier();
	}

	@Override
	public void subscribe(final Subscriber<? super E> subscriber) {
		if (null == subscriber) {
			throw SpecificationExceptions.spec_2_13_exception();
		}

		if (!alive()) {
			RingBufferSequencer<E> sequencer = coldSource();
			PublisherFactory.create(sequencer, sequencer).subscribe(subscriber);
			return;
		}

		//create a unique eventProcessor for this subscriber
		final Sequence pendingRequest = Sequencer.newSequence(0);
		final BatchSignalProcessor<E> signalProcessor =
				new BatchSignalProcessor<E>(this, pendingRequest, subscriber);

		//bind eventProcessor sequence to observe the ringBuffer

		//if only active subscriber, replay missed data
		if (incrementSubscribers()) {

			ringBuffer.addGatingSequences(signalProcessor.sequence);
			//set eventProcessor sequence to minimum index (replay)
			signalProcessor.sequence.setVolatile(minimum.get());
		}
		else {
			//otherwise only listen to new data
			//set eventProcessor sequence to ringbuffer index
			ringBuffer.addGatingSequences(signalProcessor.sequence);


		}

		//prepare the subscriber subscription to this processor
		signalProcessor.setSubscription(
				new RingBufferSubscription(pendingRequest, subscriber, signalProcessor));

		try {
			//start the subscriber thread
			executor.execute(signalProcessor);

		}
		catch (Throwable t) {
			ringBuffer.removeGatingSequence(signalProcessor.getSequence());
			decrementSubscribers();
			if (!alive() && RejectedExecutionException.class.isAssignableFrom(t.getClass())){
				RingBufferSequencer<E> sequencer = new RingBufferSequencer<E>(ringBuffer, minimum.get());
				PublisherFactory.create(sequencer, sequencer).subscribe(subscriber);
			}
			else{
				Exceptions.<E>publisher(t).subscribe(subscriber);
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
		readWait.signalAllWhenBlocking();
	}

	@Override
	protected void doComplete() {
		RingBufferSubscriberUtils.onComplete(ringBuffer);
		readWait.signalAllWhenBlocking();
	}

	protected RingBufferSequencer<E> coldSource(){
		return new RingBufferSequencer<E>(
				ringBuffer, minimum.get()
		);
	}

	@Override
	public boolean isWork() {
		return false;
	}

	RingBuffer<MutableSignal<E>> ringBuffer() {
		return ringBuffer;
	}

	public Publisher<Void> writeWith(final Publisher<? extends E> source) {
		return RingBufferSubscriberUtils.writeWith(source, ringBuffer);
	}

	@Override
	protected void requestTask(Subscription s) {
		ringBuffer.addGatingSequences(minimum);
		new NamedDaemonThreadFactory("ringbuffer-request-task", null, null, false)
				.newThread(new RequestTask(s, new Consumer<Void>() {
					@Override
					public void accept(Void aVoid) {
						if (!alive()) {
							if(cancelled){
								throw CancelException.INSTANCE;
							}
							else {
								throw AlertException.INSTANCE;
							}
						}
					}
				}, new Consumer<Long>() {
					@Override
					public void accept(Long newMin) {
						minimum.set(newMin);
					}
				}, new LongSupplier() {
					@Override
					public long get() {
						return SUBSCRIBER_COUNT.get(RingBufferProcessor.this) == 0 ?
								minimum.get() :
								ringBuffer.getMinimumGatingSequence(minimum);
					}
				}, readWait, this, ringBuffer)).start();
	}

	@Override
	protected void cancel(Subscription subscription) {
		super.cancel(subscription);
		readWait.signalAllWhenBlocking();
	}

	@Override
	public String toString() {
		return "RingBufferProcessor{" +
				"barrier=" + barrier +
				", remaining=" + ringBuffer.remainingCapacity() +
				'}';
	}

	@Override
	public long getAvailableCapacity() {
		return ringBuffer.remainingCapacity();
	}

	private final class RingBufferSubscription implements Subscription, Publishable<E> {

		private final Sequence pendingRequest;

		private final Subscriber<? super E> subscriber;

		private final BatchSignalProcessor<E> eventProcessor;

		public RingBufferSubscription(Sequence pendingRequest,
		                              Subscriber<? super E> subscriber,
		                              BatchSignalProcessor<E> eventProcessor) {
			this.subscriber = subscriber;
			this.eventProcessor = eventProcessor;
			this.pendingRequest = pendingRequest;
		}

		@Override
		public Publisher<E> upstream() {
			return RingBufferProcessor.this;
		}

		@Override
		@SuppressWarnings("unchecked")
		public void request(long n) {
			if (BackpressureUtils.checkRequest(n, subscriber)) {
				if (!eventProcessor.isRunning()) {
					return;
				}

				BackpressureUtils.getAndAdd(pendingRequest, n);
			}
		}

		@Override
		public void cancel() {
			eventProcessor.halt();
		}

		@Override
		public String toString() {
			return "RingBufferSubscription{" +
					"pendingRequest=" + pendingRequest +
					", sequence=" + eventProcessor.sequence +
					", ringBuffer=" + ringBuffer.getCursor() +
					'}';
		}
	}

	@Override
	public long getCapacity() {
		return ringBuffer.getBufferSize();
	}

	/**
	 * Get the remaining capacity for the ring buffer
	 * @return number of remaining slots
	 */
	public long remainingCapacity() {
		return ringBuffer.remainingCapacity();
	}

	/**
	 * Disruptor BatchEventProcessor port that deals with pending demand. <p> Convenience
	 * class for handling the batching semantics of consuming entries from a {@link
	 * reactor.core.processor .rb.disruptor .RingBuffer}. <p>
	 * @param <T> event implementation storing the data for sharing during exchange or
	 * parallel coordination of an event.
	 */
	private final static class BatchSignalProcessor<T> implements Runnable {

		private final AtomicBoolean running = new AtomicBoolean(false);

		private final Sequence sequence =
				Sequencer.newSequence(Sequencer.INITIAL_CURSOR_VALUE);

		private final RingBufferProcessor<T> processor;

		private final Sequence pendingRequest;

		private final Subscriber<? super T> subscriber;

		private Subscription subscription;

		/**
		 * Construct a ringbuffer consumer that will automatically track the progress by
		 * updating its sequence
		 */
		public BatchSignalProcessor(RingBufferProcessor<T> processor,
		                            Sequence pendingRequest,
		                            Subscriber<? super T> subscriber) {
			this.processor = processor;
			this.pendingRequest = pendingRequest;
			this.subscriber = subscriber;
		}

		public Subscription getSubscription() {
			return subscription;
		}

		public void setSubscription(Subscription subscription) {
			this.subscription = subscription;
		}

		public Sequence getSequence() {
			return sequence;
		}

		public void halt() {
			running.set(false);
			processor.barrier.alert();
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
					Exceptions.<T>publisher(
							new IllegalStateException("Thread is already running"))
					          .subscribe(subscriber);
					return;
				}

				try {
					subscriber.onSubscribe(subscription);
				}
				catch (Throwable t) {
					Exceptions.<T>publisher(t).subscribe(subscriber);
					return;
				}

				if (!RingBufferSubscriberUtils
						.waitRequestOrTerminalEvent(pendingRequest, processor.ringBuffer,
								processor.barrier, subscriber, running, sequence, null)) {
					return;
				}

				MutableSignal<T> event = null;
				long nextSequence = sequence.get() + 1L;
				final boolean unbounded = pendingRequest.get() == Long.MAX_VALUE;

				while (true) {
					try {

						final long availableSequence =
								processor.barrier.waitFor(nextSequence);
						while (nextSequence <= availableSequence) {
							event = processor.ringBuffer.get(nextSequence);

							//if event is Next Signal we need to handle backpressure (pendingRequests)
							if (event.type == SignalType.NEXT) {
								//if bounded and out of capacity
								while (!unbounded &&
										BackpressureUtils.getAndSub(pendingRequest, 1L) ==
												0) {
									//Todo Use WaitStrategy?
									processor.barrier.checkAlert();
									LockSupport.parkNanos(1l);
								}

								//It's an unbounded subscriber or there is enough capacity to process the signal
								RingBufferSubscriberUtils.route(event, subscriber);
								nextSequence++;
							}
							else {
								//Complete or Error are terminal events, we shutdown the processor and process the
								// signal
								running.set(false);
								RingBufferSubscriberUtils.route(event, subscriber);
								//only alert on error (immediate), complete will be drained as usual with waitFor
								if (event.type == SignalType.ERROR) {
									processor.barrier.alert();
								}
								throw AlertException.INSTANCE;
							}
						}
						sequence.set(availableSequence);
						if (SignalType.NOOP_SUBSCRIPTION !=
								processor.upstreamSubscription) {
							processor.readWait.signalAllWhenBlocking();
						}
					}
					catch (final AlertException | CancelException ex) {
						if (!running.get()) {
							break;
						}
						else {
							long cursor = processor.barrier.getCursor();
							if (processor.ringBuffer.get(cursor).type ==
									SignalType.ERROR) {
								nextSequence = cursor;
							}
							else {
								nextSequence = nextSequence - 1;
							}
							processor.barrier.clearAlert();
						}
					}
					catch (final Throwable ex) {
						Exceptions.throwIfFatal(ex);
						subscriber.onError(ex);
						sequence.set(nextSequence);
						nextSequence++;
					}
				}
			}
			finally {
				processor.ringBuffer.removeGatingSequence(sequence);
				processor.decrementSubscribers();
				running.set(false);
				processor.readWait.signalAllWhenBlocking();
			}
		}
	}

}
