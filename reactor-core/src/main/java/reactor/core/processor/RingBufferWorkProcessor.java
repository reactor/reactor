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
import reactor.core.support.NamedDaemonThreadFactory;
import reactor.core.support.SpecificationExceptions;
import reactor.jarjar.com.lmax.disruptor.*;
import reactor.jarjar.com.lmax.disruptor.dsl.ProducerType;

import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.locks.LockSupport;

/**
 * An implementation of a RingBuffer backed message-passing WorkProcessor.
 *
 * The processor is very similar to {@link reactor.core.processor.RingBufferProcessor} but only partially respects the
 * Reactive Streams contract.
 *
 * The purpose of this processor is to distribute the signals to only one of the subscribed subscribers and to share the
 * demand amongst all subscribers. The scenario is akin to Executor or Round-Robin distribution. However there is
 * no guarantee the distribution will be respecting a round-robin distribution all the time.
 *
 * The core use for this component is to scale up easily without suffering the overhead of an Executor and without using
 *  dedicated queues by subscriber, which is less used memory, less GC, more win.
 *
 * @param <E> Type of dispatched signal
 * @author Stephane Maldini
 */
public final class RingBufferWorkProcessor<E> extends ReactorProcessor<E> {
	private final SequenceBarrier              barrier;
	private final RingBuffer<MutableSignal<E>> ringBuffer;
	private final ExecutorService              executor;
	private final Sequence workSequence   = new Sequence(Sequencer.INITIAL_CURSOR_VALUE);
	private final Sequence pendingRequest = new Sequence(0);


	/**
	 * Create a new RingBufferWorkProcessor using {@link #SMALL_BUFFER_SIZE} backlog size, blockingWait Strategy
	 * and auto-cancel.
	 *
	 * A new Cached ThreadExecutorPool will be implicitely created.
	 *
	 * @param <E> Type of processed signals
	 *
	 * @return a fresh processor
	 */
	public static <E> RingBufferWorkProcessor<E> create() {
		return create(RingBufferWorkProcessor.class.getSimpleName(), SMALL_BUFFER_SIZE, new
				BlockingWaitStrategy(), true);
	}

	/**
	 * Create a new RingBufferWorkProcessor using {@link #SMALL_BUFFER_SIZE} backlog size, blockingWait Strategy
	 * and the passed auto-cancel setting.
	 *
	 * A new Cached ThreadExecutorPool will be implicitely created.
	 *
	 * @param autoCancel Should this propagate cancellation when unregistered by all subscribers ?
	 * @param <E> Type of processed signals
	 *
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
	 * @param <E> Type of processed signals
	 *
	 * @return a fresh processor
	 */
	public static <E> RingBufferWorkProcessor<E> create(ExecutorService service) {
		return create(service, SMALL_BUFFER_SIZE, new BlockingWaitStrategy(), true);
	}

	/**
	 * Create a new RingBufferWorkProcessor using {@link #SMALL_BUFFER_SIZE} backlog size, blockingWait Strategy
	 * and the passed auto-cancel setting.
	 *
	 * The passed {@link java.util.concurrent.ExecutorService} will execute as many event-loop
	 * consuming the ringbuffer as subscribers.
	 *
	 * @param service A provided ExecutorService to manage threading infrastructure
	 * @param autoCancel Should this propagate cancellation when unregistered by all subscribers ?
	 * @param <E> Type of processed signals
	 *
	 * @return a fresh processor
	 */
	public static <E> RingBufferWorkProcessor<E> create(ExecutorService service, boolean autoCancel) {
		return create(service, SMALL_BUFFER_SIZE, new BlockingWaitStrategy(), autoCancel);
	}


	/**
	 * Create a new RingBufferProcessor using the passed buffer size, blockingWait Strategy
	 * and auto-cancel.
	 *
	 * A new Cached ThreadExecutorPool will be implicitely created and will use the passed name to qualify
	 * the created threads.
	 *
	 * @param name Use a new Cached ExecutorService and assign this name to the created threads
	 * @param bufferSize A Backlog Size to mitigate slow subscribers
	 * @param <E> Type of processed signals
	 *
	 * @return a fresh processor
	 */
	public static <E> RingBufferWorkProcessor<E> create(String name, int bufferSize) {
		return create(name, bufferSize, new BlockingWaitStrategy(), true);
	}

	/**
	 * Create a new RingBufferProcessor using the passed buffer size, blockingWait Strategy
	 * and the passed auto-cancel setting.
	 *
	 * A new Cached ThreadExecutorPool will be implicitely created and will use the passed name to qualify
	 * the created threads.
	 *
	 * @param name Use a new Cached ExecutorService and assign this name to the created threads
	 * @param bufferSize A Backlog Size to mitigate slow subscribers
	 * @param autoCancel Should this propagate cancellation when unregistered by all subscribers ?
	 * @param <E> Type of processed signals
	 *
	 * @return a fresh processor
	 */
	public static <E> RingBufferWorkProcessor<E> create(String name, int bufferSize, boolean autoCancel) {
		return create(name, bufferSize, new BlockingWaitStrategy(), autoCancel);
	}


	/**
	 * Create a new RingBufferProcessor using the passed buffer size, blockingWait Strategy
	 * and auto-cancel.
	 *
	 * The passed {@link java.util.concurrent.ExecutorService} will execute as many event-loop
	 * consuming the ringbuffer as subscribers.
	 *
	 * @param service A provided ExecutorService to manage threading infrastructure
	 * @param bufferSize A Backlog Size to mitigate slow subscribers
	 * @param <E> Type of processed signals
	 *
	 * @return a fresh processor
	 */
	public static <E> RingBufferWorkProcessor<E> create(ExecutorService service, int bufferSize) {
		return create(service, bufferSize, new BlockingWaitStrategy(), true);
	}

	/**
	 * Create a new RingBufferWorkProcessor using the passed buffer size, blockingWait Strategy
	 * and auto-cancel.
	 *
	 * The passed {@link java.util.concurrent.ExecutorService} will execute as many event-loop
	 * consuming the ringbuffer as subscribers.

	 * @param service A provided ExecutorService to manage threading infrastructure
	 * @param bufferSize A Backlog Size to mitigate slow subscribers
	 * @param autoCancel Should this propagate cancellation when unregistered by all subscribers ?
	 * @param <E> Type of processed signals
	 *
	 * @return a fresh processor
	 */
	public static <E> RingBufferWorkProcessor<E> create(ExecutorService service, int bufferSize, boolean autoCancel) {
		return create(service, bufferSize, new BlockingWaitStrategy(), autoCancel);
	}


	/**
	 * Create a new RingBufferWorkProcessor using the passed buffer size, blockingWait Strategy
	 * and auto-cancel.
	 *
	 * A new Cached ThreadExecutorPool will be implicitely created and will use the passed name to qualify
	 * the created threads.
	 *
	 * @param name Use a new Cached ExecutorService and assign this name to the created threads
	 * @param bufferSize A Backlog Size to mitigate slow subscribers
	 * @param strategy A RingBuffer WaitStrategy to use instead of the default BlockingWaitStrategy.
	 * @param <E> Type of processed signals
	 *
	 * @return a fresh processor
	 */
	public static <E> RingBufferWorkProcessor<E> create(String name, int bufferSize, WaitStrategy
			strategy) {
		return create(name, bufferSize, strategy, true);
	}

	/**
	 * Create a new RingBufferWorkProcessor using the passed buffer size, blockingWait Strategy
	 * and auto-cancel settings.
	 *
	 * A new Cached ThreadExecutorPool will be implicitely created and will use the passed name to qualify
	 * the created threads.
	 *
	 * @param name Use a new Cached ExecutorService and assign this name to the created threads
	 * @param bufferSize A Backlog Size to mitigate slow subscribers
	 * @param strategy A RingBuffer WaitStrategy to use instead of the default BlockingWaitStrategy.
	 * @param autoCancel Should this propagate cancellation when unregistered by all subscribers ?
	 * @param <E> Type of processed signals
	 *
	 * @return a fresh processor
	 */
	public static <E> RingBufferWorkProcessor<E> create(String name, int bufferSize, WaitStrategy
			strategy, boolean autoCancel) {
		return new RingBufferWorkProcessor<E>(name, null, bufferSize, strategy, autoCancel);
	}

	/**
	 * Create a new RingBufferWorkProcessor using the passed buffer size and blockingWait Strategy settings
	 * but will auto-cancel.
	 *
	 * The passed {@link java.util.concurrent.ExecutorService} will execute as many event-loop
	 * consuming the ringbuffer as subscribers.

	 * @param executor A provided ExecutorService to manage threading infrastructure
	 * @param bufferSize A Backlog Size to mitigate slow subscribers
	 * @param strategy A RingBuffer WaitStrategy to use instead of the default BlockingWaitStrategy.
	 * @param <E> Type of processed signals
	 *
	 * @return a fresh processor
	 */
	public static <E> RingBufferWorkProcessor<E> create(ExecutorService executor, int bufferSize, WaitStrategy
			strategy) {
		return create(executor, bufferSize, strategy, true);
	}

	/**
	 * Create a new RingBufferWorkProcessor using the passed buffer size, wait strategy
	 * and auto-cancel settings.
	 *
	 * The passed {@link java.util.concurrent.ExecutorService} will execute as many event-loop
	 * consuming the ringbuffer as subscribers.
	 *
	 * @param executor A provided ExecutorService to manage threading infrastructure
	 * @param bufferSize A Backlog Size to mitigate slow subscribers
	 * @param strategy A RingBuffer WaitStrategy to use instead of the default BlockingWaitStrategy.
	 * @param autoCancel Should this propagate cancellation when unregistered by all subscribers ?
	 * @param <E> Type of processed signals
	 *
	 * @return a fresh processor
	 */
	public static <E> RingBufferWorkProcessor<E> create(ExecutorService executor, int bufferSize, WaitStrategy
			strategy, boolean autoCancel) {
		return new RingBufferWorkProcessor<E>(null, executor, bufferSize, strategy, autoCancel);
	}

	private RingBufferWorkProcessor(String name, ExecutorService executor,
	                                int bufferSize,
	                                WaitStrategy waitStrategy, boolean autoCancel) {
		super(autoCancel);

		this.executor = executor == null ?
				Executors.newCachedThreadPool(new NamedDaemonThreadFactory(name, context)) :
				executor;


		this.ringBuffer = RingBuffer.create(
				ProducerType.SINGLE,
				new EventFactory<MutableSignal<E>>() {
					@Override
					public MutableSignal<E> newInstance() {
						return new MutableSignal<E>(SType.NEXT, null, null);
					}
				},
				bufferSize,
				waitStrategy
		);

		ringBuffer.addGatingSequences(workSequence);

		this.barrier = ringBuffer.newBarrier();
	}

	@Override
	public void subscribe(final Subscriber<? super E> sub) {
		if (null == sub) {
			throw new NullPointerException("Cannot subscribe NULL subscriber");
		}
		try {

			final InnerWorkProcessor<E> p = new InnerWorkProcessor<E>(
					ringBuffer,
					barrier,
					pendingRequest,
					workSequence,
					sub
			);

			//set eventProcessor sequence to ringbuffer index
			//p.getSequence().set(workSequence.get());
			p.sequence.set(ringBuffer.getCursor());

			//bind eventProcessor sequence to observe the ringBuffer
			ringBuffer.addGatingSequences(p.sequence);

			//prepare the subscriber subscription to this processor
			p.s = new RingBufferSubscription(sub, p);

			//start the subscriber thread
			incrementSubscribers();
			executor.execute(p);

		} catch (Throwable t) {
			sub.onError(t);
		}
	}

	@Override
	public void onNext(E o) {
		if (o == null) {
			throw new NullPointerException("Spec 2.13: Signal cannot be null");
		}
		/*	if(context == Thread.currentThread().getContextClassLoader()){

		}*/
		final long seqId = ringBuffer.next();
		final MutableSignal<E> signal = ringBuffer.get(seqId);
		signal.value = o;
		signal.throwable = null;
		signal.type = SType.NEXT;

		ringBuffer.publish(seqId);
	}

	@Override
	public void onError(Throwable t) {
		if (t == null) {
			throw new NullPointerException("Spec 2.13: Signal cannot be null");
		}
		long seqId = ringBuffer.next();
		MutableSignal<E> signal = ringBuffer.get(seqId);
		signal.throwable = t;
		signal.value = null;
		signal.type = SType.ERROR;
		ringBuffer.publish(seqId);
	}

	@Override
	public void onComplete() {
		long seqId = ringBuffer.next();
		MutableSignal<E> signal = ringBuffer.get(seqId);
		signal.throwable = null;
		signal.value = null;
		signal.type = SType.COMPLETE;
		ringBuffer.publish(seqId);
	}

	@Override
	public String toString() {
		return "RingBufferSubscriber{" +
				", barrier=" + barrier.getCursor() +
				'}';
	}


	private final class RingBufferSubscription implements Subscription {

		private final Subscriber<? super E> sub;
		private final EventProcessor        p;

		public RingBufferSubscription(Subscriber<? super E> sub, EventProcessor p) {
			this.sub = sub;
			this.p = p;
		}

		@Override
		@SuppressWarnings("unchecked")
		public void request(long n) {
			if (n <= 0l) {
				sub.onError(SpecificationExceptions.spec_3_09_exception(n));
				return;
			}

			if (!p.isRunning() || pendingRequest.get() == Long.MAX_VALUE) {
				return;
			}

			Subscription parent = upstreamSubscription;

			if (pendingRequest.addAndGet(n) < 0) pendingRequest.set(Long.MAX_VALUE);

			if (parent != null) {
				parent.request(n);
			}


		}

		@Override
		public void cancel() {
			try {
				p.halt();
			} finally {
				decrementSubscribers();
			}
		}
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
	private final static class InnerWorkProcessor<T> implements EventProcessor {

		private final AtomicBoolean running = new AtomicBoolean(false);
		private final RingBuffer<MutableSignal<T>> dataProvider;
		private final SequenceBarrier                sequenceBarrier;
		private final Sequence sequence = new Sequence(Sequencer.INITIAL_CURSOR_VALUE);
		private final Subscriber<? super T> sub;
		private final Sequence              pendingRequest;
		private final Sequence              workSequence;
		Subscription s;


		/**
		 * Construct a {@link com.lmax.disruptor.EventProcessor} that will automatically track the progress by updating
		 * its
		 * sequence when
		 * the {@link com.lmax.disruptor.EventHandler#onEvent(Object, long, boolean)} method returns.
		 *
		 * @param dataProvider    to which events are published.
		 * @param sequenceBarrier on which it is waiting.
		 */
		public InnerWorkProcessor(final RingBuffer<MutableSignal<T>> dataProvider,
		                          SequenceBarrier sequenceBarrier,
		                          Sequence pendingRequest,
		                          Sequence workSequence,
		                          Subscriber<? super T> sub) {
			this.sub = sub;
			this.pendingRequest = pendingRequest;
			this.workSequence = workSequence;
			this.dataProvider = dataProvider;
			this.sequenceBarrier = sequenceBarrier;
		}

		@Override
		public Sequence getSequence() {
			return sequence;
		}

		@Override
		public void halt() {
			running.set(false);
			sequenceBarrier.alert();
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
				sub.onError(new IllegalStateException("Thread is already running"));
				return;
			} else {
				sequenceBarrier.clearAlert();
				sub.onSubscribe(s);
			}

			boolean processedSequence = true;
			long cachedAvailableSequence = Long.MIN_VALUE;
			long nextSequence = sequence.get();
			MutableSignal<T> event = null;
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
							nextSequence = workSequence.get() + 1L;
							sequence.set(nextSequence - 1L);
						}
						while (!workSequence.compareAndSet(nextSequence - 1L, nextSequence));
					}

					if (cachedAvailableSequence >= nextSequence) {
						event = dataProvider.get(nextSequence);

						//if event is Next Signal we need to handle backpressure (pendingRequests)
						if (event.type == SType.NEXT) {
							//if bounded and out of capacity
							if (pendingRequest.get() != Long.MAX_VALUE && pendingRequest.addAndGet(-1l) < 0l) {
								//re-add the retained capacity
								pendingRequest.incrementAndGet();

								//if current sequence does not yet match the published one
								if (nextSequence < cachedAvailableSequence) {

									//look ahead if the published event was a terminal signal
									if (dataProvider.get(nextSequence + 1l).type != SType.NEXT) {
										//terminate
										running.set(false);
										//process last signal
										RingBufferProcessor.route(dataProvider.get(nextSequence + 1l), sub);
										//short-circuit
										throw AlertException.INSTANCE;
									}

									//pause until request
									while (pendingRequest.get() <= 0l) {
										//Todo Use WaitStrategy?
										sequenceBarrier.checkAlert();
										LockSupport.parkNanos(1l);
									}
									pendingRequest.incrementAndGet();
								} else {
									//end-of-loop without processing and incrementing the nextSequence
									break;
								}
							}
						} else {
							//Complete or Error are terminal events, we shutdown the processor and process the signal
							running.set(false);
							RingBufferProcessor.route(event, sub);
							sequenceBarrier.alert();
							throw AlertException.INSTANCE;
						}

						//It's an unbounded subscriber or there is enough capacity to process the signal
						RingBufferProcessor.route(event, sub);
						processedSequence = true;

					} else {
						cachedAvailableSequence = sequenceBarrier.waitFor(nextSequence);
					}
				} catch (final AlertException ex) {
					if (!running.get()) {
						break;
					} else {
						long seq = workSequence.get();
						if (nextSequence < seq &&
								dataProvider.get(seq).type != SType.NEXT) {
							processedSequence = false;
							nextSequence = seq;
						} else {
							processedSequence = true;
						}
						sequenceBarrier.clearAlert();
					}

				} catch (final Throwable ex) {
					sub.onError(ex);
					sequence.set(nextSequence);
					processedSequence = true;
				}
			}
			dataProvider.removeGatingSequence(sequence);
			running.set(false);
		}
	}
}
