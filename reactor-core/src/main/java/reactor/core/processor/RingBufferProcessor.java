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
 * An implementation of a RingBuffer backed message-passing Processor.
 *
 * The processor respects the Reactive Streams contract and must not be signalled concurrently on any onXXXX
 * method. Each subscriber will be assigned a unique thread that will only stop on terminal event: Complete, Error or Cancel.
 * If Auto-Cancel is enabled, when all subscribers are unregistered, a cancel signal is sent to the upstream Publisher if any.
 * Executor can be customized and will define how many concurrent subscribers are allowed (fixed thread).
 * When a Subscriber requests Long.MAX, there won't be any backpressure applied and the producer will run at risk of being throttled
 * if the subscribers don't catch up. With any other strictly positive demand, a subscriber will stop reading new Next signals
 * (Complete and Error will still be read) as soon as the demand has been fully consumed by the pubisher.
 *
 * When more than 1 subscriber listens to that processor, they will all receive the exact same events if their
 * respective demand is still strictly positive, very much like a Fan-Out scenario.
 *
 * When the backlog has been completely booked and no subscribers is draining the signals, the publisher will start throttling.
 * In effect the smaller the backlog size is defined, the smaller the difference in processing rate between subscribers
 * must remain. Since the sequence for each subscriber will point to various ringBuffer locations, the processor
 * nows when a backlog can't override the previously occupied slot.
 *
 * @param <E> Type of dispatched signal
 * @author Stephane Maldini
 */
public final class RingBufferProcessor<E> extends ReactorProcessor<E> {
	private final SequenceBarrier              barrier;
	private final RingBuffer<MutableSignal<E>> ringBuffer;
	private final ExecutorService              executor;


	/**
	 * Create a new RingBufferProcessor using {@link #SMALL_BUFFER_SIZE} backlog size, blockingWait Strategy
	 * and auto-cancel.
	 *
	 * A new Cached ThreadExecutorPool will be implicitely created.
	 *
	 * @param <E> Type of processed signals
	 *
	 * @return a fresh processor
	 */
	public static <E> RingBufferProcessor<E> create() {
		return create(RingBufferProcessor.class.getSimpleName(), SMALL_BUFFER_SIZE, new BlockingWaitStrategy(), true);
	}

	/**
	 * Create a new RingBufferProcessor using {@link #SMALL_BUFFER_SIZE} backlog size, blockingWait Strategy
	 * and the passed auto-cancel setting.
	 *
	 * A new Cached ThreadExecutorPool will be implicitely created.
	 *
	 * @param autoCancel Should this propagate cancellation when unregistered by all subscribers ?
	 * @param <E> Type of processed signals
	 *
	 * @return a fresh processor
	 */
	public static <E> RingBufferProcessor<E> create(boolean autoCancel) {
		return create(RingBufferProcessor.class.getSimpleName(), SMALL_BUFFER_SIZE, new BlockingWaitStrategy(), autoCancel);
	}

	/**
	 * Create a new RingBufferProcessor using {@link #SMALL_BUFFER_SIZE} backlog size, blockingWait Strategy
	 * and auto-cancel.
	 *
	 * The passed {@link java.util.concurrent.ExecutorService} will execute as many event-loop
	 * consuming the ringbuffer as subscribers.
	 *
	 * @param service A provided ExecutorService to manage threading infrastructure
	 * @param <E> Type of processed signals
	 *
	 * @return a fresh processor
	 */
	public static <E> RingBufferProcessor<E> create(ExecutorService service) {
		return create(service, SMALL_BUFFER_SIZE, new BlockingWaitStrategy(), true);
	}

	/**
	 * Create a new RingBufferProcessor using {@link #SMALL_BUFFER_SIZE} backlog size, blockingWait Strategy
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
	public static <E> RingBufferProcessor<E> create(ExecutorService service, boolean autoCancel) {
		return create(service, SMALL_BUFFER_SIZE, new BlockingWaitStrategy(), autoCancel);
	}

	/**
	 * Create a new RingBufferProcessor using {@link #SMALL_BUFFER_SIZE} backlog size, blockingWait Strategy
	 * and the passed auto-cancel setting.
	 *
	 * A new Cached ThreadExecutorPool will be implicitely created and will use the passed name to qualify
	 * the created threads.
	 *
	 * @param name Use a new Cached ExecutorService and assign this name to the created threads
	 * @param bufferSize A Backlog Size to mitigate slow subscribers
	 * @param <E> Type of processed signals
	 *
	 * @return a fresh processor* @param <E>
	 * @return
	 */
	public static <E> RingBufferProcessor<E> create(String name, int bufferSize) {
		return create(name, bufferSize, new BlockingWaitStrategy(), true);
	}

	/**
	 * Create a new RingBufferProcessor using the blockingWait Strategy, passed backlog size,
	 * and auto-cancel settings.
	 *
	 * The passed {@link java.util.concurrent.ExecutorService} will execute as many event-loop
	 * consuming the ringbuffer as subscribers.
	 *
	 * @param name Use a new Cached ExecutorService and assign this name to the created threads
	 * @param bufferSize A Backlog Size to mitigate slow subscribers
	 * @param autoCancel Should this propagate cancellation when unregistered by all subscribers ?
	 * @param <E> Type of processed signals
	 *
	 * @return a fresh processor
	 */
	public static <E> RingBufferProcessor<E> create(String name, int bufferSize, boolean autoCancel) {
		return create(name, bufferSize, new BlockingWaitStrategy(), autoCancel);
	}

	/**
	 * Create a new RingBufferProcessor using passed backlog size, blockingWait Strategy
	 * and will auto-cancel.
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
	public static <E> RingBufferProcessor<E> create(ExecutorService service, int bufferSize) {
		return create(service, bufferSize, new BlockingWaitStrategy(), true);
	}

	/**
	 * Create a new RingBufferProcessor using passed backlog size, blockingWait Strategy
	 * and the auto-cancel argument.
	 *
	 * The passed {@link java.util.concurrent.ExecutorService} will execute as many event-loop
	 * consuming the ringbuffer as subscribers.
	 *
	 * @param service A provided ExecutorService to manage threading infrastructure
	 * @param bufferSize A Backlog Size to mitigate slow subscribers
	 * @param autoCancel Should this propagate cancellation when unregistered by all subscribers ?
	 * @param <E> Type of processed signals
	 *
	 * @return a fresh processor
	 */
	public static <E> RingBufferProcessor<E> create(ExecutorService service, int bufferSize, boolean autoCancel) {
		return create(service, bufferSize, new BlockingWaitStrategy(), autoCancel);
	}


	/**
	 * Create a new RingBufferProcessor using passed backlog size, wait strategy
	 * and will auto-cancel.
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
	public static <E> RingBufferProcessor<E> create(String name, int bufferSize, WaitStrategy strategy) {
		return new RingBufferProcessor<E>(name, null, bufferSize, strategy, true);
	}

	/**
	 * Create a new RingBufferProcessor using passed backlog size, wait strategy
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
	public static <E> RingBufferProcessor<E> create(String name, int bufferSize, WaitStrategy strategy, boolean autoCancel) {
		return new RingBufferProcessor<E>(name, null, bufferSize, strategy, autoCancel);
	}

	/**
	 * Create a new RingBufferProcessor using passed backlog size, wait strategy
	 * and will auto-cancel.
	 *
	 * The passed {@link java.util.concurrent.ExecutorService} will execute as many event-loop
	 * consuming the ringbuffer as subscribers.
	 *
	 * @param service A provided ExecutorService to manage threading infrastructure
	 * @param bufferSize A Backlog Size to mitigate slow subscribers
	 * @param strategy A RingBuffer WaitStrategy to use instead of the default BlockingWaitStrategy.
	 * @param <E> Type of processed signals
	 *
	 * @return a fresh processor
	 */
	public static <E> RingBufferProcessor<E> create(ExecutorService service, int bufferSize, WaitStrategy strategy) {
		return create(service, bufferSize, strategy, true);
	}

	/**
	 * Create a new RingBufferProcessor using passed backlog size, wait strategy
	 * and auto-cancel settings.
	 *
	 * The passed {@link java.util.concurrent.ExecutorService} will execute as many event-loop
	 * consuming the ringbuffer as subscribers.
	 *
	 * @param service A provided ExecutorService to manage threading infrastructure
	 * @param bufferSize A Backlog Size to mitigate slow subscribers
	 * @param strategy A RingBuffer WaitStrategy to use instead of the default BlockingWaitStrategy.
	 * @param autoCancel Should this propagate cancellation when unregistered by all subscribers ?
	 * @param <E> Type of processed signals
	 *
	 * @return a fresh processor
	 */
	public static <E> RingBufferProcessor<E> create(ExecutorService service, int bufferSize, WaitStrategy strategy, boolean autoCancel) {
		return new RingBufferProcessor<E>(null, service, bufferSize, strategy, autoCancel);
	}


	static <E> void route(MutableSignal<E> task, Subscriber<? super E> sub) {
		try {
			if (task.type == SType.COMPLETE) {
				sub.onComplete();
			} else if (task.type == SType.ERROR) {
				sub.onError(task.throwable);
			} else if (task.value != null) {
				sub.onNext(task.value);
			}
		} catch (Throwable t) {
			sub.onError(t);
		}
	}

	private RingBufferProcessor(String name,
	                            ExecutorService executor,
	                            int bufferSize,
	                            WaitStrategy waitStrategy,
	                            boolean autoCancel) {
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

		this.barrier = ringBuffer.newBarrier();
	}

	@Override
	public void subscribe(final Subscriber<? super E> sub) {
		if (null == sub) {
			throw new NullPointerException("Cannot subscribe NULL subscriber");
		}
		try {
			//create a unique eventProcessor for this subscriber
			final Sequence pendingRequest = new Sequence(0);
			final InnerEventProcessor<E> p = new InnerEventProcessor<E>(
					ringBuffer,
					barrier,
					pendingRequest,
					sub
			);

			//set eventProcessor sequence to ringbuffer index
			p.getSequence().set(ringBuffer.getCursor());

			//bind eventProcessor sequence to observe the ringBuffer
			ringBuffer.addGatingSequences(p.getSequence());

			//prepare the subscriber subscription to this processor
			p.s = new RingBufferSubscription(pendingRequest, sub, p);

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

		final         Sequence              pendingRequest;
		private final Subscriber<? super E> sub;
		private final EventProcessor        p;

		public RingBufferSubscription(Sequence pendingRequest, Subscriber<? super E> sub, EventProcessor p) {
			this.sub = sub;
			this.p = p;
			this.pendingRequest = pendingRequest;
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
			final long toRequest;

			//buffered data in producer unpublished
			long currentSequence = p.getSequence().get();
			long cursor = ringBuffer.getCursor();

			//if the current subscriber sequence behind ringBuffer cursor, count the distance from the next slot to the end
			long buffered = currentSequence <  cursor - 1l ?
					cursor - (currentSequence == Sequencer.INITIAL_CURSOR_VALUE ? currentSequence + 1l : currentSequence) :
					0l;

			//System.out.println(Thread.currentThread()+": +"+n+" [before] pending: "+pendingRequest+" - "+currentSequence+"/"+cursor+" - buffered:"+buffered+" @"+this.hashCode());
			if (pendingRequest.addAndGet(n) < 0) pendingRequest.set(Long.MAX_VALUE);

			if (buffered > 0l && n != Long.MAX_VALUE) {
				toRequest = (n - buffered) <= 0l ? n : n - buffered;
				//if (pendingRequest.addAndGet(buffered + n) < 0) pendingRequest.set(Long.MAX_VALUE);
			} else {
				toRequest = n;
			}


			//System.out.println(Thread.currentThread()+": +"+n+" [after] pending: "+pendingRequest+" - "+toRequest+"/"+cursor+" - buffered:"+buffered+" @"+this.hashCode());
			if (toRequest > 0l) {
				if (parent != null) {
					parent.request(toRequest);
				}
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
	 * Disruptor BatchEventProcessor port that deals with pending demand.
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
	private final static class InnerEventProcessor<T> implements EventProcessor {

		private final AtomicBoolean running = new AtomicBoolean(false);
		private final RingBuffer<MutableSignal<T>> dataProvider;
		private final SequenceBarrier                sequenceBarrier;
		private final Sequence sequence = new Sequence(Sequencer.INITIAL_CURSOR_VALUE);
		private final Subscriber<? super T> sub;
		private final Sequence              pendingRequest;
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
		public InnerEventProcessor(final RingBuffer<MutableSignal<T>> dataProvider,
		                           SequenceBarrier sequenceBarrier,
		                           Sequence pendingRequest,
		                           Subscriber<? super T> sub) {
			this.sub = sub;
			this.pendingRequest = pendingRequest;
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

			MutableSignal<T> event = null;
			long nextSequence = sequence.get() + 1L;
			try {
				while (true) {
					try {
						final long availableSequence = sequenceBarrier.waitFor(nextSequence);
						while (nextSequence <= availableSequence) {
							event = dataProvider.get(nextSequence);

							//if event is Next Signal we need to handle backpressure (pendingRequests)
							if (event.type == SType.NEXT) {
								//if bounded and out of capacity
								if (pendingRequest.get() != Long.MAX_VALUE && pendingRequest.addAndGet(-1l) < 0l) {
									//re-add the retained capacity
									pendingRequest.incrementAndGet();

									//if current sequence does not yet match the published one
									if (nextSequence < availableSequence) {
										//pause until request
										while (pendingRequest.addAndGet(-1l) < 0l) {
											pendingRequest.incrementAndGet();
											//Todo Use WaitStrategy?
											sequenceBarrier.checkAlert();
											LockSupport.parkNanos(1l);
										}
									} else {
										//end-of-loop without processing and incrementing the nextSequence
										break;
									}
								}

								//It's an unbounded subscriber or there is enough capacity to process the signal
								route(event, sub);
								nextSequence++;
							} else {
								//Complete or Error are terminal events, we shutdown the processor and process the signal
								running.set(false);
								route(event, sub);
								//only alert on error (immediate), complete will be drained as usual with waitFor
								if(event.type == SType.ERROR){
									sequenceBarrier.alert();
								}
								throw AlertException.INSTANCE;
							}
						}
						sequence.set(availableSequence);
					} catch (final TimeoutException e) {
						//IGNORE
					} catch (final AlertException ex) {
						if (!running.get()) {
							break;
						} else {
							long cursor = sequenceBarrier.getCursor();
							if (dataProvider.get(cursor).type != SType.NEXT) {
								sequence.set(cursor);
								nextSequence = cursor;
							} else {
								sequence.set(cursor - 1l);
							}
							sequenceBarrier.clearAlert();
						}
					} catch (final Throwable ex) {
						sub.onError(ex);
						sequence.set(nextSequence);
						nextSequence++;
					}
				}
			} finally {
				dataProvider.removeGatingSequence(sequence);
				running.set(false);
			}
		}
	}
}
