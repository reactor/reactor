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
 * An implementation of a RingBuffer backed message-passing Processor
 *
 * @param <E> Type of dispatched signal
 * @author Stephane Maldini
 */
public final class RingBufferProcessor<E> extends ReactorProcessor<E> {
	private final SequenceBarrier              barrier;
	private final RingBuffer<MutableSignal<E>> ringBuffer;
	private final ExecutorService              executor;

	/**
	 * @return
	 */
	public static <E> RingBufferProcessor<E> create() {
		return create(RingBufferProcessor.class.getSimpleName(), SMALL_BUFFER_SIZE, new BlockingWaitStrategy(), true);
	}

	/**
	 *
	 * @param autoCancel
	 * @param <E>
	 * @return
	 */
	public static <E> RingBufferProcessor<E> create(boolean autoCancel) {
		return create(RingBufferProcessor.class.getSimpleName(), SMALL_BUFFER_SIZE, new BlockingWaitStrategy(), autoCancel);
	}

	/**
	 * @param service
	 * @param <E>
	 * @return
	 */
	public static <E> RingBufferProcessor<E> create(ExecutorService service) {
		return create(service, SMALL_BUFFER_SIZE, new BlockingWaitStrategy(), true);
	}

	/**
	 *
	 * @param service
	 * @param autoCancel
	 * @param <E>
	 * @return
	 */
	public static <E> RingBufferProcessor<E> create(ExecutorService service, boolean autoCancel) {
		return create(service, SMALL_BUFFER_SIZE, new BlockingWaitStrategy(), true);
	}

	/**
	 * @param name
	 * @return
	 */
	public static <E> RingBufferProcessor<E> create(String name, int bufferSize) {
		return create(name, bufferSize, new BlockingWaitStrategy(), true);
	}

	/**
	 *
	 * @param name
	 * @param bufferSize
	 * @param autoCancel
	 * @param <E>
	 * @return
	 */
	public static <E> RingBufferProcessor<E> create(String name, int bufferSize, boolean autoCancel) {
		return create(name, bufferSize, new BlockingWaitStrategy(), autoCancel);
	}

	/**
	 * @param service
	 * @param bufferSize
	 * @param <E>
	 * @return
	 */
	public static <E> RingBufferProcessor<E> create(ExecutorService service, int bufferSize) {
		return create(service, bufferSize, new BlockingWaitStrategy(), true);
	}

	/**
	 *
	 * @param service
	 * @param bufferSize
	 * @param autoCancel
	 * @param <E>
	 * @return
	 */
	public static <E> RingBufferProcessor<E> create(ExecutorService service, int bufferSize, boolean autoCancel) {
		return create(service, bufferSize, new BlockingWaitStrategy(), autoCancel);
	}

	/**
	 * @param name
	 * @param bufferSize
	 * @param strategy
	 * @return
	 */
	public static <E> RingBufferProcessor<E> create(String name, int bufferSize, WaitStrategy strategy) {
		return new RingBufferProcessor<E>(name, null, bufferSize, strategy, true);
	}

	/**
	 *
	 * @param name
	 * @param bufferSize
	 * @param strategy
	 * @param autoCancel
	 * @param <E>
	 * @return
	 */
	public static <E> RingBufferProcessor<E> create(String name, int bufferSize, WaitStrategy strategy, boolean autoCancel) {
		return new RingBufferProcessor<E>(name, null, bufferSize, strategy, autoCancel);
	}

	/**
	 * @param service
	 * @param bufferSize
	 * @param strategy
	 * @param <E>
	 * @return
	 */
	public static <E> RingBufferProcessor<E> create(ExecutorService service, int bufferSize, WaitStrategy strategy) {
		return create(service, bufferSize, strategy, true);
	}

	/**
	 *
	 * @param service
	 * @param bufferSize
	 * @param strategy
	 * @param autoCancel
	 * @param <E>
	 * @return
	 */
	public static <E> RingBufferProcessor<E> create(ExecutorService service, int bufferSize, WaitStrategy strategy, boolean autoCancel) {
		return new RingBufferProcessor<E>(null, service, bufferSize, strategy, autoCancel);
	}


	static <E> void route(MutableSignal<E> task, Subscription s, Subscriber<? super E> sub) {
		try {
			if (task.type == SType.COMPLETE) {
				if (s != null) {
					s.cancel();
				}
				sub.onComplete();
			} else if (task.type == SType.ERROR) {
				if (s != null) {
					s.cancel();
				}
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
			long toPublish = p.getSequence().get() > Sequencer.INITIAL_CURSOR_VALUE ?
					Math.abs(ringBuffer.getCursor() - (p.getSequence().get() + 1l)) :
					ringBuffer.getCursor();

			if (toPublish > 0l && n != Long.MAX_VALUE) {
				toRequest = n - Math.abs(n - toPublish);

				if (pendingRequest.addAndGet(toPublish) < 0) pendingRequest.set(Long.MAX_VALUE);
			} else {
				toRequest = n;
				if (pendingRequest.addAndGet(n) < 0) pendingRequest.set(Long.MAX_VALUE);
			}

			if (toRequest > 0l) {
				if (parent != null) {
					parent.request(toRequest);
				}
			}


		}

		@Override
		public void cancel() {
			try {
				ringBuffer.removeGatingSequence(p.getSequence());
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
		private final DataProvider<MutableSignal<T>> dataProvider;
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
		public InnerEventProcessor(final DataProvider<MutableSignal<T>> dataProvider,
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

										//look ahead if the published event was a terminal signal
										if (dataProvider.get(availableSequence).type != SType.NEXT) {
											//terminate
											running.set(false);
											//process last signal
											route(dataProvider.get(availableSequence), s, sub);
											//short-circuit
											throw AlertException.INSTANCE;
										}
										//pause until request
										while (pendingRequest.get() <= 0l) {
											//Todo Use WaitStrategy?
											LockSupport.parkNanos(1l);
										}
									} else {
										//end-of-loop without processing and incrementing the nextSequence
										break;
									}
								}

								//It's an unbounded subscriber or there is enough capacity to process the signal
								route(event, s, sub);
								nextSequence++;
							} else {
								//Complete or Error are terminal events, we shutdown the processor and process the signal
								running.set(false);
								route(event, s, sub);
								throw AlertException.INSTANCE;
							}
						}
						sequence.set(availableSequence);
					} catch (final TimeoutException e) {
						//IGNORE
					} catch (final AlertException ex) {
						if (!running.get()) {
							break;
						}
					} catch (final Throwable ex) {
						sub.onError(ex);
						sequence.set(nextSequence);
						nextSequence++;
					}
				}
			} finally {
				running.set(false);
			}
		}
	}
}
