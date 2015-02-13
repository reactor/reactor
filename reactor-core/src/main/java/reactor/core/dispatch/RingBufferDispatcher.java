/*
 * Copyright (c) 2011-2014 Pivotal Software, Inc.
 *
 *  Licensed under the Apache License, Version 2.0 (the "License");
 *  you may not use this file except in compliance with the License.
 *  You may obtain a copy of the License at
 *
 *         http://www.apache.org/licenses/LICENSE-2.0
 *
 *  Unless required by applicable law or agreed to in writing, software
 *  distributed under the License is distributed on an "AS IS" BASIS,
 *  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  See the License for the specific language governing permissions and
 *  limitations under the License.
 */

package reactor.core.dispatch;

import org.reactivestreams.Processor;
import org.reactivestreams.Subscriber;
import org.reactivestreams.Subscription;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import reactor.core.dispatch.wait.WaitingMood;
import reactor.core.support.NamedDaemonThreadFactory;
import reactor.core.support.SpecificationExceptions;
import reactor.fn.Consumer;
import reactor.jarjar.com.lmax.disruptor.*;
import reactor.jarjar.com.lmax.disruptor.dsl.Disruptor;
import reactor.jarjar.com.lmax.disruptor.dsl.ProducerType;

import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

/**
 * Implementation of a {@link reactor.core.Dispatcher} that uses a {@link RingBuffer} to queue tasks to execute.
 *
 * @author Jon Brisbin
 * @author Stephane Maldini
 */
public final class RingBufferDispatcher extends SingleThreadDispatcher implements WaitingMood {

	private static final int DEFAULT_BUFFER_SIZE = 1024;

	private final Logger log = LoggerFactory.getLogger(getClass());
	private final ExecutorService            executor;
	private final Disruptor<RingBufferTask>  disruptor;
	private final RingBuffer<RingBufferTask> ringBuffer;
	private final WaitingMood                waitingMood;

	/**
	 * Creates a new {@code RingBufferDispatcher} with the given {@code name}. It will use a RingBuffer with 1024 slots,
	 * configured with a producer type of {@link ProducerType#MULTI MULTI} and a {@link BlockingWaitStrategy blocking
	 * wait
	 * strategy}.
	 *
	 * @param name The name of the dispatcher.
	 */
	public RingBufferDispatcher(String name) {
		this(name, DEFAULT_BUFFER_SIZE);
	}

	/**
	 * Creates a new {@code RingBufferDispatcher} with the given {@code name} and {@param bufferSize},
	 * configured with a producer type of {@link ProducerType#MULTI MULTI} and a {@link BlockingWaitStrategy blocking
	 * wait
	 * strategy}.
	 *
	 * @param name       The name of the dispatcher
	 * @param bufferSize The size to configure the ring buffer with
	 */
	@SuppressWarnings({"unchecked"})
	public RingBufferDispatcher(String name,
	                            int bufferSize
	) {
		this(name, bufferSize, null);
	}

	/**
	 * Creates a new {@literal RingBufferDispatcher} with the given {@code name}. It will use a {@link RingBuffer} with
	 * {@code bufferSize} slots, configured with a producer type of {@link ProducerType#MULTI MULTI}
	 * and a {@link BlockingWaitStrategy blocking wait. A given @param uncaughtExceptionHandler} will catch anything not
	 * handled e.g. by the owning {@link reactor.bus.EventBus} or {@link reactor.rx.Stream}.
	 *
	 * @param name                     The name of the dispatcher
	 * @param bufferSize               The size to configure the ring buffer with
	 * @param uncaughtExceptionHandler The last resort exception handler
	 */
	@SuppressWarnings({"unchecked"})
	public RingBufferDispatcher(String name,
	                            int bufferSize,
	                            final Consumer<Throwable> uncaughtExceptionHandler) {
		this(name, bufferSize, uncaughtExceptionHandler, ProducerType.MULTI, new BlockingWaitStrategy());

	}

	/**
	 * Creates a new {@literal RingBufferDispatcher} with the given {@code name}. It will use a {@link RingBuffer} with
	 * {@code bufferSize} slots, configured with the given {@code producerType}, {@param uncaughtExceptionHandler}
	 * and {@code waitStrategy}. A null {@param uncaughtExceptionHandler} will make this dispatcher logging such
	 * exceptions.
	 *
	 * @param name                     The name of the dispatcher
	 * @param bufferSize               The size to configure the ring buffer with
	 * @param producerType             The producer type to configure the ring buffer with
	 * @param waitStrategy             The wait strategy to configure the ring buffer with
	 * @param uncaughtExceptionHandler The last resort exception handler
	 */
	@SuppressWarnings({"unchecked"})
	public RingBufferDispatcher(String name,
	                            int bufferSize,
	                            final Consumer<Throwable> uncaughtExceptionHandler,
	                            ProducerType producerType,
	                            WaitStrategy waitStrategy) {
		super(bufferSize);

		if (WaitingMood.class.isAssignableFrom(waitStrategy.getClass())) {
			this.waitingMood = (WaitingMood) waitStrategy;
		} else {
			this.waitingMood = null;
		}

		this.executor = Executors.newCachedThreadPool(new NamedDaemonThreadFactory(name, getContext()));
		this.disruptor = new Disruptor<RingBufferTask>(
				new EventFactory<RingBufferTask>() {
					@Override
					public RingBufferTask newInstance() {
						return new RingBufferTask();
					}
				},
				bufferSize,
				executor,
				producerType,
				waitStrategy
		);

		this.disruptor.handleExceptionsWith(new ExceptionHandler() {
			@Override
			public void handleEventException(Throwable ex, long sequence, Object event) {
				handleOnStartException(ex);
			}

			@Override
			public void handleOnStartException(Throwable ex) {
				if (null != uncaughtExceptionHandler) {
					uncaughtExceptionHandler.accept(ex);
				} else {
					log.error(ex.getMessage(), ex);
				}
			}

			@Override
			public void handleOnShutdownException(Throwable ex) {
				handleOnStartException(ex);
			}
		});
		this.disruptor.handleEventsWith(new EventHandler<RingBufferTask>() {
			@Override
			public void onEvent(RingBufferTask task, long sequence, boolean endOfBatch) throws Exception {
				task.run();
			}
		});

		this.ringBuffer = disruptor.start();
	}

	//@Override
	public <E> Processor<E, E> dispatch() {
		if (alive()) {
			disruptor.shutdown();
		}

		final SequenceBarrier barrier = ringBuffer.newBarrier();
		return new RingBufferProcessor<>(barrier);
	}

	@Override
	public boolean awaitAndShutdown(long timeout, TimeUnit timeUnit) {
		boolean alive = alive();
		shutdown();
		try {
			executor.awaitTermination(timeout, timeUnit);
			if (alive) {
				disruptor.shutdown();
			}
		} catch (InterruptedException e) {
			return false;
		}
		return true;
	}

	@Override
	public void shutdown() {
		executor.shutdown();
		disruptor.shutdown();
		super.shutdown();
	}

	@Override
	public void forceShutdown() {
		executor.shutdownNow();
		disruptor.halt();
		super.forceShutdown();
	}

	@Override
	public long remainingSlots() {
		return ringBuffer.remainingCapacity();
	}

	@Override
	public void nervous() {
		if (waitingMood != null) {
			execute(new Runnable() {
				@Override
				public void run() {
					waitingMood.nervous();
				}
			});
		}
	}

	@Override
	public void calm() {
		if (waitingMood != null) {
			execute(new Runnable() {
				@Override
				public void run() {
					waitingMood.calm();
				}
			});

		}
	}

	@Override
	protected Task tryAllocateTask() throws InsufficientCapacityException {
		try {
			long seqId = ringBuffer.tryNext();
			return ringBuffer.get(seqId).setSequenceId(seqId);
		} catch (reactor.jarjar.com.lmax.disruptor.InsufficientCapacityException e) {
			throw InsufficientCapacityException.INSTANCE;
		}
	}

	@Override
	protected Task allocateTask() {
		long seqId = ringBuffer.next();
		return ringBuffer.get(seqId).setSequenceId(seqId);
	}

	protected void execute(Task task) {
		ringBuffer.publish(((RingBufferTask) task).getSequenceId());
	}

	private class RingBufferTask extends SingleThreadTask {
		private long sequenceId;

		public long getSequenceId() {
			return sequenceId;
		}

		public RingBufferTask setSequenceId(long sequenceId) {
			this.sequenceId = sequenceId;
			return this;
		}
	}

	private class RingBufferTaskEventHandler<E> implements EventHandler<RingBufferTask>, Consumer<E> {
		private final Subscriber<? super E>  sub;
		private final RingBufferProcessor<E> owner;

		Subscription s;

		public RingBufferTaskEventHandler(RingBufferProcessor<E> owner, Subscriber<? super E> sub) {
			this.sub = sub;
			this.owner = owner;
		}

		@Override
		public void accept(E e) {
			try {
				sub.onNext(e);
			} catch (Throwable t) {
				sub.onError(t);
			}
		}

		@Override
		public void onEvent(RingBufferTask ringBufferTask, long seq, boolean end) throws Exception {
			try {
				handleSubscriber(ringBufferTask, seq, end);
				recurse();
			} catch (Throwable e) {
				sub.onError(e);
			}
		}

		@SuppressWarnings("unchecked")
		private void handleSubscriber(Task task, long seq, boolean end) {
			if (task.eventConsumer == owner.COMPLETE_SENTINEL) {

				if (s != null) {
					s.cancel();
					s = null;
				}
				sub.onComplete();

			} else if (task.eventConsumer == owner.ERROR_SENTINEL) {
				if (s != null) {
					s.cancel();
					s = null;
				}
				sub.onError((Throwable) task.data);
			} else if (task.eventConsumer == owner) {
				System.out.println(Thread.currentThread() + "-" + inContext() + " ] Task:" + task.data + " seq:" + seq + " " +
						end);
				sub.onNext((E) task.data);
			}
		}

		void recurse() {
			//Process any recursive tasks
			if (tailRecurseSeq < 0) {
				return;
			}
			int next = -1;
			while (next < tailRecurseSeq) {
				handleSubscriber(tailRecursionPile.get(++next), -1, false);
			}

			// clean up extra tasks
			next = tailRecurseSeq;
			int max = backlog * 2;
			while (next >= max) {
				tailRecursionPile.remove(next--);
			}
			tailRecursionPileSize = max;
			tailRecurseSeq = -1;
		}
	}

	private class RingBufferProcessor<E> implements Processor<E, E>, Consumer<E> {
		private final SequenceBarrier barrier;
		private final Sequence pending           = new Sequence(0l);
		private final Sequence batch             = new Sequence(0l);
		final         Consumer COMPLETE_SENTINEL = new Consumer() {
			@Override
			public void accept(Object o) {
			}
		};

		final Consumer<Throwable> ERROR_SENTINEL = new Consumer<Throwable>() {
			@Override
			public void accept(Throwable throwable) {
			}
		};

		Subscription s;

		public RingBufferProcessor(SequenceBarrier barrier) {
			this.barrier = barrier;
		}

		@Override
		public void subscribe(Subscriber<? super E> sub) {
			final RingBufferTaskEventHandler<E> eventHandler = new RingBufferTaskEventHandler<>(this, sub);
			final EventProcessor p = new BatchEventProcessor<RingBufferTask>(
					ringBuffer,
					barrier,
					eventHandler
			);


			p.getSequence().set(barrier.getCursor());
			ringBuffer.addGatingSequences(p.getSequence());
			executor.execute(p);

			Subscription subscription = new Subscription() {

				@Override
				@SuppressWarnings("unchecked")
				public void request(long n) {
					if (n <= 0l) {
						sub.onError(SpecificationExceptions.spec_3_09_exception(n));
						return;
					}

					if (pending.get() == Long.MAX_VALUE) {
						return;
					}

					synchronized (this) {
						if (pending.addAndGet(n) < 0l) pending.set(Long.MAX_VALUE);
					}

					if (s != null) {
						if (n == Long.MAX_VALUE) {
							s.request(n);
						} else {
							int toRequest = (int)Math.min(Integer.MAX_VALUE, Math.min(n, remainingSlots()));
							if (toRequest > 0) {
								s.request(toRequest);
								if(batch.get() > 1l){
									ringBuffer.next(toRequest);
								}
							}
						}
					}

				}

				@Override
				public void cancel() {
					try {
						ringBuffer.removeGatingSequence(p.getSequence());
						p.halt();
					} finally {
						s.cancel();
					}
				}
			};

			eventHandler.s = subscription;
			sub.onSubscribe(subscription);
		}

		@Override
		public void onSubscribe(final Subscription s) {
			if (this.s != null) {
				s.cancel();
				return;
			}
			this.s = s;
		}

		void publishSignal(Object data, Consumer consumer) {
			long batch = this.batch.incrementAndGet();
			long demand = pending.get();
			long seqId;

			if (batch > 1l) {
				seqId = (ringBuffer.getCursor() - demand) + batch;
			} else if (demand == 0l || demand == Long.MAX_VALUE) {
				seqId = ringBuffer.next();
			} else {
				int preallocate =
						(int) Math.min(Integer.MAX_VALUE,
								Math.min(
										ringBuffer.remainingCapacity(),
										Math.abs(batch - demand)
								) + 1l
						);

				if (preallocate > 0l) {
					seqId = ringBuffer.next(preallocate);
					System.out.println(Thread.currentThread() + " is nexting " + preallocate + " with " + data + " on seq " +
							seqId);
					seqId = seqId - (preallocate - 1l);
				} else {
					seqId = ringBuffer.next();
				}
			}

			System.out.println(Thread.currentThread() + " is marking " + seqId + " with " + data+"="+batch+"/"+demand+" : "+ringBuffer.getCursor())   ;
			ringBuffer.get(seqId)
					.setSequenceId(seqId)
					.setData(data)
					.setEventConsumer(consumer);

			if (demand == Long.MAX_VALUE) {
				ringBuffer.publish(seqId);
			} else if (demand == batch) {
				synchronized (this) {
					if (this.pending.addAndGet(-demand) < 0l) this.pending.set(0l);
				}
				this.batch.set(0l);
				ringBuffer.publish(seqId - (batch - 1l), seqId);
			}
		}

		@Override
		public void onNext(E o) {
			if (!inContext()) {
				publishSignal(o, this);

			} else {
				allocateRecursiveTask()
						.setData(o)
						.setEventConsumer(this);
			}
		}

		@Override
		public void accept(E e) {
			//IGNORE
		}

		@Override
		public void onError(Throwable t) {
			try {
				pending.set(0l);
				publishSignal(t, ERROR_SENTINEL);
			} finally {
				if (s != null) {
					s.cancel();
				}
			}
		}

		@Override
		public void onComplete() {
			try {
				pending.set(0l);
				publishSignal(null, COMPLETE_SENTINEL);
			} finally {
				if (s != null) {
					s.cancel();
				}
			}
		}

		@Override
		public String toString() {
			return "RingBufferSubscriber{" +
					", barrier=" + barrier.getCursor() +
					", pending=" + pending +
					'}';
		}
	}
}
