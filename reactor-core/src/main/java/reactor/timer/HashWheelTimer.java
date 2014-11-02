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

package reactor.timer;

import reactor.event.registry.Registration;
import reactor.event.selector.Selector;
import reactor.function.Consumer;
import reactor.function.Pausable;
import reactor.jarjar.com.lmax.disruptor.EventFactory;
import reactor.jarjar.com.lmax.disruptor.RingBuffer;
import reactor.support.NamedDaemonThreadFactory;
import reactor.util.Assert;

import java.util.Set;
import java.util.concurrent.ConcurrentSkipListSet;
import java.util.concurrent.Executor;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;

/**
 * Hash Wheel Timer, as per the paper:
 *
 * Hashed and hierarchical timing wheels:
 * http://www.cs.columbia.edu/~nahum/w6998/papers/ton97-timing-wheels.pdf
 *
 * More comprehensive slides, explaining the paper can be found here:
 * http://www.cse.wustl.edu/~cdgill/courses/cs6874/TimingWheels.ppt
 *
 * Hash Wheel timer is an approximated timer that allows performant execution of
 * larger amount of tasks with better performance compared to traditional scheduling.
 *
 * @author Oleksandr Petrov
 */
public class HashWheelTimer implements Timer {

	public static final  int    DEFAULT_WHEEL_SIZE = 512;
	private static final String DEFAULT_TIMER_NAME = "hash-wheel-timer";

	private final RingBuffer<Set<TimerRegistration>> wheel;
	private final int                                resolution;
	private final Thread                             loop;
	private final Executor                           executor;
	private final WaitStrategy                       waitStrategy;

	/**
	 * Create a new {@code HashWheelTimer} using the given with default resolution of 100 milliseconds and
	 * default wheel size.
	 */
	public HashWheelTimer() {
		this(100, DEFAULT_WHEEL_SIZE, new SleepWait());
	}

	/**
	 * Create a new {@code HashWheelTimer} using the given timer resolution. All times will rounded up to the closest
	 * multiple of this resolution.
	 *
	 * @param resolution
	 * 		the resolution of this timer, in milliseconds
	 */
	public HashWheelTimer(int resolution) {
		this(resolution, DEFAULT_WHEEL_SIZE, new SleepWait());
	}

	/**
	 * Create a new {@code HashWheelTimer} using the given timer {@param res} and {@param wheelSize}. All times will
	 * rounded up to the closest multiple of this resolution.
	 *
	 * @param res
	 * 		resolution of this timer in milliseconds
	 * @param wheelSize
	 * 		size of the Ring Buffer supporting the Timer, the larger the wheel, the less the lookup time is
	 * 		for sparse timeouts. Sane default is 512.
	 * @param waitStrategy
	 * 		strategy for waiting for the next tick
	 */
	public HashWheelTimer(int res, int wheelSize, WaitStrategy waitStrategy) {
		this(DEFAULT_TIMER_NAME, res, wheelSize, waitStrategy, Executors.newFixedThreadPool(1));
	}

	/**
	 * Create a new {@code HashWheelTimer} using the given timer {@param resolution} and {@param wheelSize}. All times will
	 * rounded up to the closest multiple of this resolution.
	 *
	 * @param name
	 * 		name for daemon thread factory to be displayed
	 * @param res
	 * 		resolution of this timer in milliseconds
	 * @param wheelSize
	 * 		size of the Ring Buffer supporting the Timer, the larger the wheel, the less the lookup time is
	 * 		for sparse timeouts. Sane default is 512.
	 * @param strategy
	 * 		strategy for waiting for the next tick
	 * @param exec
	 * 		Executor instance to submit tasks to
	 */
	public HashWheelTimer(String name, int res, int wheelSize, WaitStrategy strategy, Executor exec) {
		this.waitStrategy = strategy;

		this.wheel = RingBuffer.createSingleProducer(new EventFactory<Set<TimerRegistration>>() {
			@Override
			public Set<TimerRegistration> newInstance() {
				return new ConcurrentSkipListSet<TimerRegistration>();
			}
		}, wheelSize);

		this.resolution = res;
		this.loop = new NamedDaemonThreadFactory(name).newThread(new Runnable() {
			@Override
			public void run() {
				long deadline = System.currentTimeMillis();

				while(true) {
					Set<TimerRegistration> registrations = wheel.get(wheel.getCursor());

					for(TimerRegistration r : registrations) {
						if(r.isCancelled()) {
							registrations.remove(r);
						} else if(r.ready()) {
							executor.execute(r);
							registrations.remove(r);

							if(!r.isCancelAfterUse()) {
								reschedule(r);
							}
						} else if(r.isPaused()) {
							reschedule(r);
						} else {
							r.decrement();
						}
					}

					deadline += resolution;

					try {
						waitStrategy.waitUntil(deadline);
					} catch(InterruptedException e) {
						return;
					}

					wheel.publish(wheel.next());
				}
			}
		});

		this.executor = exec;
		this.start();
	}

	@Override
	public long getResolution() {
		return resolution;
	}

	@SuppressWarnings("unchecked")
	@Override
	public TimerRegistration<? extends Consumer<Long>> schedule(Consumer<Long> consumer,
	                                                            long period,
	                                                            TimeUnit timeUnit,
	                                                            long delayInMilliseconds) {
		Assert.isTrue(!loop.isInterrupted(), "Cannot submit tasks to this timer as it has been cancelled.");
		return schedule(TimeUnit.MILLISECONDS.convert(period, timeUnit), delayInMilliseconds, consumer);
	}

	@Override
	@SuppressWarnings("unchecked")
	public TimerRegistration<? extends Consumer<Long>> submit(Consumer<Long> consumer,
	                                                          long period,
	                                                          TimeUnit timeUnit) {
		Assert.isTrue(!loop.isInterrupted(), "Cannot submit tasks to this timer as it has been cancelled.");
		long ms = TimeUnit.MILLISECONDS.convert(period, timeUnit);
		return (TimerRegistration<? extends Consumer<Long>>)schedule(ms, ms, consumer).cancelAfterUse();
	}

	@Override
	@SuppressWarnings("unchecked")
	public TimerRegistration<? extends Consumer<Long>> submit(Consumer<Long> consumer) {
		return (TimerRegistration<? extends Consumer<Long>>)submit(consumer, resolution, TimeUnit.MILLISECONDS);
	}

	@Override
	@SuppressWarnings("unchecked")
	public TimerRegistration<? extends Consumer<Long>> schedule(Consumer<Long> consumer,
	                                                            long period,
	                                                            TimeUnit timeUnit) {
		return (TimerRegistration<? extends Consumer<Long>>)schedule(TimeUnit.MILLISECONDS.convert(period, timeUnit), 0, consumer);
	}

	@SuppressWarnings("unchecked")
	private TimerRegistration<? extends Consumer<Long>> schedule(long recurringTimeout,
	                                                             long firstDelay,
	                                                             Consumer<Long> consumer) {
		Assert.isTrue(recurringTimeout >= resolution,
		              "Cannot schedule tasks for amount of time less than timer precision.");

		long offset = recurringTimeout / resolution;
		long rounds = offset / wheel.getBufferSize();

		long firstFireOffset = firstDelay / resolution;
		long firstFireRounds = firstFireOffset / wheel.getBufferSize();

		TimerRegistration r = new TimerRegistration(firstFireRounds, offset, consumer, rounds);
		wheel.get(wheel.getCursor() + firstFireOffset + 1).add(r);
		return r;
	}

	/**
	 * Reschedule a {@link TimerRegistration}  for the next fire
	 *
	 * @param registration
	 */
	private void reschedule(TimerRegistration registration) {
		registration.reset();
		wheel.get(wheel.getCursor() + registration.getOffset()).add(registration);
	}

	/**
	 * Start the Timer
	 */
	public void start() {
		this.loop.start();
		wheel.publish(0);
	}

	/**
	 * Cancel current Timer
	 */
	public void cancel() {
		this.loop.interrupt();
	}


	/**
	 * Timer Registration
	 *
	 * @param <T>
	 * 		type of the Timer Registration Consumer
	 */
	public static class TimerRegistration<T extends Consumer<Long>> implements Runnable,
	                                                                           Comparable,
	                                                                           Pausable,
	                                                                           Registration {

		public static int STATUS_PAUSED    = 1;
		public static int STATUS_CANCELLED = -1;
		public static int STATUS_READY     = 0;

		private final T             delegate;
		private final long          rescheduleRounds;
		private final long          scheduleOffset;
		private final AtomicLong    rounds;
		private final AtomicInteger status;
		private final AtomicBoolean cancelAfterUse;

		/**
		 * Creates a new Timer Registration with given {@param rounds}, {@param offset} and {@param delegate}.
		 *
		 * @param rounds
		 * 		amount of rounds the Registration should go through until it's elapsed
		 * @param offset
		 * 		offset of in the Ring Buffer for rescheduling
		 * @param delegate
		 * 		delegate that will be ran whenever the timer is elapsed
		 */
		public TimerRegistration(long rounds, long offset, T delegate, long rescheduleRounds) {
			this.rescheduleRounds = rescheduleRounds;
			this.scheduleOffset = offset;
			this.delegate = delegate;
			this.rounds = new AtomicLong(rounds);
			this.status = new AtomicInteger(STATUS_READY);
			this.cancelAfterUse = new AtomicBoolean(false);
		}

		/**
		 * Decrement an amount of runs Registration has to run until it's elapsed
		 */
		public void decrement() {
			rounds.decrementAndGet();
		}

		/**
		 * Check whether the current Registration is ready for execution
		 *
		 * @return whether or not the current Registration is ready for execution
		 */
		public boolean ready() {
			return status.get() == STATUS_READY && rounds.get() == 0;
		}

		/**
		 * Run the delegate of the current Registration
		 */
		@Override
		public void run() {
			delegate.accept(TimeUtils.approxCurrentTimeMillis());
		}

		/**
		 * Reset the Registration
		 */
		public void reset() {
			this.status.set(STATUS_READY);
			this.rounds.set(rescheduleRounds);
		}

		/**
		 * Cancel the registration
		 *
		 * @return current Registration
		 */
		public Registration cancel() {
			this.status.set(STATUS_CANCELLED);
			return this;
		}

		/**
		 * Check whether the current Registration is cancelled
		 *
		 * @return whether or not the current Registration is cancelled
		 */
		@Override
		public boolean isCancelled() {
			return status.get() == STATUS_CANCELLED;
		}

		/**
		 * Pause the current Regisration
		 *
		 * @return current Registration
		 */
		@Override
		public Registration pause() {
			this.status.set(STATUS_PAUSED);
			return this;
		}

		/**
		 * Check whether the current Registration is paused
		 *
		 * @return whether or not the current Registration is paused
		 */
		@Override
		public boolean isPaused() {
			return this.status.get() == STATUS_PAUSED;
		}

		/**
		 * Resume current Registration
		 *
		 * @return current Registration
		 */
		@Override
		public Registration resume() {
			reset();
			return this;
		}

		/**
		 * Get the offset of the Registration relative to the current Ring Buffer position
		 * to make it fire timely.
		 *
		 * @return the offset of current Registration
		 */
		private long getOffset() {
			return this.scheduleOffset;
		}

		@Override
		public Selector getSelector() {
			return null;
		}

		@Override
		public Object getObject() {
			return delegate;
		}

		/**
		 * Cancel this {@link reactor.timer.HashWheelTimer.TimerRegistration} after it has been selected and used. {@link
		 * reactor.event.dispatch.Dispatcher} implementations should respect this value and perform
		 * the cancellation.
		 *
		 * @return {@literal this}
		 */
		public TimerRegistration<T> cancelAfterUse() {
			cancelAfterUse.set(false);
			return this;
		}

		@Override
		public boolean isCancelAfterUse() {
			return this.cancelAfterUse.get();
		}

		@Override
		public int compareTo(Object o) {
			TimerRegistration other = (TimerRegistration)o;
			if(rounds.get() == other.rounds.get()) {
				return other == this ? 0 : -1;
			} else {
				return Long.compare(rounds.get(), other.rounds.get());
			}
		}

		@Override
		public String toString() {
			return String.format("HashWheelTimer { Rounds left: %d, Status: %d }", rounds.get(), status.get());
		}
	}

	@Override
	public String toString() {
		return String.format("HashWheelTimer { Buffer Size: %d, Resolution: %d }",
		                     wheel.getBufferSize(),
		                     resolution);
	}


	/**
	 * Wait strategy for the timer
	 */
	public static interface WaitStrategy {

		/**
		 * Wait until the given deadline, {@param deadlineMilliseconds}
		 *
		 * @param deadlineMilliseconds
		 * 		deadline to wait for, in milliseconds
		 */
		public void waitUntil(long deadlineMilliseconds) throws InterruptedException;
	}

	/**
	 * Yielding wait strategy.
	 *
	 * Spins in the loop, until the deadline is reached. Releases the flow control
	 * by means of Thread.yield() call. This strategy is less precise than BusySpin
	 * one, but is more scheduler-friendly.
	 */
	public static class YieldingWait implements WaitStrategy {

		@Override
		public void waitUntil(long deadlineMilliseconds) throws InterruptedException {
			while(deadlineMilliseconds >= System.currentTimeMillis()) {
				Thread.yield();
				if(Thread.currentThread().isInterrupted()) {
					throw new InterruptedException();
				}
			}
		}
	}

	/**
	 * BusySpin wait strategy.
	 *
	 * Spins in the loop until the deadline is reached. In a multi-core environment,
	 * will occupy an entire core. Is more precise than Sleep wait strategy, but
	 * consumes more resources.
	 */
	public static class BusySpinWait implements WaitStrategy {

		@Override
		public void waitUntil(long deadlineMilliseconds) throws InterruptedException {
			while(deadlineMilliseconds >= System.currentTimeMillis()) {
				if(Thread.currentThread().isInterrupted()) {
					throw new InterruptedException();
				}
			}
		}
	}

	/**
	 * Sleep wait strategy.
	 *
	 * Will release the flow control, giving other threads a possibility of execution
	 * on the same processor. Uses less resources than BusySpin wait, but is less
	 * precise.
	 */
	public static class SleepWait implements WaitStrategy {

		@Override
		public void waitUntil(long deadlineMilliseconds) throws InterruptedException {
			long sleepTimeMs = deadlineMilliseconds - System.currentTimeMillis();
			if(sleepTimeMs > 0) {
				Thread.sleep(sleepTimeMs);
			}
		}
	}

}