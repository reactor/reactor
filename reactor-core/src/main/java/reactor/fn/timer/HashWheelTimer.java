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

package reactor.fn.timer;

import org.reactivestreams.Processor;
import reactor.core.error.CancelException;
import reactor.core.processor.rb.disruptor.RingBuffer;
import reactor.core.support.Assert;
import reactor.core.support.NamedDaemonThreadFactory;
import reactor.fn.Consumer;
import reactor.fn.Pausable;
import reactor.fn.Supplier;

import java.util.Set;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;

/**
 * Hash Wheel Timer, as per the paper:
 * <p>
 * Hashed and hierarchical timing wheels:
 * http://www.cs.columbia.edu/~nahum/w6998/papers/ton97-timing-wheels.pdf
 * <p>
 * More comprehensive slides, explaining the paper can be found here:
 * http://www.cse.wustl.edu/~cdgill/courses/cs6874/TimingWheels.ppt
 * <p>
 * Hash Wheel timer is an approximated timer that allows performant execution of
 * larger amount of tasks with better performance compared to traditional scheduling.
 *
 * @author Oleksandr Petrov
 * @author Jon Brisbin
 * @author Stephane Maldini
 */
public class HashWheelTimer implements Timer {

	public static final  int    DEFAULT_WHEEL_SIZE = 512;
	private static final String DEFAULT_TIMER_NAME = "hash-wheel-timer";

	private final RingBuffer<Set<TimerPausable>> wheel;
	private final int                            resolution;
	private final Thread                         loop;
	private final Executor                       executor;
	private final WaitStrategy                   waitStrategy;

	/**
	 * Create a new {@code HashWheelTimer} using the given with default resolution of 100 milliseconds and
	 * default wheel size.
	 */
	public HashWheelTimer() {
		this(50, DEFAULT_WHEEL_SIZE, new SleepWait());
	}

	/**
	 * Create a new {@code HashWheelTimer} using the given timer resolution. All times will rounded up to the closest
	 * multiple of this resolution.
	 *
	 * @param resolution the resolution of this timer, in milliseconds
	 */
	public HashWheelTimer(int resolution) {
		this(resolution, DEFAULT_WHEEL_SIZE, new SleepWait());
	}

	/**
	 * Create a new {@code HashWheelTimer} using the given timer {@param res} and {@param wheelSize}. All times will
	 * rounded up to the closest multiple of this resolution.
	 *
	 * @param res          resolution of this timer in milliseconds
	 * @param wheelSize    size of the Ring Buffer supporting the Timer, the larger the wheel, the less the lookup time
	 *                     is
	 *                     for sparse timeouts. Sane default is 512.
	 * @param waitStrategy strategy for waiting for the next tick
	 */
	public HashWheelTimer(int res, int wheelSize, WaitStrategy waitStrategy) {
		this(DEFAULT_TIMER_NAME, res, wheelSize, waitStrategy, null);
	}

	/**
	 * Create a new {@code HashWheelTimer} using the given timer {@param resolution} and {@param wheelSize}. All times
	 * will
	 * rounded up to the closest multiple of this resolution.
	 *
	 * @param name      name for daemon thread factory to be displayed
	 * @param res       resolution of this timer in milliseconds
	 * @param wheelSize size of the Ring Buffer supporting the Timer, the larger the wheel, the less the lookup time is
	 *                  for sparse timeouts. Sane default is 512.
	 * @param strategy  strategy for waiting for the next tick
	 * @param exec      Executor instance to submit tasks to
	 */
	public HashWheelTimer(String name, int res, int wheelSize, WaitStrategy strategy, Executor exec) {
		this.waitStrategy = strategy;

		this.wheel = RingBuffer.createSingleProducer(new Supplier<Set<TimerPausable>>() {
			@Override
			public Set<TimerPausable> get() {
				return new ConcurrentSkipListSet<TimerPausable>();
			}
		}, wheelSize);

		if(exec == null){
			this.executor = Executors.newFixedThreadPool(1, new NamedDaemonThreadFactory(name + "-run",
			  new ClassLoader(Thread.currentThread().getContextClassLoader()) {}));
		} else {
			this.executor = exec;
		}

		this.resolution = res;
		this.loop = new NamedDaemonThreadFactory(name).newThread(new Runnable() {
			@Override
			public void run() {
				long deadline = System.currentTimeMillis();

				while (true) {
					Set<TimerPausable> registrations = wheel.get(wheel.getCursor());

					for (TimerPausable r : registrations) {
						if (r.isCancelled()) {
							registrations.remove(r);
						} else if (r.ready()) {
							executor.execute(r);
							registrations.remove(r);

							if (!r.isCancelAfterUse()) {
								reschedule(r);
							}
						} else if (r.isPaused()) {
							reschedule(r);
						} else {
							r.decrement();
						}
					}

					deadline += resolution;

					try {
						waitStrategy.waitUntil(deadline);
					} catch (InterruptedException e) {
						return;
					}

					wheel.publish(wheel.next());
				}
			}
		});


		this.start();
	}

	@Override
	public long getResolution() {
		return resolution;
	}

	@SuppressWarnings("unchecked")
	@Override
	public Pausable schedule(Consumer<Long> consumer,
	                         long period,
	                         TimeUnit timeUnit,
	                         long delayInMilliseconds) {
		Assert.isTrue(!loop.isInterrupted(), "Cannot submit tasks to this timer as it has been cancelled.");
		return schedule(TimeUnit.MILLISECONDS.convert(period, timeUnit), delayInMilliseconds, consumer);
	}

	@Override
	@SuppressWarnings("unchecked")
	public Pausable submit(Consumer<Long> consumer,
	                       long period,
	                       TimeUnit timeUnit) {
		Assert.isTrue(!loop.isInterrupted(), "Cannot submit tasks to this timer as it has been cancelled.");
		long ms = TimeUnit.MILLISECONDS.convert(period, timeUnit);
		return schedule(0, ms, consumer).cancelAfterUse();
	}

	@Override
	@SuppressWarnings("unchecked")
	public Pausable submit(Consumer<Long> consumer) {
		return submit(consumer, resolution, TimeUnit.MILLISECONDS);
	}

	@Override
	@SuppressWarnings("unchecked")
	public Pausable schedule(Consumer<Long> consumer,
	                         long period,
	                         TimeUnit timeUnit) {
		return schedule(TimeUnit.MILLISECONDS.convert(period, timeUnit),
		  0,
		  consumer);
	}

	@SuppressWarnings("unchecked")
	private TimerPausable schedule(long recurringTimeout,
	                               long firstDelay,
	                               Consumer<Long> consumer) {
		if (recurringTimeout != 0) {
			TimeUtils.checkResolution(recurringTimeout, resolution);
		}

		long offset = recurringTimeout / resolution;
		long rounds = offset / wheel.getBufferSize();

		long firstFireOffset = firstDelay / resolution;
		long firstFireRounds = firstFireOffset / wheel.getBufferSize();

		TimerPausable r = new TimerPausable(firstFireRounds, offset, consumer, rounds);
		wheel.get(wheel.getCursor() + firstFireOffset + (recurringTimeout != 0 ? 1 : 0)).add(r);
		return r;
	}

	/**
	 * Reschedule a {@link TimerPausable}  for the next fire
	 *
	 * @param registration
	 */
	private void reschedule(TimerPausable registration) {
		registration.rounds.set(registration.rescheduleRounds);
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
		if(executor instanceof Processor){
			((Processor)executor).onComplete();
		}else if(executor instanceof ExecutorService){
			((ExecutorService)executor).shutdown();
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
	public interface WaitStrategy {

		/**
		 * Wait until the given deadline, {@param deadlineMilliseconds}
		 *
		 * @param deadlineMilliseconds deadline to wait for, in milliseconds
		 */
		void waitUntil(long deadlineMilliseconds) throws InterruptedException;
	}

	/**
	 * Timer Registration
	 *
	 * @param <T> type of the Timer Registration Consumer
	 */
	public static class TimerPausable<T extends Consumer<Long>> implements Runnable,
	  Comparable,
	  Pausable {

		public static int STATUS_PAUSED    = 1;
		public static int STATUS_CANCELLED = -1;
		public static int STATUS_READY     = 0;

		private final T             delegate;
		private final long          rescheduleRounds;
		private final long          scheduleOffset;
		private final AtomicLong    rounds;
		private final AtomicInteger status;
		private final AtomicBoolean cancelAfterUse;
		private final boolean       lifecycle;

		/**
		 * Creates a new Timer Registration with given {@param rounds}, {@param offset} and {@param delegate}.
		 *
		 * @param rounds   amount of rounds the Registration should go through until it's elapsed
		 * @param offset   offset of in the Ring Buffer for rescheduling
		 * @param delegate delegate that will be ran whenever the timer is elapsed
		 */
		public TimerPausable(long rounds, long offset, T delegate, long rescheduleRounds) {
			Assert.notNull(delegate, "Delegate cannot be null");
			this.rescheduleRounds = rescheduleRounds;
			this.scheduleOffset = offset;
			this.delegate = delegate;
			this.rounds = new AtomicLong(rounds);
			this.status = new AtomicInteger(STATUS_READY);
			this.cancelAfterUse = new AtomicBoolean(false);
			this.lifecycle = Pausable.class.isAssignableFrom(delegate.getClass());
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
			try {
				delegate.accept(TimeUtils.approxCurrentTimeMillis());
			} catch(CancelException e){
				cancel();
			}
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
		@Override
		public TimerPausable cancel() {
			if (!isCancelled()) {
				if (lifecycle) {
					((Pausable) delegate).cancel();
				}
				this.status.set(STATUS_CANCELLED);
			}
			return this;
		}

		/**
		 * Check whether the current Registration is cancelled
		 *
		 * @return whether or not the current Registration is cancelled
		 */
		public boolean isCancelled() {
			return status.get() == STATUS_CANCELLED;
		}

		/**
		 * Pause the current Regisration
		 *
		 * @return current Registration
		 */
		@Override
		public TimerPausable pause() {
			if (!isPaused()) {
				if (lifecycle) {
					((Pausable) delegate).pause();
				}
				this.status.set(STATUS_PAUSED);
			}
			return this;
		}

		/**
		 * Check whether the current Registration is paused
		 *
		 * @return whether or not the current Registration is paused
		 */
		public boolean isPaused() {
			return this.status.get() == STATUS_PAUSED;
		}

		/**
		 * Resume current Registration
		 *
		 * @return current Registration
		 */
		@Override
		public TimerPausable resume() {
			if (isPaused()) {
				if (lifecycle) {
					((Pausable) delegate).resume();
				}
				reset();
			}
			return this;
		}

		/**
		 * Cancel this {@link HashWheelTimer.TimerPausable} after it has been selected and used.
		 * Implementations should respect this value and perform
		 * the cancellation.
		 *
		 * @return {@literal this}
		 */
		public TimerPausable<T> cancelAfterUse() {
			cancelAfterUse.set(true);
			return this;
		}

		public boolean isCancelAfterUse() {
			return this.cancelAfterUse.get();
		}

		@Override
		public int compareTo(Object o) {
			TimerPausable other = (TimerPausable) o;
			if (rounds.get() == other.rounds.get()) {
				return other == this ? 0 : -1;
			} else {
				return Long.compare(rounds.get(), other.rounds.get());
			}
		}

		@Override
		public String toString() {
			return String.format("HashWheelTimer { Rounds left: %d, Status: %d }", rounds.get(), status.get());
		}

		public long getOffset() {
			return scheduleOffset;
		}
	}

	/**
	 * Yielding wait strategy.
	 * <p>
	 * Spins in the loop, until the deadline is reached. Releases the flow control
	 * by means of Thread.yield() call. This strategy is less precise than BusySpin
	 * one, but is more scheduler-friendly.
	 */
	public static class YieldingWait implements WaitStrategy {

		@Override
		public void waitUntil(long deadlineMilliseconds) throws InterruptedException {
			while (deadlineMilliseconds >= System.currentTimeMillis()) {
				Thread.yield();
				if (Thread.currentThread().isInterrupted()) {
					throw new InterruptedException();
				}
			}
		}
	}

	/**
	 * BusySpin wait strategy.
	 * <p>
	 * Spins in the loop until the deadline is reached. In a multi-core environment,
	 * will occupy an entire core. Is more precise than Sleep wait strategy, but
	 * consumes more resources.
	 */
	public static class BusySpinWait implements WaitStrategy {

		@Override
		public void waitUntil(long deadlineMilliseconds) throws InterruptedException {
			while (deadlineMilliseconds >= System.currentTimeMillis()) {
				if (Thread.currentThread().isInterrupted()) {
					throw new InterruptedException();
				}
			}
		}
	}

	/**
	 * Sleep wait strategy.
	 * <p>
	 * Will release the flow control, giving other threads a possibility of execution
	 * on the same processor. Uses less resources than BusySpin wait, but is less
	 * precise.
	 */
	public static class SleepWait implements WaitStrategy {

		@Override
		public void waitUntil(long deadlineMilliseconds) throws InterruptedException {
			long sleepTimeMs = deadlineMilliseconds - System.currentTimeMillis();
			if (sleepTimeMs > 0) {
				Thread.sleep(sleepTimeMs);
			}
		}
	}

}