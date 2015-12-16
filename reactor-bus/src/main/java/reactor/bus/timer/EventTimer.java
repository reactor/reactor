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

package reactor.bus.timer;

import reactor.core.support.Logger;
import reactor.bus.registry.Registration;
import reactor.bus.registry.Registries;
import reactor.bus.registry.Registry;
import reactor.bus.selector.Selector;
import reactor.core.error.CancelException;
import reactor.core.support.Assert;
import reactor.core.support.NamedDaemonThreadFactory;
import reactor.core.error.ReactorFatalException;
import reactor.fn.Consumer;
import reactor.fn.Function;
import reactor.core.timer.Timer;

import java.util.Map;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

/**
 * A hashed wheel timer implementation that uses a {@link reactor.bus.registry.Registry} and custom {@link
 * reactor.bus.selector.Selector Selectors} to determine when tasks should be executed.
 * <p>
 * This is specifically useful when RingBuffer HashWheelTimer is not supported (Android).
 * <p>
 * A {@code SimpleHashWheelTimer} has two variations for scheduling tasks: {@link #schedule(reactor.fn.Consumer,
 * long,
 * java.util.concurrent.TimeUnit)} and {@link #schedule(reactor.fn.Consumer, long, java.util.concurrent.TimeUnit,
 * long)} which are for scheduling repeating tasks, and {@link #submit(reactor.fn.Consumer, long,
 * java.util.concurrent.TimeUnit)} which is for scheduling single-run delayed tasks.
 * </p>
 * <p>
 * To schedule a repeating task, specify the period of time which should elapse before invoking the given {@link
 * reactor.fn.Consumer}. To schedule a task that repeats every 5 seconds, for example, one would do something
 * like:
 * </p>
 * <p>
 * <code><pre>
 *   SimpleHashWheelTimer timer = new SimpleHashWheelTimer();
 * <p>
 *   timer.schedule(new Consumer&lt;Long&gt;() {
 *     public void accept(Long now) {
 *       // run a task
 *     }
 *   }, 5, TimeUnit.SECONDS);
 * </pre></code>
 * </p>
 * <p>
 * NOTE: Without delaying a task, it will be run immediately, in addition to being run after the elapsed time has
 * expired. To run a task only once every N time units and not immediately, use the {@link
 * #schedule(reactor.fn.Consumer, long, java.util.concurrent.TimeUnit, long)} method, which allows you to specify
 * an additional delay that must expire before the task will be executed.
 * </p>
 *
 * @author Jon Brisbin
 * @author Stephane Maldini
 */
public class EventTimer implements Timer {

	private static final Logger LOG = Logger.getLogger(EventTimer.class);

	private final Registry<Long, Consumer<Long>> tasks = Registries.create(true, false, null);
	private final int    resolution;
	private final Thread loop;
	private final AtomicBoolean started = new AtomicBoolean();


	/**
	 * Create a new {@code SimpleHashWheelTimer} using the default resolution of 50ms.
	 */
	Timer create() {
		return create(50);
	}

	/**
	 * Create a new {@code SimpleHashWheelTimer} using the given timer resolution. All times will rounded up to the
	 * closest
	 * multiple of this resolution.
	 *
	 * @param resolution the resolution of this timer, in milliseconds
	 */
	Timer create(final int resolution) {
		return new EventTimer(resolution);
	}


	/**
	 * Create a new {@code SimpleHashWheelTimer} using the given timer resolution. All times will rounded up to the
	 * closest
	 * multiple of this resolution.
	 *
	 * @param resolution the resolution of this timer, in milliseconds
	 */
	private EventTimer(final int resolution) {
		this.resolution = resolution;

		this.loop = new NamedDaemonThreadFactory("simple-hash-wheel-timer").newThread(
		  new Runnable() {
			  @Override
			  public void run() {
				  while (!Thread.currentThread().isInterrupted()) {
					  long now = now(resolution);
					  for (Registration<Long, ? extends Consumer<Long>> reg : tasks.select(now)) {
						  try {
							  if (reg.isCancelled() || reg.isPaused()) {
								  continue;
							  }
							  reg.getObject().accept(now);
						  } catch (CancelException cce) {
							  reg.cancel();
						  } catch (Throwable t) {
							  LOG.error(t.getMessage(), t);
						  } finally {
							  if (reg.isCancelAfterUse()) {
								  reg.cancel();
							  }
						  }
					  }
					  try {
						  Thread.sleep(resolution);
					  } catch (InterruptedException e) {
						  Thread.currentThread().interrupt();
						  return;
					  }
				  }
			  }
		  }
		);
	}

	@Override
	public long period() {
		return resolution;
	}

	@Override
	public Registration<Long, ? extends Consumer<Long>> schedule(Consumer<Long> consumer,
	                                                             long period,
	                                                             TimeUnit timeUnit,
	                                                             long delayInMilliseconds) {
		Assert.isTrue(!loop.isInterrupted(), "Cannot submit tasks to this timer as it has been cancelled.");
		long milliPeriod = TimeUnit.MILLISECONDS.convert(period, timeUnit);
		if (milliPeriod % resolution != 0) {
			throw ReactorFatalException.create(new IllegalArgumentException(
				"Period must be a multiple of timer resolution (e.g. period % resolution == 0 )")
			);
		}
		return tasks.register(
		  new PeriodSelector(milliPeriod, delayInMilliseconds, resolution),
		  consumer
		);
	}

	@Override
	public Registration<Long, ? extends Consumer<Long>> schedule(Consumer<Long> consumer,
	                                                             long period,
	                                                             TimeUnit timeUnit) {
		return schedule(consumer, period, timeUnit, 0);
	}

	@Override
	public Registration<Long, ? extends Consumer<Long>> submit(Consumer<Long> consumer,
	                                                           long delay,
	                                                           TimeUnit timeUnit) {
		Assert.isTrue(!loop.isInterrupted(), "Cannot submit tasks to this timer as it has been cancelled.");
		long ms = TimeUnit.MILLISECONDS.convert(delay, timeUnit);
		return tasks.register(
		  new PeriodSelector(0l, ms, resolution),
		  consumer
		).cancelAfterUse();
	}

	@Override
	public Registration<Long, ? extends Consumer<Long>> submit(Consumer<Long> consumer) {
		return submit(consumer, resolution, TimeUnit.MILLISECONDS);
	}

	/**
	 * Start the Timer
	 */
	@Override
	public void start() {
		if (started.compareAndSet(false, true)) {
			this.loop.start();
		}
		else {
			throw new IllegalStateException("Timer already started");
		}
	}

	@Override
	public void cancel() {
		this.loop.interrupt();
	}

	@Override
	public boolean isCancelled() {
		return loop.isInterrupted();
	}

	private static long now(int resolution) {
		return (long) (Math.ceil(System.currentTimeMillis() / resolution) * resolution);
	}

	private static class PeriodSelector implements Selector<Long> {
		private final long period;
		private final long delay;
		private final long createdMillis;
		private final int  resolution;

		private PeriodSelector(long period, long delay, int resolution) {
			this.period = period;
			this.delay = delay;
			this.resolution = resolution;
			this.createdMillis = now(resolution);
		}

		@Override
		public Object getObject() {
			return period;
		}

		@Override
		public boolean matches(Long key) {
			long now = key;
			long period = (long) (Math.ceil((now - createdMillis) / resolution) * resolution);
			return period >= delay && period % this.period == 0;
		}

		@Override
		public Function<Object, Map<String, Object>> getHeaderResolver() {
			return null;
		}
	}

}
