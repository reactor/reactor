package reactor.core;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import reactor.event.registry.CachingRegistry;
import reactor.event.registry.Registration;
import reactor.event.registry.Registry;
import reactor.event.selector.HeaderResolver;
import reactor.event.selector.Selector;
import reactor.function.Consumer;
import reactor.function.support.CancelConsumerException;
import reactor.function.support.SingleUseConsumer;
import reactor.support.NamedDaemonThreadFactory;
import reactor.util.Assert;
import reactor.util.UUIDUtils;

import java.util.UUID;
import java.util.concurrent.TimeUnit;

/**
 * A hashed wheel timer implementation that uses a {@link reactor.event.registry.Registry} and custom {@link
 * reactor.event.selector.Selector Selectors} to determine when tasks should be executed.
 * <p>
 * A {@code HashWheelTimer} has two variations for scheduling tasks: {@link #schedule(reactor.function.Consumer, long,
 * java.util.concurrent.TimeUnit)} and {@link #schedule(reactor.function.Consumer, long, java.util.concurrent.TimeUnit,
 * long)} which are for scheduling repeating tasks, and {@link #submit(reactor.function.Consumer, long,
 * java.util.concurrent.TimeUnit)} which is for scheduling single-run delayed tasks.
 * </p>
 * <p>
 * To schedule a repeating task, specify the period of time which should elapse before invoking the given {@link
 * reactor.function.Consumer}. To schedule a task that repeats every 5 seconds, for example, one would do something
 * like:
 * </p>
 * <p>
 * <code><pre>
 *   HashWheelTimer timer = new HashWheelTimer();
 *
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
 * #schedule(reactor.function.Consumer, long, java.util.concurrent.TimeUnit, long)} method, which allows you to specify
 * an additional delay that must expire before the task will be executed.
 * </p>
 *
 * @author Jon Brisbin
 */
public class HashWheelTimer {

	private static final Logger LOG = LoggerFactory.getLogger(HashWheelTimer.class);

	private final Registry<Consumer<Long>> tasks = new CachingRegistry<Consumer<Long>>(false);
	private final int    resolution;
	private final Thread loop;

	/**
	 * Create a new {@code HashWheelTimer} using the default resolution of 50ms.
	 */
	public HashWheelTimer() {
		this(50);
	}

	/**
	 * Create a new {@code HashWheelTimer} using the given timer resolution. All times will rounded up to the closest
	 * multiple of this resolution.
	 *
	 * @param resolution
	 * 		the resolution of this timer, in milliseconds
	 */
	public HashWheelTimer(final int resolution) {
		this.resolution = resolution;

		this.loop = new NamedDaemonThreadFactory("hash-wheel-timer").newThread(
				new Runnable() {
					@Override
					public void run() {
						while(!Thread.currentThread().isInterrupted()) {
							long now = now(resolution);
							for(Registration<? extends Consumer<Long>> reg : tasks.select(now)) {
								try {
									if(reg.isCancelled() || reg.isPaused()) {
										continue;
									}
									reg.getObject().accept(now);
								} catch(CancelConsumerException cce) {
									reg.cancel();
								} catch(Throwable t) {
									LOG.error(t.getMessage(), t);
								} finally {
									if(reg.isCancelAfterUse()) {
										reg.cancel();
									}
								}
							}
							try {
								Thread.sleep(resolution);
							} catch(InterruptedException e) {
								Thread.currentThread().interrupt();
							}
						}
					}
				}
		);
		this.loop.start();
	}

	/**
	 * Schedule a recurring task. The given {@link reactor.function.Consumer} will be invoked once every N time units
	 * after the given delay.
	 *
	 * @param consumer
	 * 		the {@code Consumer} to invoke each period
	 * @param period
	 * 		the amount of time that should elapse between invocations of the given {@code Consumer}
	 * @param timeUnit
	 * 		the unit of time the {@code period} is to be measured in
	 * @param delayInMilliseconds
	 * 		a number of milliseconds in which to delay any execution of the given {@code Consumer}
	 *
	 * @return a {@link reactor.event.registry.Registration} that can be used to {@link
	 * reactor.event.registry.Registration#cancel() cancel}, {@link reactor.event.registry.Registration#pause() pause} or
	 * {@link reactor.event.registry.Registration#resume() resume} the given task.
	 */
	public Registration<? extends Consumer<Long>> schedule(Consumer<Long> consumer,
	                                                       long period,
	                                                       TimeUnit timeUnit,
	                                                       long delayInMilliseconds) {
		Assert.isTrue(!loop.isInterrupted(), "Cannot submit tasks to this timer as it has been cancelled.");
		return tasks.register(
				new PeriodSelector(TimeUnit.MILLISECONDS.convert(period, timeUnit), delayInMilliseconds, resolution),
				consumer
		);
	}

	/**
	 * Schedule a recurring task. The given {@link reactor.function.Consumer} will be invoked immediately, as well as
	 * once
	 * every N time units.
	 *
	 * @param consumer
	 * 		the {@code Consumer} to invoke each period
	 * @param period
	 * 		the amount of time that should elapse between invocations of the given {@code Consumer}
	 * @param timeUnit
	 * 		the unit of time the {@code period} is to be measured in
	 *
	 * @return a {@link reactor.event.registry.Registration} that can be used to {@link
	 * reactor.event.registry.Registration#cancel() cancel}, {@link reactor.event.registry.Registration#pause() pause} or
	 * {@link reactor.event.registry.Registration#resume() resume} the given task.
	 *
	 * @see #schedule(reactor.function.Consumer, long, java.util.concurrent.TimeUnit, long)
	 */
	public Registration<? extends Consumer<Long>> schedule(Consumer<Long> consumer,
	                                                       long period,
	                                                       TimeUnit timeUnit) {
		return schedule(consumer, period, timeUnit, 0);
	}

	/**
	 * Submit a task for arbitrary execution after the given time delay.
	 *
	 * @param consumer
	 * 		the {@code Consumer} to invoke
	 * @param delay
	 * 		the amount of time that should elapse before invocations of the given {@code Consumer}
	 * @param timeUnit
	 * 		the unit of time the {@code period} is to be measured in
	 *
	 * @return a {@link reactor.event.registry.Registration} that can be used to {@link
	 * reactor.event.registry.Registration#cancel() cancel}, {@link reactor.event.registry.Registration#pause() pause} or
	 * {@link reactor.event.registry.Registration#resume() resume} the given task.
	 */
	public Registration<? extends Consumer<Long>> submit(Consumer<Long> consumer,
	                                                     long delay,
	                                                     TimeUnit timeUnit) {
		Assert.isTrue(!loop.isInterrupted(), "Cannot submit tasks to this timer as it has been cancelled.");
		long ms = TimeUnit.MILLISECONDS.convert(delay, timeUnit);
		return tasks.register(
				new PeriodSelector(ms, ms, resolution),
				new SingleUseConsumer<Long>(consumer)
		).cancelAfterUse();
	}

	/**
	 * Submit a task for arbitrary execution after the delay of this timer's resolution.
	 *
	 * @param consumer
	 * 		the {@code Consumer} to invoke
	 *
	 * @return {@literal this}
	 */
	public HashWheelTimer submit(Consumer<Long> consumer) {
		submit(consumer, resolution, TimeUnit.MILLISECONDS);
		return this;
	}


	/**
	 * Cancel this timer by interrupting the task thread. No more tasks can be submitted to this timer after
	 * cancellation.
	 */
	public void cancel() {
		this.loop.interrupt();
	}

	private static long now(int resolution) {
		return (long)(Math.ceil(System.currentTimeMillis() / resolution) * resolution);
	}

	private static class PeriodSelector implements Selector {
		private final UUID uuid = UUIDUtils.create();
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
		public UUID getId() {
			return uuid;
		}

		@Override
		public Object getObject() {
			return period;
		}

		@Override
		public boolean matches(Object key) {
			long now = (Long)key;
			long period = (long)(Math.ceil((now - createdMillis) / resolution) * resolution);
			return period >= delay && period % this.period == 0;
		}

		@Override
		public HeaderResolver getHeaderResolver() {
			return null;
		}
	}

}
