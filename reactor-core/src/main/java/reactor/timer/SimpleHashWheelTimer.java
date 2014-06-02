package reactor.timer;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import reactor.event.registry.CachingRegistry;
import reactor.event.registry.Registration;
import reactor.event.registry.Registry;
import reactor.event.selector.HeaderResolver;
import reactor.event.selector.Selector;
import reactor.function.Consumer;
import reactor.function.support.CancelConsumerException;
import reactor.support.NamedDaemonThreadFactory;
import reactor.util.Assert;

import java.util.concurrent.TimeUnit;

/**
 * A hashed wheel timer implementation that uses a {@link reactor.event.registry.Registry} and custom {@link
 * reactor.event.selector.Selector Selectors} to determine when tasks should be executed.
 * <p>
 * A {@code SimpleHashWheelTimer} has two variations for scheduling tasks: {@link #schedule(reactor.function.Consumer,
 * long,
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
 *   SimpleHashWheelTimer timer = new SimpleHashWheelTimer();
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
 * @author Stephane Maldini
 */
public class SimpleHashWheelTimer implements Timer {

	private static final Logger LOG = LoggerFactory.getLogger(SimpleHashWheelTimer.class);

	private final Registry<Consumer<Long>> tasks = new CachingRegistry<Consumer<Long>>();
	private final int    resolution;
	private final Thread loop;

	/**
	 * Create a new {@code SimpleHashWheelTimer} using the default resolution of 50ms.
	 */
	public SimpleHashWheelTimer() {
		this(50);
	}

	/**
	 * Create a new {@code SimpleHashWheelTimer} using the given timer resolution. All times will rounded up to the
	 * closest
	 * multiple of this resolution.
	 *
	 * @param resolution
	 * 		the resolution of this timer, in milliseconds
	 */
	public SimpleHashWheelTimer(final int resolution) {
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
								return;
							}
						}
					}
				}
		);
		this.loop.start();
	}

	@Override
	public long getResolution() {
		return resolution;
	}

	@Override
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

	@Override
	public Registration<? extends Consumer<Long>> schedule(Consumer<Long> consumer,
	                                                       long period,
	                                                       TimeUnit timeUnit) {
		return schedule(consumer, period, timeUnit, 0);
	}

	@Override
	public Registration<? extends Consumer<Long>> submit(Consumer<Long> consumer,
	                                                     long delay,
	                                                     TimeUnit timeUnit) {
		Assert.isTrue(!loop.isInterrupted(), "Cannot submit tasks to this timer as it has been cancelled.");
		long ms = TimeUnit.MILLISECONDS.convert(delay, timeUnit);
		return tasks.register(
				new PeriodSelector(ms, ms, resolution),
				consumer
		).cancelAfterUse();
	}

	@Override
	public Registration<? extends Consumer<Long>> submit(Consumer<Long> consumer) {
		return submit(consumer, resolution, TimeUnit.MILLISECONDS);
	}


	@Override
	public void cancel() {
		this.loop.interrupt();
	}

	private static long now(int resolution) {
		return (long)(Math.ceil(System.currentTimeMillis() / resolution) * resolution);
	}

	private static class PeriodSelector implements Selector {
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
