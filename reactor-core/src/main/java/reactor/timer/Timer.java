package reactor.timer;

import reactor.event.registry.Registration;
import reactor.function.Consumer;

import java.util.concurrent.TimeUnit;

/**
 * @author Jon Brisbin
 */
public interface Timer {
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
	Registration<? extends Consumer<Long>> schedule(Consumer<Long> consumer,
	                                                long period,
	                                                TimeUnit timeUnit,
	                                                long delayInMilliseconds);

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
	Registration<? extends Consumer<Long>> schedule(Consumer<Long> consumer,
	                                                long period,
	                                                TimeUnit timeUnit);

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
	Registration<? extends Consumer<Long>> submit(Consumer<Long> consumer,
	                                              long delay,
	                                              TimeUnit timeUnit);

	/**
	 * Submit a task for arbitrary execution after the delay of this timer's resolution.
	 *
	 * @param consumer
	 * 		the {@code Consumer} to invoke
	 *
	 * @return {@literal this}
	 */
	Timer submit(Consumer<Long> consumer);

	/**
	 * Cancel this timer by interrupting the task thread. No more tasks can be submitted to this timer after
	 * cancellation.
	 */
	void cancel();
}
