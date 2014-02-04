package reactor.timer;

import reactor.event.lifecycle.Lifecycle;
import reactor.event.registry.Registration;
import reactor.function.Consumer;

import java.util.concurrent.TimeUnit;

public interface Timer {

  /**
   * Submit a task for arbitrary execution after the given time delay.
   *
   * @param consumer
   *    the {@code Consumer} to invoke
   * @param period
   *    the amount of time that should elapse before invocations of the given {@code Consumer}
   * @param timeUnit
   *    the unit of time the {@code period} is to be measured in
   *
   * @return a {@link reactor.event.registry.Registration} that can be used to {@link
   * reactor.event.registry.Registration#cancel() cancel}, {@link reactor.event.registry.Registration#pause() pause} or
   * {@link reactor.event.registry.Registration#resume() resume} the given task.
   */
  public Registration<? extends Consumer<Long>> schedule(Consumer<Long> consumer,
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
  public Registration<? extends Consumer<Long>> schedule(Consumer<Long> consumer,
                                                         long period,
                                                         TimeUnit timeUnit);


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
  public Registration<? extends Consumer<Long>> submit(Consumer<Long> consumer,
                                                       long period,
                                                       TimeUnit timeUnit);

  /**
   * Cancel this timer
   */
  public void cancel();

  /**
   * Cancel this timer
   */
  public void cancelAll();
}

