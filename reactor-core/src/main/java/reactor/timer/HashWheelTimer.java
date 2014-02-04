package reactor.timer;

import com.lmax.disruptor.EventFactory;
import com.lmax.disruptor.RingBuffer;
import reactor.event.lifecycle.Pausable;
import reactor.event.registry.Registration;
import reactor.event.selector.Selector;
import reactor.function.Consumer;
import reactor.support.NamedDaemonThreadFactory;
import reactor.support.TimeUtils;
import reactor.timer.Timer;
import reactor.util.Assert;

import java.util.Set;
import java.util.TreeSet;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.locks.LockSupport;

/**
 * A hashed wheel timer implementation that uses a {@link reactor.event.registry.Registry} and custom {@link
 * reactor.event.selector.Selector Selectors} to determine when tasks should be executed.
 *
 * WARNING: This timer will occupy an entire core. It will busy-spin waiting for the next timeout.
 *
 * <p>
 * A {@code HashWheelTimer} has two variations for scheduling tasks: {@link #schedule(reactor.function.Consumer, long,
 * java.util.concurrent.TimeUnit)} and {@link #schedule(reactor.function.Consumer, long, java.util.concurrent.TimeUnit,
 * long)} which are for scheduling repeating tasks, and {@link #submit(reactor.function.Consumer, long,
 * java.util.concurrent.TimeUnit)} which is for scheduling single-run delayed tasks.
 * </p>
 * <p>
 * To reschedule a repeating task, specify the period of time which should elapse before invoking the given {@link
 * reactor.function.Consumer}. To reschedule a task that repeats every 5 seconds, for example, one would do something
 * like:
 * </p>
 * <p>
 * <code><pre>
 *   HashWheelTimer timer = new HashWheelTimer();
 *
 *   timer.reschedule(new Consumer&lt;Long&gt;() {
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
 * @author Oleksandr Petrov
 */
public class HashWheelTimer implements Timer {

  public static HashWheelTimer instance;

  static {
    instance = new HashWheelTimer();
    instance.start();
  }

  public void resetTimerTo(int resolution) {
    instance.cancel();
    instance = new HashWheelTimer(resolution);
    instance.start();
  }

  public void resetTimerTo(int resolution, int wheelSize) {
    instance.cancel();
    instance = new HashWheelTimer(resolution, wheelSize);
    instance.start();
  }

  public static final int DEFAULT_WHEEL_SIZE = 512;

  private final RingBuffer<Set<TimerRegistration>> wheel;
  private final int                                resolution;
  private final Thread                             loop;
  private final ExecutorService                    executor;


  /**
   * Create a new {@code HashWheelTimer} using the given with default resolution of 100 milliseconds and
   * default wheel size.
   */
  private HashWheelTimer() {
    this(10, DEFAULT_WHEEL_SIZE);
  }

  /**
   * Create a new {@code HashWheelTimer} using the given timer resolution. All times will rounded up to the closest
   * multiple of this resolution.
   *
   * @param resolution
   * 		the resolution of this timer, in milliseconds
   */
  private HashWheelTimer(int resolution) {
    this(resolution, DEFAULT_WHEEL_SIZE);
  }

  /**
   * Create a new {@code HashWheelTimer} using the given timer {@data resolution} and {@data wheelSize}. All times will
   * rounded up to the closest multiple of this resolution.
   *
   * @param res resolution of this timer in milliseconds
   * @param wheelSize size of the Ring Buffer supporting the Timer, the larger the wheel, the less the lookup time is
   *                  for sparse timeouts. Sane default is 512.
   */
  private HashWheelTimer(int res, int wheelSize) {
    this.wheel = RingBuffer.createSingleProducer(new EventFactory<Set<TimerRegistration>>() {
      @Override
      public Set<TimerRegistration> newInstance() {
        return new TreeSet<TimerRegistration>();
      }
    }, wheelSize);

    this.resolution = res;
    this.loop = new NamedDaemonThreadFactory("hash-wheel-timer").newThread(
        new Runnable() {
          @Override
          public void run() {
            long deadline = System.currentTimeMillis();
            while(true) {
              long now = System.currentTimeMillis();
              if(now >= deadline) {
                deadline += resolution;
                if(Thread.currentThread().isInterrupted()) {
                  return;
                }
              } else {
                continue;
              }

              Set<TimerRegistration> registrations = wheel.get(wheel.getCursor());

              for(TimerRegistration r: registrations) {
                if (r.isCancelled()) {
                  registrations.remove(r);
                } else if (r.ready()) {
                  executor.submit(r);
                  registrations.remove(r);

                  if(!r.isCancelAfterUse()) {
                    reschedule(r);
                  }
                } else if (r.isPaused()) {
                  reschedule(r);
                } else {
                  r.decrement();
                }
              }

              wheel.publish(wheel.next());
            }
          }
        });

    this.executor = Executors.newSingleThreadExecutor();
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
  public TimerRegistration<? extends Consumer<Long>> schedule(Consumer<Long> consumer,
                                                              long period,
                                                              TimeUnit timeUnit,
                                                              long delayInMilliseconds) {
    Assert.isTrue(!loop.isInterrupted(), "Cannot submit tasks to this timer as it has been cancelled.");
    return schedule(TimeUnit.MILLISECONDS.convert(period, timeUnit), delayInMilliseconds, consumer);
  }

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
  public TimerRegistration<? extends Consumer<Long>> submit(Consumer<Long> consumer,
                                                            long period,
                                                            TimeUnit timeUnit) {
    Assert.isTrue(!loop.isInterrupted(), "Cannot submit tasks to this timer as it has been cancelled.");
    long ms = TimeUnit.MILLISECONDS.convert(period, timeUnit);
    return schedule(ms, ms, consumer).cancelAfterUse();
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
  public TimerRegistration<? extends Consumer<Long>> schedule(Consumer<Long> consumer,
                                                              long period,
                                                              TimeUnit timeUnit) {
    return schedule(TimeUnit.MILLISECONDS.convert(period, timeUnit), 0, consumer);
  }

  private TimerRegistration<? extends Consumer<Long>> schedule(long recurringTimeout, long firstDelay, Consumer<Long> consumer) {
    Assert.isTrue(recurringTimeout >= resolution, "Cannot schedule tasks for amount of time less than timer precision.");

    long offset = recurringTimeout / resolution;
    long rounds = offset / wheel.getBufferSize();

    long firstFireOffset = firstDelay / resolution;
    long firstFireRounds = firstFireOffset / wheel.getBufferSize();

    TimerRegistration r = new TimerRegistration(firstFireRounds, offset, consumer, rounds);
    wheel.get(wheel.getCursor() + firstFireOffset + 1).add(r);
    return r;
  }

  /**
   * Rechedule a {@link TimerRegistration}  for the next fire
   * @param registration
   */
  public void reschedule(TimerRegistration registration) {
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
  @Override
  public void cancel() {
    this.loop.interrupt();
  }

  /**
   * Cancel current Timer
   */
  @Override
  public void cancelAll() {
    for(int i = 0; i < wheel.getBufferSize(); i++) {
      wheel.get(i).clear();
    }
  }

  /**
   * Timer Registration
   * @param <T> type of the Timer Registration Consumer
   */
  public static class TimerRegistration<T extends Consumer<Long>> implements Runnable, Comparable, Pausable, Registration {

    public static int STATUS_PAUSED = 1;
    public static int STATUS_CANCELLED = -1;
    public static int STATUS_READY = 0;

    private final  T            delegate;
    private final long          rescheduleRounds;
    private final long          scheduleOffset;
    private final AtomicLong    rounds;
    private final AtomicInteger status;
    private final AtomicBoolean cancelAfterUse;

    /**
     * Creates a new Timer Registration with given {@data rounds}, {@data offset} and {@data delegate}.
     *
     * @param rounds amount of rounds the Registration should go through until it's elapsed
     * @param offset offset of in the Ring Buffer for rescheduling
     * @param delegate delegate that will be ran whenever the timer is elapsed
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
     * @return current Registration
     */
    public Registration cancel() {
      this.status.set(STATUS_CANCELLED);
      return this;
    }

    /**
     * Check whether the current Registration is cancelled
     * @return whether or not the current Registration is cancelled
     */
    @Override
    public boolean isCancelled() {
      return status.get() == STATUS_CANCELLED;
    }

    /**
     * Pause the current Regisration
     * @return current Registration
     */
    @Override
    public Registration pause() {
      this.status.set(STATUS_PAUSED);
      return this;
    }

    /**
     * Check whether the current Registration is paused
     * @return whether or not the current Registration is paused
     */
    @Override
    public boolean isPaused() {
      return this.status.get() == STATUS_PAUSED;
    }

    /**
     * Resume current Registration
     * @return current Registration
     */
    @Override
    public Registration resume() {
      this.status.set(STATUS_READY);
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
      return null;
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
      TimerRegistration other = (TimerRegistration) o;
      return Long.compare(rounds.get(), other.rounds.get());
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

}
