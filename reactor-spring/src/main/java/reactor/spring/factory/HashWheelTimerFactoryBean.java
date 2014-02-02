package reactor.spring.factory;

import org.springframework.beans.factory.FactoryBean;
import reactor.event.timer.HashWheelTimer;
import reactor.function.Supplier;
import reactor.function.Suppliers;

/**
 * A {@link org.springframework.beans.factory.FactoryBean} implementation that provides {@link
 * reactor.event.timer.HashWheelTimer HashWheelTimers} in a round-robin fashion. The default is to create a single timer but
 * using the {@link #HashWheelTimerFactoryBean(int, int)} constructor, one can create a "pool" of timers which will be
 * handed out to requestors in a round-robin fashion.
 *
 * @author Jon Brisbin
 */
public class HashWheelTimerFactoryBean implements FactoryBean<HashWheelTimer> {

	private final Supplier<HashWheelTimer> timers;

	/**
	 * Create a single {@link reactor.event.timer.HashWheelTimer} with a default resolution of 50 milliseconds.
	 */
	public HashWheelTimerFactoryBean() {
		this(1, 50);
	}

	/**
	 * Create {@code numOfTimers} number of {@link reactor.event.timer.HashWheelTimer HashWheelTimers}.
	 *
	 * @param numOfTimers
	 * 		the number of timers to create
	 * @param resolution
	 * 		the resolution of the timers, in milliseconds
	 */
	public HashWheelTimerFactoryBean(int numOfTimers, int resolution) {
		HashWheelTimer[] timers = new HashWheelTimer[numOfTimers];
		for(int i = 0; i < numOfTimers; i++) {
			timers[i] = new HashWheelTimer(resolution);
		}
		this.timers = Suppliers.roundRobin(timers);
	}

	@Override public HashWheelTimer getObject() throws Exception {
		return timers.get();
	}

	@Override public Class<?> getObjectType() {
		return HashWheelTimer.class;
	}

	@Override public boolean isSingleton() {
		return false;
	}

}
