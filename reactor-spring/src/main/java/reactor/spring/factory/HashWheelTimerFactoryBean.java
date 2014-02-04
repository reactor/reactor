package reactor.spring.factory;

import org.springframework.beans.factory.FactoryBean;
import reactor.timer.SimpleHashWheelTimer;
import reactor.function.Supplier;
import reactor.function.Suppliers;
import reactor.timer.Timer;

/**
 * A {@link org.springframework.beans.factory.FactoryBean} implementation that provides {@link
 * reactor.timer.SimpleHashWheelTimer HashWheelTimers} in a round-robin fashion. The default is to create a single timer but
 * using the {@link #HashWheelTimerFactoryBean(int, int)} constructor, one can create a "pool" of timers which will be
 * handed out to requestors in a round-robin fashion.
 *
 * @author Jon Brisbin
 */
public class HashWheelTimerFactoryBean implements FactoryBean<Timer> {

	private final Supplier<Timer> timers;

	/**
	 * Create a single {@link reactor.timer.SimpleHashWheelTimer} with a default resolution of 50 milliseconds.
	 */
	public HashWheelTimerFactoryBean() {
		this(1, 50);
	}

	/**
	 * Create {@code numOfTimers} number of {@link reactor.timer.SimpleHashWheelTimer HashWheelTimers}.
	 *
	 * @param numOfTimers
	 * 		the number of timers to create
	 * @param resolution
	 * 		the resolution of the timers, in milliseconds
	 */
	public HashWheelTimerFactoryBean(int numOfTimers, int resolution) {
		Timer[] timers = new Timer[numOfTimers];
		for(int i = 0; i < numOfTimers; i++) {
			timers[i] = new SimpleHashWheelTimer(resolution);
		}
		this.timers = Suppliers.roundRobin(timers);
	}

	@Override
	public Timer getObject() throws Exception {
		return timers.get();
	}

	@Override public Class<?> getObjectType() {
		return SimpleHashWheelTimer.class;
	}

	@Override public boolean isSingleton() {
		return false;
	}

}
