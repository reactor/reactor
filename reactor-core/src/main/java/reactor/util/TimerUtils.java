package reactor.util;

import reactor.core.HashWheelTimer;
import reactor.function.Consumer;

import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;

/**
 * @author Jon Brisbin
 */
public abstract class TimerUtils {

	private static final int        DEFAULT_RESOLUTION = 100;
	private static final AtomicLong now                = new AtomicLong();
	private static HashWheelTimer timer;

	protected TimerUtils() {
	}

	public static long approxCurrentTimeMillis() {
		getTimer();
		return now.get();
	}

	public static void setTimer(HashWheelTimer timer) {
		timer.schedule(new Consumer<Long>() {
			@Override
			public void accept(Long aLong) {
				now.set(System.currentTimeMillis());
			}
		}, DEFAULT_RESOLUTION, TimeUnit.MILLISECONDS, DEFAULT_RESOLUTION);
		now.set(System.currentTimeMillis());
		TimerUtils.timer = timer;
	}

	public static HashWheelTimer getTimer() {
		if(null == timer) {
			setTimer(new HashWheelTimer(DEFAULT_RESOLUTION));
		}
		return timer;
	}

}
