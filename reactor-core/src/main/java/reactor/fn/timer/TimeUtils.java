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

import reactor.core.error.ReactorFatalException;
import reactor.core.processor.rb.disruptor.Sequence;
import reactor.core.processor.rb.disruptor.Sequencer;
import reactor.core.support.internal.PlatformDependent;
import reactor.core.support.wait.SleepingWaitStrategy;
import reactor.fn.Consumer;
import reactor.fn.LongSupplier;
import reactor.fn.Pausable;

import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReferenceFieldUpdater;

/**
 * @author Jon Brisbin
 * @author Stephane Maldini
 */
public final class TimeUtils {

	final static private LongSupplier SYSTEM_NOW = new LongSupplier() {
		@Override
		public long get() {
			return System.currentTimeMillis();
		}
	};

	final static private Timer NOOP = new NoopTimer();

	private static final int      DEFAULT_RESOLUTION = 100;
	private static final Sequence now                = Sequencer.newSequence(-1);

	private static final TimeUtils INSTANCE = new TimeUtils();

	private volatile Timer timer = NOOP;

	private static final AtomicReferenceFieldUpdater<TimeUtils, Timer> REF =
			PlatformDependent.newAtomicReferenceFieldUpdater(TimeUtils.class, "timer");

	protected TimeUtils() {
	}

	public static long approxCurrentTimeMillis() {
		INSTANCE.getTimer();
		return now.get();
	}

	public static boolean isEnabled(){
		return INSTANCE.timer != NOOP;
	}

	public static void enable(){
		INSTANCE.getTimer();
	}

	public static void disable(){
		INSTANCE.cancelTimer();
	}

	public static LongSupplier currentTimeMillisResolver(){
		if (isEnabled()){
			return now;
		}
		else{
			return SYSTEM_NOW;
		}
	}

	void cancelTimer(){
		Timer timer;
		for(;;){
			timer = this.timer;
			if(REF.compareAndSet(this, timer, NOOP)){
				timer.cancel();
				break;
			}
		}
	}

	Timer getTimer() {
		Timer timer = this.timer;
		if (NOOP == timer) {
			timer = new HashWheelTimer("time-utils", DEFAULT_RESOLUTION, HashWheelTimer.DEFAULT_WHEEL_SIZE, new
					SleepingWaitStrategy(), null);
			if(!REF.compareAndSet(this, NOOP, timer)){
				timer.cancel();
				timer = this.timer;
			}
			else {
				timer.start();
			}
		}
		return timer;
	}

	public static void checkResolution(long time, long resolution) {
		if (time % resolution != 0) {
			throw ReactorFatalException.create(new IllegalArgumentException(
			  "Period must be a multiple of Timer resolution (e.g. period % resolution == 0 ). " +
				"Resolution for this Timer is: " + resolution + "ms"
			));
		}
	}

	private final static class NoopTimer implements Timer{

		@Override
		public long getResolution() {
			return 0;
		}

		@Override
		public Pausable schedule(Consumer<Long> consumer, long period, TimeUnit timeUnit, long delayInMilliseconds) {
			return null;
		}

		@Override
		public Pausable schedule(Consumer<Long> consumer, long period, TimeUnit timeUnit) {
			return null;
		}

		@Override
		public Pausable submit(Consumer<Long> consumer, long delay, TimeUnit timeUnit) {
			return null;
		}

		@Override
		public Pausable submit(Consumer<Long> consumer) {
			return null;
		}

		@Override
		public void start() {

		}

		@Override
		public void cancel() {

		}

		@Override
		public boolean isCancelled() {
			return false;
		}
	}

}
