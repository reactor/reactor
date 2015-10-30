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
import reactor.fn.Consumer;

import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReferenceFieldUpdater;

/**
 * @author Jon Brisbin
 * @author Stephane Maldini
 */
public final class TimeUtils {


	private static final int      DEFAULT_RESOLUTION = 100;
	private static final Sequence now                = Sequencer.newSequence(-1);

	private static final TimeUtils INSTANCE = new TimeUtils();

	private volatile Timer timer;

	private static final AtomicReferenceFieldUpdater<TimeUtils, Timer> REF = PlatformDependent
			.newAtomicReferenceFieldUpdater(TimeUtils.class, "timer");

	protected TimeUtils() {
	}

	public static long approxCurrentTimeMillis() {
		INSTANCE.getTimer();
		return now.get();
	}

	Timer getTimer() {
		Timer timer = this.timer;
		if (null == timer) {
			timer = new HashWheelTimer(DEFAULT_RESOLUTION);
			if(!REF.compareAndSet(this, null, timer)){
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

}
