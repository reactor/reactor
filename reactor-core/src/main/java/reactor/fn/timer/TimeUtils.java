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
import reactor.fn.Consumer;

import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;

/**
 * @author Jon Brisbin
 */
public abstract class TimeUtils {


	private static final int        DEFAULT_RESOLUTION = 100;
	private static final AtomicLong now                = new AtomicLong();
	private static volatile Timer timer;

	protected TimeUtils() {
	}

	public static long approxCurrentTimeMillis() {
		getTimer();
		return now.get();
	}

	public static void setTimer(Timer timer) {
		timer.schedule(new Consumer<Long>() {
			@Override
			public void accept(Long aLong) {
				now.set(System.currentTimeMillis());
			}
		}, DEFAULT_RESOLUTION, TimeUnit.MILLISECONDS, DEFAULT_RESOLUTION);
		now.set(System.currentTimeMillis());
		TimeUtils.timer = timer;
	}

	public static Timer getTimer() {
		if (null == timer) {
			setTimer(new HashWheelTimer(DEFAULT_RESOLUTION));
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
