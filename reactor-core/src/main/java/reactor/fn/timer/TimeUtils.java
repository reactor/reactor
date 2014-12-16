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

import reactor.fn.Consumer;

import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;

/**
 * @author Jon Brisbin
 */
public abstract class TimeUtils {

	private static final int        DEFAULT_RESOLUTION = 100;
	private static final AtomicLong now                = new AtomicLong();
	private static Timer timer;

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
		if(null == timer) {
			setTimer(new SimpleHashWheelTimer(DEFAULT_RESOLUTION));
		}
		return timer;
	}

}
