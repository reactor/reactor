/*
 * Copyright (c) 2011-2015 Pivotal Software Inc, All Rights Reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *       http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package reactor.fn.timer;

import java.util.concurrent.atomic.AtomicReference;

/**
 * A Global Timer
 *
 * @author Stephane Maldini
 * @since 2.1
 */
public class GlobalTimer extends HashWheelTimer {

	private static final AtomicReference<GlobalTimer> globalTimer = new AtomicReference<>();

	public GlobalTimer() {
		super("global-timer", 50, DEFAULT_WHEEL_SIZE, new SleepWait(), null);
	}

	private void _cancel() {
		super.cancel();
	}

	@Override
	public void cancel() {
		// IGNORE
	}

	/**
	 * Obtain the default global timer from the current context. The globalTimer is created lazily so
	 * it is preferrable to fetch them out of the critical path.
	 * <p>
	 * The global timer will also ignore {@link Timer#cancel()} calls and should be cleaned using {@link
	 * #unregister()} ()}.
	 * <p>
	 * The default globalTimer is a {@link HashWheelTimer}. It is suitable for non blocking
	 * periodic
	 * work
	 * such as  eventing, memory access, lock-free code, dispatching...
	 *
	 * @return the globalTimer, usually a {@link HashWheelTimer}
	 */
	public static Timer get() {
		if (null == globalTimer.get()) {
			synchronized (globalTimer) {
				GlobalTimer t = new GlobalTimer();
				if (!globalTimer.compareAndSet(null, t)) {
					t.cancel();
				}
			}
		}
		return globalTimer.get();
	}

	/**
	 * Clean current global timer references and cancel the respective {@link Timer}.
	 * A new global timer can be assigned later with {@link #get()}.
	 */
	public static void unregister() {
		GlobalTimer timer;
		while ((timer = globalTimer.getAndSet(null)) != null) {
			timer._cancel();
		}
	}

	/**
	 * Read if the context timer has been set
	 *
	 * @return true if context timer is initialized
	 */
	public static boolean available() {
		return globalTimer.get() != null;
	}

	/**
	 * The returned timer SHOULD always be cancelled after use, however global timer will ignore it.
	 *
	 * @return eventually the global timer or if not set a fresh timer.
	 */
	public static Timer globalOrNew() {
		Timer timer = globalTimer.get();

		if (timer == null) {
			return new HashWheelTimer(50, 64, new SleepWait());
		} else {
			return timer;
		}
	}

}
