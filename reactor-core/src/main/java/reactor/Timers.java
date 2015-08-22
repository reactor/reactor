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
package reactor;

import reactor.fn.timer.HashWheelTimer;
import reactor.fn.timer.Timer;

import java.io.Closeable;
import java.io.IOException;
import java.util.concurrent.atomic.AtomicReference;

/**
 * @author Stephane Maldini
 * @since 2.1
 */
public final class Timers implements Closeable {

	private static final AtomicReference<Timer> globalTimer = new AtomicReference<Timer>();

	/**
	 * Read if the context timer has been set
	 *
	 * @return true if context timer is initialized
	 */
	public static boolean available() {
		return globalTimer.get() != null;
	}

	/**
	 * Obtain the default global timer from the current context. The globalTimer is created lazily so
	 * it is preferrable to fetch them out of the critical path.
	 * <p>
	 * The default globalTimer is a {@link reactor.fn.timer.HashWheelTimer}. It is suitable for non blocking periodic
	 * work
	 * such as  eventing, memory access, lock=free code, dispatching...
	 *
	 * @return the globalTimer, usually a {@link reactor.fn.timer.HashWheelTimer}
	 */
	public static Timer global() {
		if (null == globalTimer.get()) {
			synchronized (globalTimer) {
				Timer t = new HashWheelTimer(){
					@Override
					public void cancel() {
						globalTimer.compareAndSet(this, null);
						super.cancel();
					}
				};
				if (!globalTimer.compareAndSet(null, t)) {
					t.cancel();
				}
			}
		}
		return globalTimer.get();
	}

	/**
	 * Clean current global timer references and cancel the respective {@link Timer}.
	 * A new global timer can be assigned later with {@link #global()}.
	 */
	public static void unregisterGlobal() {
		Timer timer;
		while ((timer = globalTimer.getAndSet(null)) != null) {
			timer.cancel();
		}
	}

	@Override
	public void close() throws IOException {
		unregisterGlobal();
	}

}
