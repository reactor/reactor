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

import reactor.fn.Consumer;
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
	 * Create and assign a context environment bound to the current classloader.
	 *
	 * @return the produced {@link Environment}
	 */
	public static Timers initialize() {
		return new Timers().assignErrorJournal();
	}


	/**
	 * Create and assign a context environment bound to the current classloader only if it not already set. Otherwise
	 * returns
	 * the current context environment
	 *
	 * @return the produced {@link Environment}
	 */
	public static Environment initializeIfEmpty() {
		if (alive()) {
			return get();
		} else {
			return assign(new Environment());
		}
	}

	/**
	 * Assign an environment to the context in order to make it available statically in the application from the
	 * current
	 * classloader.
	 *
	 * @param environment The environment to assign to the current context
	 * @return the assigned {@link Environment}
	 */
	public static Environment assign(Environment environment) {
		if (!enviromentReference.compareAndSet(null, environment)) {
			environment.shutdown();
			throw new IllegalStateException("An environment is already initialized in the current context");
		}
		return environment;
	}

	/**
	 * Read if the context environment has been set
	 *
	 * @return true if context environment is initialized
	 */
	public static boolean alive() {
		return enviromentReference.get() != null;
	}

	/**
	 * Read the context environment. It must have been previously assigned with
	 * {@link this#assign(Environment)}.
	 *
	 * @return the context environment.
	 * @throws java.lang.IllegalStateException if there is no environment initialized.
	 */
	public static Environment get() throws IllegalStateException {
		Environment environment = enviromentReference.get();
		if (environment == null) {
			throw new IllegalStateException("The environment has not been initialized yet");
		}
		return environment;
	}

	/**
	 * Clean and Shutdown the context environment. It must have been previously assigned with
	 * {@link this#assign(Environment)}.
	 *
	 * @throws java.lang.IllegalStateException if there is no environment initialized.
	 */
	public static void terminate() throws IllegalStateException {
		Environment env = get();
		enviromentReference.compareAndSet(env, null);
		env.shutdown();
	}

	/**
	 * Obtain the default globalTimer from the current environment. The globalTimer is created lazily so
	 * it is preferrable to fetch them out of the critical path.
	 * <p>
	 * The default globalTimer is a {@link reactor.fn.timer.HashWheelTimer}. It is suitable for non blocking periodic
	 * work
	 * such as
	 * eventing, memory access, lock=free code, dispatching...
	 *
	 * @return the globalTimer, usually a {@link reactor.fn.timer.HashWheelTimer}
	 */
	public static Timer global() {
		if (null == globalTimer.get()) {
			synchronized (globalTimer) {
				Timer t = new HashWheelTimer();
				if (!globalTimer.compareAndSet(null, t)) {
					t.cancel();
				}
			}
		}
		return globalTimer.get();
	}

	public void shutdown() {
		Timer timer = Timers.globalTimer.get();
		if (null != timer) {
			timer.cancel();
		}
	}

	@Override
	public void close() throws IOException {
		shutdown();
	}

}
