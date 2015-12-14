/*
 * Copyright (c) 2011-2016 Pivotal Software Inc, All Rights Reserved.
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

package reactor.core.timer;

import reactor.core.support.ReactiveState;
import reactor.fn.Consumer;
import reactor.fn.Pausable;

import java.util.concurrent.TimeUnit;

/**
 * @author Jon Brisbin
 * @author Stephane Maldini
 */
public interface Timer extends ReactiveState.Timed {

	/**
	 * Schedule a recurring task. The given {@link reactor.fn.Consumer} will be invoked once every N time units
	 * after the given delay.
	 *
	 * @param consumer            the {@code Consumer} to invoke each period
	 * @param period              the amount of time that should elapse between invocations of the given {@code
	 * Consumer}
	 * @param timeUnit            the unit of time the {@code period} is to be measured in
	 * @param delayInMilliseconds a number of milliseconds in which to delay any execution of the given {@code
	 * Consumer}
	 * @return a {@link reactor.fn.Pausable} that can be used to {@link
	 * reactor.fn.Pausable#cancel() cancel}, {@link reactor.fn.Pausable#pause() pause} or
	 * {@link reactor.fn.Pausable#resume() resume} the given task.
	 */
	Pausable schedule(Consumer<Long> consumer,
	                  long period,
	                  TimeUnit timeUnit,
	                  long delayInMilliseconds);

	/**
	 * Schedule a recurring task. The given {@link reactor.fn.Consumer} will be invoked immediately, as well as
	 * once
	 * every N time units.
	 *
	 * @param consumer the {@code Consumer} to invoke each period
	 * @param period   the amount of time that should elapse between invocations of the given {@code Consumer}
	 * @param timeUnit the unit of time the {@code period} is to be measured in
	 * @return a {@link reactor.fn.Pausable} that can be used to {@link
	 * reactor.fn.Pausable#cancel() cancel}, {@link reactor.fn.Pausable#pause() pause} or
	 * {@link reactor.fn.Pausable#resume() resume} the given task.
	 * @see #schedule(reactor.fn.Consumer, long, java.util.concurrent.TimeUnit, long)
	 */
	Pausable schedule(Consumer<Long> consumer,
	                  long period,
	                  TimeUnit timeUnit);

	/**
	 * Submit a task for arbitrary execution after the given time delay.
	 *
	 * @param consumer the {@code Consumer} to invoke
	 * @param delay    the amount of time that should elapse before invocations of the given {@code Consumer}
	 * @param timeUnit the unit of time the {@code period} is to be measured in
	 * @return a {@link reactor.fn.Pausable} that can be used to {@link
	 * reactor.fn.Pausable#cancel() cancel}, {@link reactor.fn.Pausable#pause() pause} or
	 * {@link reactor.fn.Pausable#resume() resume} the given task.
	 */
	Pausable submit(Consumer<Long> consumer,
	                long delay,
	                TimeUnit timeUnit);

	/**
	 * Submit a task for arbitrary execution after the delay of this timer's resolution.
	 *
	 * @param consumer the {@code Consumer} to invoke
	 * @return {@literal this}
	 */
	Pausable submit(Consumer<Long> consumer);

	/**
	 * Start the Timer, may throw an IllegalStateException if already started
	 */
	void start();

	/**
	 * Cancel this timer by interrupting the task thread. No more tasks can be submitted to this timer after
	 * cancellation.
	 */
	void cancel();

	/**
	 * Is this timer cancelled ?
	 */
	boolean isCancelled();

}
