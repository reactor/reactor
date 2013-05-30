/*
 * Copyright (c) 2011-2013 the original author or authors.
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

package reactor.fn.dispatch;

/**
 * Implementations of this interface provide a {@link Task} holder which the called can fill in with the details of the
 * task execution it desires to have scheduled. Calling {@link reactor.fn.dispatch.Task#submit()} will cause the task to
 * be submitted for execution using whatever method the implementation chooses to use.
 *
 * @author Jon Brisbin
 */
public interface Dispatcher {

	/**
	 * Determine whether this {@code Dispatcher} can accept {@link Task} submissions.
	 *
	 * @return {@literal true} is this {@code Dispatcher} is alive, {@literal false} otherwise.
	 */
	boolean alive();

	/**
	 * Shutdown this object.
	 */
	void shutdown();

	/**
	 * Shutdown this {@code Dispatcher} and forcibly halt any tasks currently executing, and clear the queues of any
	 * submitted tasks not yet executed.
	 */
	void halt();

	/**
	 * Return to the caller a {@link Task} object that holds the information required to dispatch an event to a set of
	 * consumers.
	 *
	 * @return A {@link Task} object, probably when a pool, used to hold the various parts of a dispatch event.
	 */
	<T> Task<T> nextTask();
}
