/*
 * Copyright (c) 2011-2015 Pivotal Software Inc., Inc. All Rights Reserved.
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
package reactor.core.support;

import java.util.concurrent.TimeUnit;

/**
 * A Resource is a component with an active state, generally consuming hardware capacities (memory, cpu, io).
 * The interface does not contract the creation of the resource but the closing methods, a graceful shutdown and an
 * enforced shutdown.
 *
 * @author Stephane Maldini
 * @since 2.0
 */
public interface Resource {

	/**
	 * Determine whether this {@code Resource} can be used.
	 *
	 * @return {@literal true} if this {@code Resource} is alive and can be used, {@literal false} otherwise.
	 */
	boolean alive();

	/**
	 * Shutdown this active {@code Resource} such that it can no longer be used. If the resource carries any work,
	 * it will wait (but NOT blocking the caller) for all the remaining tasks to perform before closing the resource.
	 */
	void shutdown();


	/**
	 * Block until all submitted tasks have completed, then do a normal {@link #shutdown()}.
	 */
	boolean awaitAndShutdown();

	/**
	 * Block until all submitted tasks have completed, then do a normal {@link #shutdown()}.
	 */
	boolean awaitAndShutdown(long timeout, TimeUnit timeUnit);

	/**
	 * Shutdown this {@code Resource}, forcibly halting any work currently executing and discarding any tasks that
	 * have not yet been executed.
	 */
	void forceShutdown();

}
