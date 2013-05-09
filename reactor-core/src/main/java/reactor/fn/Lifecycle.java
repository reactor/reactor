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

package reactor.fn;

/**
 * Implementations of this interface provide other components with hooks for important lifecycle events.
 *
 * @author Jon Brisbin
 */
public interface Lifecycle {

	/**
	 * Close/Shutdown this object
	 *
	 * @return {@literal this}
	 */
	Lifecycle destroy();

	/**
	 * Pause this object
	 *
	 * @return {@literal this}
	 */
	Lifecycle stop();

	/**
	 * Resume normal behavior
	 *
	 * @return {@literal this}
	 */
	Lifecycle start();

	/**
	 * Is object active?
	 *
	 * @return {@literal alive}
	 */
	boolean isAlive();

}
