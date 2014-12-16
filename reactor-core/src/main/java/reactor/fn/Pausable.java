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
package reactor.fn;

/**
 * @author Stephane Maldini
 */
public interface Pausable {

	/**
	 * Cancel this {@literal Pausable}. The implementing component should never react to any stimulus,
	 * closing resources if necessary.
	 *
	 * @return {@literal this}
	 */
	Pausable cancel();

	/**
	 * Pause this {@literal Pausable}. The implementing component should stop reacting, pausing resources if necessary.
	 *
	 * @return {@literal this}
	 */
	Pausable pause();

	/**
	 * Unpause this {@literal Pausable}. The implementing component should resume back from a previous pause,
	 * re-activating resources if necessary.
	 *
	 * @return {@literal this}
	 */
	Pausable resume();

}
