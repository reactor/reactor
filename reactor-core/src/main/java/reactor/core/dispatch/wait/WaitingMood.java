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

package reactor.core.dispatch.wait;

/**
 * A Component with some waiting capacities such as {@link reactor.core.Dispatcher} that uses a CPU-friendly strategy
 * (e.g. blocking wait) and a CPU-starving strategy (e.g. Thread.yield spining).
 *
 * @author Stephane Maldini
 *
 * @since 2.0
 */
public interface WaitingMood {

	/**
	 * Turn the mood into aggressive CPU demand mode to effectively give an an additional resources boost to the underlying
	 * component.
	 */
	void nervous();

	/**
	 * Turn the mood into eco CPU demand mode to save resources from the underlying component.
	 */
	void calm();

}
