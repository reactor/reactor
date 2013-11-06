/*
 * Copyright (c) 2011-2013 GoPivotal, Inc. All Rights Reserved.
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

package reactor.spring.factory;

/**
 * An enumeration of the different types of event routing
 *
 * @author Jon Brisbin
 */
public enum EventRouting {

	/**
	 * Events will be routed to every matching consumer
	 */
	BROADCAST_EVENT_ROUTING,

	/**
	 * Events will be routed to one consumer randomly selected from the matching consumers
	 */
	RANDOM_EVENT_ROUTING,

	/**
	 * Events will be routed to one consumer selected from the matching consumers using a random
	 * algorithm
	 */
	ROUND_ROBIN_EVENT_ROUTING
}
