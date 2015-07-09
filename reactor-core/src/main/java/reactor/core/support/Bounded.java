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
package reactor.core.support;

import reactor.core.Dispatcher;

/**
 * A capacity aware component
 *
 * @author Stephane Maldini
 */
public interface Bounded {

	/**
	 * Get the assigned {@link reactor.core.Dispatcher}.
	 *
	 * @return true if the component wishes to use a back-pressure ready message-passing (e.g., ReactiveSubscription)
	 */
	boolean isReactivePull(Dispatcher dispatcher, long producerCapacity);

	/**
	 * Return defined element capacity, used to drive new {@link org.reactivestreams.Subscription}
	 * request needs. This is the maximum in-flight data allowed to transit to this elements.
	 *
	 * @return long capacity
	 */
	long getCapacity();
}
