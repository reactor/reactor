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

package reactor.core.support;

import java.util.Iterator;

import org.reactivestreams.Publisher;
import org.reactivestreams.Subscriber;

/**
 * A components that supports extra state control and access for reactive components: buffers, capacity, names,
 * upstream...
 * @author Stephane Maldini
 * @since 2.1
 */
public interface ReactiveState {

	/*

	Capacity State : Buffer size (capacity), buffered,...

	 */

	/**
	 * A capacity aware component
	 */
	interface Bounded extends ReactiveState {

		/**
		 * Return defined element capacity, used to drive new {@link org.reactivestreams.Subscription} request needs.
		 * This is the maximum in-flight data allowed to transit to this elements.
		 * @return long capacity
		 */
		long getCapacity();
	}

	/**
	 * A capacity aware component
	 */
	interface Buffering extends Bounded {

		/**
		 * Return current used space in buffer
		 * @return long capacity
		 */
		long pending();
	}

	/*

	Upstream State : Publisher(S), outstanding request, ...

	 */

	/**
	 * A component that is linked to a source {@link Publisher}. Useful to traverse from left to right a pipeline of
	 * reactive actions implementing this interface.
	 */
	interface Upstream extends ReactiveState {

		/**
		 * Return the direct source of data
		 */
		Object upstream();
	}

	/**
	 * A component that is linked to N {@link Publisher}. Useful to traverse from left to right a pipeline of
	 * reactive actions implementing this interface.
	 */
	interface LinkedUpstreams extends ReactiveState {

		/**
		 * Return the connected sources of data
		 */
		Iterator<?> upstreams();

		/**
		 * @return the number of upstreams
		 */
		long upstreamsCount();
	}

	/**
	 * A request aware component
	 */
	interface UpstreamDemand extends ReactiveState {

		/**
		 * Return defined element capacity, used to drive new {@link org.reactivestreams.Subscription} request needs.
		 * This is the maximum in-flight data allowed to transit to this elements.
		 * @return long capacity
		 */
		long expectedFromUpstream();
	}

	/**
	 * A request aware component
	 */
	interface UpstreamPrefetch extends UpstreamDemand {

		/**
		 *
		 * @return
		 */
		long limit();
	}

	/*

	Downstream State : Subscriber(S), Request from downstream...

	 */

	/**
	 * A component that is linked to N target {@link Subscriber}. Useful to traverse from right to left a pipeline of
	 * reactive actions implementing this interface.
	 */
	interface Downstream<T> extends ReactiveState {

		/**
		 * Return the direct data receiver
		 */
		Subscriber<? super T> downstream();
	}


	/**
	 * A component that is linked to N target {@link Subscriber}. Useful to traverse from right to left a pipeline of
	 * reactive actions implementing this interface.
	 */
	interface LinkedDownstreams extends ReactiveState {

		/**
		 * @return the connected data receivers
		 */
		Iterator<?> downstreams();

		/**
		 * @return
		 */
		long downstreamsCount();

	}

	/**
	 * A request aware component
	 */
	interface DownstreamDemand extends ReactiveState {

		/**
		 * Return defined element capacity, used to drive new {@link org.reactivestreams.Subscription} request needs.
		 * This is the maximum in-flight data allowed to transit to this elements.
		 * @return long capacity
		 */
		long requestedFromDownstream();
	}

	/*

	Running State : Name, controls (start, stop, pause),...

	 */

	/**
	 * An identifiable component
	 */
	interface Named extends ReactiveState {

		/**
		 * Return defined identifier
		 */
		String getName();
	}

	/**
	 * A criteria grouped component
	 */
	interface Grouped extends ReactiveState {

		/**
		 * Return defined identifier
		 */
		String getKey();
	}

	/**
	 * A component that is meant to be introspectable on finest logging level
	 */
	interface Trace extends ReactiveState {
	}


	/**
	 * A component that is delegating to a sub-flow (processor, or publisher/subscriber chain)
	 */
	interface FeedbackLoop extends ReactiveState {

		Object delegateInput();

		Object delegateOutput();
	}


	/*

	 */
}
