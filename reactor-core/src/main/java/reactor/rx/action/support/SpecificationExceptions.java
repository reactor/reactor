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
package reactor.rx.action.support;

import org.reactivestreams.Publisher;
import org.reactivestreams.Subscriber;

/**
 * @author Stephane Maldini
 * @since 2.0
 */
public final class SpecificationExceptions {

	public static IllegalStateException spec_2_12_exception() {
		return new IllegalStateException("Spec. Rule 2.12 - Subscriber.onSubscribe MUST NOT be called more than once " +
				"(based on object equality)");
	}

	public static IllegalArgumentException spec_3_09_exception(long elements) {
		return new IllegalArgumentException("Spec. Rule 3.9 - Cannot request a non strictly positive number: " + elements);
	}

	public static IllegalStateException spec_3_17_exception(
			Publisher<?> publisher, Subscriber<?> subscriber, long currentPending, long elements) {
		return new IllegalStateException("Spec. Rule 3.17 - "+(publisher != null ? publisher.getClass().getSimpleName() : "")
		+" to "+subscriber.getClass().getSimpleName()+" - Cannot support pending " + currentPending + " elements " +
				"plus requested " + elements + " elements, it overflows Long.MAX_VALUE ("+(currentPending+elements)+")");
	}
}
