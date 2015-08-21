/*
 * Copyright (c) 2011-2015 Pivotal Software Inc, All Rights Reserved.
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
package reactor.core.subscriber;

import org.reactivestreams.Subscriber;
import org.reactivestreams.Subscription;
import reactor.core.error.SpecificationExceptions;

/**
 * Convenience base class
 *
 * @author Stephane Maldini
 * @since 2.1
 */
public class BaseSubscriber<T> implements Subscriber<T> {
	@Override
	public void onSubscribe(Subscription s) {
		if (s == null) {
			throw SpecificationExceptions.spec_2_13_exception();
		}
	}

	@Override
	public void onNext(T t) {
		if (t == null) {
			throw SpecificationExceptions.spec_2_13_exception();
		}

	}

	@Override
	public void onError(Throwable t) {
		if (t == null) {
			throw SpecificationExceptions.spec_2_13_exception();
		}
	}

	@Override
	public void onComplete() {

	}
}
