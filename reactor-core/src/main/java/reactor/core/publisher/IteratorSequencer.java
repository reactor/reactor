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
package reactor.core.publisher;

import java.util.Iterator;

import org.reactivestreams.Subscriber;
import reactor.core.subscriber.SubscriberWithContext;
import reactor.fn.Consumer;
import reactor.fn.Function;

/**
 * Simple Publisher implementations
 */

//

public final class IteratorSequencer<T>
		implements Consumer<SubscriberWithContext<T, Iterator<? extends T>>>,
		           Function<Subscriber<? super T>, Iterator<? extends T>> {

	private final Iterable<T> defaultValues;

	public IteratorSequencer(Iterable<T> defaultValues) {
		this.defaultValues = defaultValues;
	}

	@Override
	public void accept(SubscriberWithContext<T, Iterator<? extends T>> subscriber) {
		final Iterator<? extends T> iterator = subscriber.context();
		if (iterator.hasNext()) {
			subscriber.onNext(iterator.next());
		}
		else {
			subscriber.onComplete();
		}
	}

	@Override
	public Iterator<? extends T> apply(Subscriber<? super T> subscriber) {
		if (defaultValues == null) {
			throw PublisherFactory.PrematureCompleteException.INSTANCE;
		}
		Iterator<? extends T> it = defaultValues.iterator();
		if(!it.hasNext()){
			throw PublisherFactory.PrematureCompleteException.INSTANCE;
		}
		return it;
	}

	@Override
	public String toString() {
		return "iterable=" + defaultValues;
	}
}
