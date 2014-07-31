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
package reactor.rx.action;

import org.reactivestreams.Subscriber;
import reactor.event.dispatch.Dispatcher;
import reactor.rx.StreamSubscription;

import java.util.Collection;
import java.util.Iterator;

/**
 * @author Stephane Maldini
 * @since 1.1
 */
public class ForEachAction<T> extends Action<Iterable<T>, T> {

	final private Iterable<T> defaultValues;

	public ForEachAction(Dispatcher dispatcher) {
		this(null, dispatcher);
	}


	public ForEachAction(Iterable<T> defaultValues,
	                     Dispatcher dispatcher) {
		super(dispatcher);
		this.defaultValues = defaultValues;
		if (defaultValues != null) {
			state = State.COMPLETE;
			if (Collection.class.isAssignableFrom(defaultValues.getClass())) {
				capacity(((Collection<T>) defaultValues).size());
			}
			setKeepAlive(true);
		}
	}

	@Override
	protected void checkAndSubscribe(Subscriber<T> subscriber, StreamSubscription<T> subscription) {
		if (state == State.SHUTDOWN) {
			subscriber.onError(new IllegalStateException("Publisher has shutdown"));
		} else if (state == State.ERROR) {
			subscriber.onError(error);
		} else if (addSubscription(subscription)) {
			subscriber.onSubscribe(subscription);
		}
	}

	@Override
	protected StreamSubscription<T> createSubscription(Subscriber<T> subscriber) {
		if (defaultValues != null) {
			return new StreamSubscription<T>(this, subscriber) {
				Iterator<T> iterator = defaultValues.iterator();

				@Override
				public void request(int elements) {
					super.request(elements);

					if(buffer.isComplete()) return;

					long i = 0;
					while (i < elements && iterator.hasNext()) {
						onNext(iterator.next());
						i++;
					}

					if (!iterator.hasNext() && !buffer.isComplete()) {
						onComplete();
					}
				}
			};
		} else {
			return super.createSubscription(subscriber);
		}
	}

	@Override
	protected void doNext(Iterable<T> values) {
		if (values == null) {
			broadcastNext(null);
			return;
		}
		for (T it : values) {
			broadcastNext(it);
		}
	}

}
