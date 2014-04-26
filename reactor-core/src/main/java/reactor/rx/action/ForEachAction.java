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

import org.reactivestreams.spi.Subscriber;
import reactor.event.dispatch.Dispatcher;
import reactor.rx.StreamSubscription;

import java.util.Collection;
import java.util.Iterator;
import java.util.concurrent.atomic.AtomicLong;

/**
 * @author Stephane Maldini
 * @since 1.1
 */
public class ForEachAction<T> extends Action<Iterable<T>, T> {

	final private Iterable<T> defaultValues;
	final private boolean     infinite;

	public ForEachAction(Dispatcher dispatcher) {
		this(null, dispatcher);
	}


	public ForEachAction(Iterable<T> defaultValues,
	                     Dispatcher dispatcher) {
		super(dispatcher);
		this.defaultValues = defaultValues;
		if (defaultValues != null) {
			if (Collection.class.isAssignableFrom(defaultValues.getClass())) {
				prefetch(((Collection<T>) defaultValues).size());
			}
			setKeepAlive(true);
		}
		this.infinite = batchSize == -1;
	}

	@Override
	protected StreamSubscription<T> createSubscription(Subscriber<T> subscriber) {
		if (defaultValues != null) {
			return new StreamSubscription<T>(this, subscriber) {
				AtomicLong cursor = new AtomicLong();

				@Override
				public void requestMore(int elements) {
					super.requestMore(elements);

					long i = 0;
					Iterator<T> iterator = defaultValues.iterator();

					while (i < cursor.get() && iterator.hasNext()) {
						iterator.next();
						i++;
					}
					i = 0;

					while (i < elements && iterator.hasNext()) {
						cursor.getAndIncrement();
						onNext(iterator.next());
						i++;
					}

					if (!iterator.hasNext()) {
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
		if (values == null) return;
		int i = 0;
		for (T it : values) {
			broadcastNext(it);
			i++;
			if (defaultValues == null && !infinite && i % batchSize == 0) {
				broadcastFlush();
			}
		}
		if (defaultValues == null && infinite) {
			broadcastFlush();
		}
	}

	@Override
	protected void doFlush() {
		if (defaultValues != null) {
			doNext(defaultValues);
			doComplete();
		}
	}

}
