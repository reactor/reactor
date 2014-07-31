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
import reactor.event.selector.ClassSelector;
import reactor.rx.StreamSubscription;

/**
 * @author Stephane Maldini
 * @since 1.1
 */
public class RecoverAction<T, E extends Throwable> extends Action<T, E> {

	private final ClassSelector selector;
	private       E             lastError;

	public RecoverAction(Dispatcher dispatcher, ClassSelector selector) {
		super(dispatcher);
		this.selector = selector;
	}

	@Override
	protected StreamSubscription<E> createSubscription(final Subscriber<E> subscriber) {
		if (lastError != null) {
			return new StreamSubscription<E>(this, subscriber) {
				@Override
				public void request(int elements) {
					subscriber.onNext(lastError);
				}
			};
		} else {
			return super.createSubscription(subscriber);
		}
	}

	@Override
	public void onNext(T ev) {
		//IGNORE
	}

	@Override
	@SuppressWarnings("unchecked")
	protected void doError(Throwable cause) {
		if (selector.matches(cause.getClass())) {
			lastError = (E) cause;
			broadcastNext((E) cause);
		} else {
			super.doError(cause);
		}
	}

	@Override
	public String toString() {
		return super.toString() + "{" +
				"catch-type=" + selector.getObject() + ", " +
				'}';
	}
}
