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
import reactor.rx.Stream;
import reactor.rx.StreamSubscription;

/**
 * @author Stephane Maldini
 * @since 2.0
 */
public class NestAction<T, E extends Stream<T>> extends Action<T, E> {

	private final E stream;

	public NestAction(Dispatcher dispatcher, E stream) {
		super(dispatcher);
		this.stream = stream;
	}

	@Override
	protected StreamSubscription<E> createSubscription(Subscriber<E> subscriber) {
		StreamSubscription<E> streamSubscription = super.createSubscription(subscriber);
		streamSubscription.onNext(stream);
		return  streamSubscription;
	}
}
