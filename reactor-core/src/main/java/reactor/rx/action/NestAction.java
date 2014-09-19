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

import org.reactivestreams.Publisher;
import org.reactivestreams.Subscriber;
import reactor.event.dispatch.Dispatcher;
import reactor.function.Consumer;
import reactor.rx.Stream;
import reactor.rx.StreamSubscription;
import reactor.tuple.Tuple2;

/**
 * @author Stephane Maldini
 * @since 2.0
 */
public class NestAction<T, E extends Stream<T>, K> extends Action<T, E> {

	private final E                      stream;
	private final Publisher<K>           controlStream;
	private final Consumer<Tuple2<E, K>> controller;

	public NestAction(Dispatcher dispatcher, E stream) {
		this(dispatcher, stream, null, null);
	}

	public NestAction(Dispatcher dispatcher, E stream, Publisher<K> controlStream, Consumer<Tuple2<E, K>> consumer) {
		super(dispatcher);
		this.stream = stream;
		this.controller = consumer;
		this.controlStream = controlStream;
	}

	@Override
	protected StreamSubscription<E> createSubscription(Subscriber<? super E> subscriber) {
		StreamSubscription<E> streamSubscription = super.createSubscription(subscriber);
		if (controller == null) {
			streamSubscription.onNext(stream);
			streamSubscription.onComplete();
		}
		return streamSubscription;
	}

	@Override
	protected void doNext(Object ev) {
		//ignore
	}
}
