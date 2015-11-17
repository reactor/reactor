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
package reactor.rx.action;

import org.reactivestreams.Processor;
import org.reactivestreams.Publisher;
import org.reactivestreams.Subscriber;
import org.reactivestreams.Subscription;
import reactor.core.support.Bounded;
import reactor.core.support.Publishable;
import reactor.core.support.Subscribable;
import reactor.rx.Stream;
import reactor.rx.StreamUtils;

/**
 * Create a Processor decorated with Stream API
 *
 * @author Stephane Maldini
 * @since 2.0, 2.1
 */
final public class ProcessorAction<E, O> extends Stream<O> implements Processor<E, O>, Publishable<O>, Subscribable<E> {

	private final Subscriber<E> receiver;
	private final Publisher<O> publisher;

	/**
	 *
	 * @param receiver
	 * @param publisher
	 * @param <E>
	 * @param <O>
	 * @return
	 */
	public static <E, O> ProcessorAction<E, O> create(Subscriber<E> receiver, Publisher<O> publisher){
		return new ProcessorAction<>(receiver, publisher);
	}

	ProcessorAction(Subscriber<E> receiver, Publisher<O> publisher) {
		this.receiver = receiver;
		this.publisher = publisher;
	}

	@Override
	public Subscriber<E> downstream() {
		return receiver;
	}

	@Override
	public Publisher<O> upstream() {
		return publisher;
	}

	@Override
	public void subscribe(Subscriber<? super O> s) {
		publisher.subscribe(s);
	}

	@Override
	public void onSubscribe(Subscription s) {
		receiver.onSubscribe(s);
	}

	@Override
	public void onNext(E e) {
		receiver.onNext(e);
	}

	@Override
	public void onError(Throwable t) {
		receiver.onError(t);
	}

	@Override
	public void onComplete() {
		receiver.onComplete();
	}


	@Override
	public boolean isExposedToOverflow(Bounded parentPublisher) {
		return Bounded.class.isAssignableFrom(publisher.getClass()) && ((Bounded) publisher).isExposedToOverflow(
				parentPublisher);
	}

	@Override
	public long getCapacity() {
		return Bounded.class.isAssignableFrom(publisher.getClass()) ? ((Bounded) publisher).getCapacity() : Long.MAX_VALUE;
	}

	@Override
	public String toString() {
		return "ProcessorAction{" +
				"receiver=" + receiver +
				", publisher=" + publisher +
				'}';
	}
}
