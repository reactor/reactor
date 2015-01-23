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
package reactor.rx.action.combination;

import org.reactivestreams.Subscriber;
import org.reactivestreams.Subscription;
import reactor.Environment;
import reactor.rx.Stream;
import reactor.rx.StreamUtils;
import reactor.rx.action.Action;
import reactor.rx.broadcast.Broadcaster;

/**
 * Create a Processor where a given head/tail couple is provided as a Stream (Input upstream) and Action (Output
 * downstream).
 *
 * @author Stephane Maldini
 * @since 2.0
 */
final public class CombineAction<E, O> extends Action<E, O> {
	private final Action<?, O> publisher;
	private final Action<E, ?> subscriber;

	public CombineAction(Action<E, ?> head, Action<?, O> tail) {
		super(head.getDispatcher(), head.getCapacity());
		this.publisher = tail;
		this.subscriber = head;
	}

	public Stream<O> output() {
		return publisher;
	}

	public Action<E, ?> input() {
		return subscriber;
	}


	@Override
	public void subscribe(Subscriber<? super O> s) {
		publisher.subscribe(s);
	}

	@Override
	public void onSubscribe(Subscription s) {
		subscriber.onSubscribe(s);
	}

	@Override
	public void onNext(E e) {
		subscriber.onNext(e);
	}

	@Override
	public void onError(Throwable t) {
		subscriber.onError(t);
	}

	@Override
	public void onComplete() {
		subscriber.onComplete();
	}

	@Override
	public StreamUtils.StreamVisitor debug() {
		return publisher.debug();
	}

	@Override
	public Action<E, O> capacity(long elements) {
		publisher.capacity(elements);
		subscriber.capacity(elements);
		return super.capacity(elements);
	}

	@Override
	public Stream<O> env(Environment environment) {
		if( Broadcaster.class.isAssignableFrom(publisher.getClass())){
			publisher.env(environment);
		}
		return super.env(environment);
	}

	@Override
	public void cancel() {
		publisher.cancel();
		subscriber.cancel();
		super.cancel();
	}

	@Override
	public String toString() {
		return "input=" + (subscriber.getClass().getSimpleName().isEmpty() ? subscriber : subscriber.getClass()
				.getSimpleName().replaceAll("Action", "")) +
				", output=" + (publisher.getClass().getSimpleName().isEmpty() ? publisher : publisher.getClass().getSimpleName
				().replaceAll("Action", ""));
	}

	@Override
	protected void doNext(E ev) {
		//ignore
	}
}
