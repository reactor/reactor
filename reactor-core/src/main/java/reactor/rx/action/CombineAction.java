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
import org.reactivestreams.Subscription;
import reactor.core.Environment;
import reactor.event.dispatch.Dispatcher;
import reactor.rx.Stream;
import reactor.rx.StreamUtils;

/**
 * Create a Processor where a given head/tail couple is provided as a Stream (Input upstream) and Action (Output
 * downstream).
 *
 * @author Stephane Maldini
 * @since 2.0
 */
final public class CombineAction<E, O> extends Action<E, O> {
	private final Stream<O>    publisher;
	private final Action<E, ?> subscriber;

	public CombineAction(Action<E, ?> head, Stream<O> tail) {
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
	public void broadcastNext(O ev) {
		publisher.broadcastNext(ev);
	}

	@Override
	public void broadcastError(Throwable throwable) {
		publisher.broadcastError(throwable);
	}

	@Override
	public void broadcastComplete() {
		publisher.broadcastComplete();
	}

	@Override
	public State getState() {
		return publisher.getState();
	}

	@Override
	public Throwable getError() {
		return publisher.getError();
	}

	@Override
	public Dispatcher getDispatcher() {
		return publisher.getDispatcher();
	}

	@Override
	public Environment getEnvironment() {
		return publisher.getEnvironment();
	}

	@Override
	public StreamUtils.StreamVisitor debug() {
		return publisher.debug();
	}

	@Override
	public CombineAction<E, O> keepAlive(boolean keepAlive) {
		publisher.keepAlive(keepAlive);
		subscriber.keepAlive(keepAlive);
		super.keepAlive(keepAlive);
		return this;
	}

	@Override
	public Action<E, O> capacity(long elements) {
		publisher.capacity(elements);
		subscriber.capacity(elements);
		return super.capacity(elements);
	}

	@Override
	public Action<E, O> ignoreErrors(boolean ignore) {
		publisher.ignoreErrors(ignore);
		subscriber.ignoreErrors(ignore);
		return super.ignoreErrors(ignore);
	}

	@Override
	public Action<E, O> env(Environment environment) {
		publisher.env(environment);
		subscriber.env(environment);
		return super.env(environment);
	}

	@Override
	public Action<E, O> pause() {
		publisher.pause();
		subscriber.pause();
		return super.pause();
	}

	@Override
	public Action<E, O> resume() {
		publisher.resume();
		subscriber.resume();
		return super.resume();
	}

	@Override
	public Action<E, O> cancel() {
		publisher.cancel();
		subscriber.cancel();
		return super.cancel();
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
