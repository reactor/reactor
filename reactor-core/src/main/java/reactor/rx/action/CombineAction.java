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
final public class CombineAction<E, O, S extends Stream<O>> extends Action<E, O> {
	private final S            publisher;
	private final Action<E, O> subscriber;

	public CombineAction(S publisher, Action<E, O> subscriber) {
		super(subscriber.getDispatcher(), subscriber.getMaxCapacity());
		this.publisher = publisher;
		this.subscriber = subscriber;
	}

	public S output() {
		return publisher;
	}

	public Action<E, O> input() {
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
	public void setKeepAlive(boolean keepAlive) {
		publisher.setKeepAlive(keepAlive);
	}

	@Override
	public StreamUtils.StreamVisitor debug() {
		return publisher.debug();
	}

	@Override
	public Action<E, O> cancel() {
		publisher.cancel();
		return super.cancel();
	}

	@Override
	public String toString() {
		return "input=" + (subscriber.getClass().getSimpleName().isEmpty() ? subscriber : subscriber.getClass().getSimpleName().replaceAll("Action","")) +
				", output=" + (publisher.getClass().getSimpleName().isEmpty() ? publisher : publisher.getClass().getSimpleName().replaceAll("Action",""));
	}
}
