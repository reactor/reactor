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
package reactor.rx.stream;

import org.reactivestreams.Subscriber;
import reactor.Environment;
import reactor.core.Dispatcher;
import reactor.core.dispatch.SynchronousDispatcher;
import reactor.core.queue.CompletableQueue;
import reactor.rx.action.Action;
import reactor.rx.subscription.PushSubscription;
import reactor.rx.subscription.ReactiveSubscription;

/**
 * @author Stephane Maldini
 */
public class Broadcaster<O> extends Action<O, O> {
	private Throwable error;
	private boolean keepAlive = false;

	public Broadcaster(Dispatcher dispatcher, long capacity) {
		super(dispatcher, capacity);
	}

	@Override
	protected void doNext(O ev) {
		broadcastNext(ev);
	}

	@Override
	public void broadcastError(Throwable ev) {
		this.error = ev;
		super.broadcastError(ev);
	}

	@Override
	public void broadcastComplete() {
		super.broadcastComplete();
	}

	@Override
	protected PushSubscription<O> createSubscription(Subscriber<? super O> subscriber, CompletableQueue<O> queue) {
		if (queue != null) {
			return new ReactiveSubscription<O>(this, subscriber, queue) {

				@Override
				protected void onRequest(long elements) {
					if (upstreamSubscription != null) {
						super.onRequest(elements);
						requestUpstream(capacity, buffer.isComplete(), elements);
					}
				}
			};
		} else {
			return super.createSubscription(subscriber, null);
		}
	}

	@Override
	public void subscribe(Subscriber<? super O> subscriber) {
		if (error == null) {
			super.subscribe(subscriber);
		} else {
			subscriber.onError(error);
		}
	}

	@Override
	protected PushSubscription<O> createSubscription(Subscriber<? super O> subscriber, boolean reactivePull) {
		if (reactivePull) {
			return super.createSubscription(subscriber, true);
		} else {
			return super.createSubscription(subscriber,
					dispatcher != SynchronousDispatcher.INSTANCE &&
							(upstreamSubscription != null && !upstreamSubscription.hasPublisher()));
		}
	}

	@Override
	public Broadcaster<O> env(Environment environment) {
		super.env(environment);
		return this;
	}

	@Override
	public Broadcaster<O> capacity(long elements) {
		super.capacity(elements);
		return this;
	}

	public Broadcaster<O> keepAlive(boolean keepAlive) {
		this.keepAlive = keepAlive;
		return this;
	}

	@Override
	public Broadcaster<O> keepAlive() {
		this.keepAlive(true);
		return this;
	}

	@Override
	protected void onShutdown() {
		if (!keepAlive) {
			super.onShutdown();
		}
	}

	@Override
	protected void doComplete() {
		if (keepAlive && downstreamSubscription == null) {
			cancel();
		}
		broadcastComplete();
	}


	public Throwable error() {
		return error;
	}

	@Override
	public String toString() {
		return super.toString() + "{keepAlive=" + keepAlive +
				"}";
	}
}
