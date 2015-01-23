/*
 * Copyright (c) 2011-2015 Pivotal Software Inc, All Rights Reserved.
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
package reactor.rx.broadcast;

import org.reactivestreams.Subscriber;
import reactor.Environment;
import reactor.core.Dispatcher;
import reactor.core.dispatch.SynchronousDispatcher;
import reactor.core.queue.CompletableQueue;
import reactor.rx.action.Action;
import reactor.rx.subscription.PushSubscription;
import reactor.rx.subscription.ReactiveSubscription;

/**
 * A {@code Broadcaster} is a subclass of {@code Stream} which exposes methods for publishing values into the pipeline.
 * It is possible to publish discreet values typed to the generic type of the {@code Stream} as well as error conditions
 * and the Reactive Streams "complete" signal via the {@link #onComplete()} method.
 *
 * @author Stephane Maldini
 */
public class BehaviorBroadcaster<O> extends Action<O, O> {
	private boolean keepAlive = false;

	public BehaviorBroadcaster(Dispatcher dispatcher, long capacity) {
		super(dispatcher, capacity);
	}

	@Override
	protected void doNext(O ev) {
		broadcastNext(ev);
	}


	@Override
	protected void doComplete() {
		if (!keepAlive && downstreamSubscription == null) {
			cancel();
		}
		broadcastComplete();
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
	public BehaviorBroadcaster<O> env(Environment environment) {
		super.env(environment);
		return this;
	}

	@Override
	public BehaviorBroadcaster<O> capacity(long elements) {
		super.capacity(elements);
		return this;
	}

	public BehaviorBroadcaster<O> keepAlive(boolean keepAlive) {
		this.keepAlive = keepAlive;
		return this;
	}

	@Override
	public BehaviorBroadcaster<O> keepAlive() {
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
	public String toString() {
		return super.toString() + "{keepAlive=" + keepAlive +
				"}";
	}
}
