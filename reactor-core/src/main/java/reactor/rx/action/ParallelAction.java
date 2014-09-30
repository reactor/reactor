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
import reactor.rx.subscription.ReactiveSubscription;

/**
 * A stream emitted from the {@link reactor.rx.Stream#parallel()} action that tracks its position in the hosting
 * {@link reactor.rx.action.ConcurrentAction}. It also retains the last time it requested anything for
 * latency monitoring {@link reactor.rx.action.ConcurrentAction#monitorLatency(long)}.
 * The Stream will complete or fail whever the parent parallel action terminates itself or when broadcastXXX is called.
 * <p>
 * Create such stream with the provided factory, E.g.:
 * {@code
 * Streams.parallel(8).consume(parallelStream -> parallelStream.consume())
 * }
 *
 * @author Stephane Maldini
 */
public final class ParallelAction<O> extends Action<O, O> {
	private final ConcurrentAction<O> parallelAction;
	private final int                 index;

	private volatile long lastRequestedTime = -1l;
	private ReactiveSubscription<O> reactiveSubscription;

	public ParallelAction(ConcurrentAction<O> parallelAction, Dispatcher dispatcher, int index) {
		super(dispatcher);
		this.dispatcher = dispatcher;
		this.parallelAction = parallelAction;
		this.index = index;
	}

	public long getCurrentCapacity() {
		if(reactiveSubscription == null){
			return 0;
		}else{
			return reactiveSubscription.getCapacity().get();
		}
	}

	@Override
	protected void doNext(O ev) {
		broadcastNext(ev);
	}

	public long getLastRequestedTime() {
		return lastRequestedTime;
	}

	public int getIndex() {
		return index;
	}

	@Override
	protected ReactiveSubscription<O> createSubscription(Subscriber<? super O> subscriber, boolean reactivePull) {
		reactiveSubscription = new ReactiveSubscription<O>(this, subscriber) {
			@Override
			public void request(long elements) {
				super.request(elements);
				lastRequestedTime = System.currentTimeMillis();
				parallelAction.parallelRequest(elements, index);
			}

			@Override
			public void cancel() {
				super.cancel();
				parallelAction.clean(index);
			}

		};
		return reactiveSubscription;
	}

	@Override
	public String toString() {
		return super.toString() + "{" + (index + 1) + "/" + parallelAction.getPoolSize() + "}";
	}
}
