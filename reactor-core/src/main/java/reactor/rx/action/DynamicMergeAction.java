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
import org.reactivestreams.Subscription;
import reactor.event.dispatch.Dispatcher;

import java.util.concurrent.atomic.AtomicLong;

/**
 * @author Stephane Maldini
 * @author Jon Brisbin
 * @since 1.1
 */
public class DynamicMergeAction<I, O, E extends Publisher<O>> extends Action<I, O> {

	private final MergeAction<O> mergeAction;

	public DynamicMergeAction(
	                          Dispatcher dispatcher
	) {
		super(dispatcher);
		this.mergeAction = new MergeAction<O>(dispatcher){
			@Override
			protected void requestUpstream(AtomicLong currentCapacity, boolean terminated, int elements) {
				if(currentCapacity.get() > 0 && DynamicMergeAction.this.getSubscription() != null){
					DynamicMergeAction.this.getSubscription().request(elements);
				}
			}
		};
		this.mergeAction.runningComposables.incrementAndGet();
	}


	@Override
	public void subscribe(Subscriber<O> subscriber) {
		mergeAction.subscribe(subscriber);
	}


	@Override
	@SuppressWarnings("unchecked")
	protected void doNext(I value) {
		mergeAction.addPublisher((E)value);
	}

	@Override
	protected void doSubscribe(Subscription subscription){
		mergeAction.prefetch(batchSize).env(getEnvironment()).onSubscribe(subscription);
	}

	@Override
	protected void doFlush() {
		mergeAction.doFlush();
	}

	@Override
	protected void doComplete() {
		mergeAction.doComplete();
	}

	@Override
	protected void doError(Throwable ev) {
		mergeAction.doError(ev);
	}

	@Override
	public Action<I,O> resume() {
		mergeAction.resume();
		return super.resume();
	}

	@Override
	public Action<I,O> pause() {
		mergeAction.pause();
		return super.pause();
	}

	public MergeAction<O> mergedStream(){
		return mergeAction;
	}
}
