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
import reactor.core.Environment;
import reactor.event.dispatch.Dispatcher;

import java.util.concurrent.atomic.AtomicLong;

/**
 * @author Stephane Maldini
 * @since 2.0
 */
public class DynamicMergeAction<I, O, E extends Publisher<O>> extends Action<I, O> {

	private final MergeAction<O> mergeAction;

	public DynamicMergeAction(
			Dispatcher dispatcher
	) {
		super(dispatcher);
		final DynamicMergeAction<I, O, E> thiz = this;
		this.mergeAction = new MergeAction<O>(dispatcher, thiz) {
			@Override
			protected void requestUpstream(AtomicLong capacity, boolean terminated, int elements) {
				if (thiz.state == State.READY) {
					thiz.requestUpstream(capacity, terminated, elements);
					super.requestUpstream(capacity, terminated, elements);
				}
			}
		};
	}

	@Override
	public void subscribe(Subscriber<O> subscriber) {
		mergeAction.subscribe(subscriber);
	}

	@Override
	@SuppressWarnings("unchecked")
	protected void doNext(I value) {
		mergeAction.addPublisher((E) value);
	}

	@Override
	protected void doComplete() {
		super.doComplete();
		if (mergeAction.runningComposables.decrementAndGet() == 0) {
			mergeAction.doComplete();
		}
	}

	@Override
	protected void doError(Throwable ev) {
		super.doError(ev);
		mergeAction.runningComposables.decrementAndGet();
		mergeAction.doError(ev);
	}

	@Override
	public Action<I, O> capacity(int elements) {
		mergeAction.capacity(elements);
		return super.capacity(elements);
	}

	@Override
	public void setKeepAlive(boolean keepAlive) {
		mergeAction.setKeepAlive(keepAlive);
		super.setKeepAlive(keepAlive);
	}

	@Override
	public Action<I, O> env(Environment environment) {
		mergeAction.env(environment);
		return super.env(environment);
	}

	@Override
	public Action<I, O> resume() {
		mergeAction.resume();
		return super.resume();
	}

	@Override
	public Action<I, O> pause() {
		mergeAction.pause();
		return super.pause();
	}


	public MergeAction<O> mergedStream() {
		return mergeAction;
	}

}
