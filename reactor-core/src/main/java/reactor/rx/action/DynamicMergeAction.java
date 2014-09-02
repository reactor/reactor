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

/**
 * @author Stephane Maldini
 * @since 2.0
 */
public class DynamicMergeAction<I, O, E> extends Action<I, O> {

	private final FanInAction<E, O> fanInAction;

	@SuppressWarnings("unchecked")
	public DynamicMergeAction(
			Dispatcher dispatcher
	){
		this(dispatcher, (FanInAction<E,O>)new MergeAction<O>(dispatcher));
	}

	public DynamicMergeAction(
			Dispatcher dispatcher,
	    FanInAction<E,O> fanInAction
	) {
		super(dispatcher);
		this.fanInAction = fanInAction;
		fanInAction.runningComposables.incrementAndGet();
		fanInAction.masterAction = this;
	}

	@Override
	public void subscribe(Subscriber<? super O> subscriber) {
		fanInAction.subscribe(subscriber);
	}

	@Override
	@SuppressWarnings("unchecked")
	protected void doNext(I value) {
		fanInAction.addPublisher((Publisher<E>) value);
	}

	@Override
	protected void doComplete() {
		super.doComplete();
		if (fanInAction.runningComposables.decrementAndGet() == 0) {
			fanInAction.doComplete();
		}
	}

	@Override
	protected void doError(Throwable ev) {
		super.doError(ev);
		fanInAction.runningComposables.decrementAndGet();
		fanInAction.doError(ev);
	}

	@Override
	public Action<I, O> capacity(long elements) {
		fanInAction.capacity(elements);
		return super.capacity(elements);
	}

	@Override
	public void setKeepAlive(boolean keepAlive) {
		fanInAction.setKeepAlive(keepAlive);
		super.setKeepAlive(keepAlive);
	}

	@Override
	public Action<I, O> env(Environment environment) {
		fanInAction.env(environment);
		return super.env(environment);
	}

	@Override
	public Action<I, O> resume() {
		fanInAction.resume();
		return super.resume();
	}

	@Override
	public Action<I, O> pause() {
		fanInAction.pause();
		return super.pause();
	}


	public FanInAction<E,O> mergedStream() {
		return fanInAction;
	}

}
