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

import org.reactivestreams.spi.Subscription;
import reactor.event.dispatch.Dispatcher;
import reactor.function.Function;
import reactor.rx.Stream;
import reactor.util.Assert;

import java.util.concurrent.atomic.AtomicLong;

/**
 * @author Stephane Maldini
 * @author Jon Brisbin
 * @since 1.1
 */
public class MapManyAction<I, O, E extends Pipeline<O>> extends Action<I, O> {

	private final Function<I, E> fn;
	private final MergeAction<O> mergeAction;

	public MapManyAction(Function<I, E> fn,
	                     Dispatcher dispatcher
	) {
		super(dispatcher);
		Assert.notNull(fn, "FlatMap function cannot be null.");
		this.fn = fn;
		this.mergeAction = new MergeAction<O>(dispatcher){
			@Override
			protected void requestUpstream(AtomicLong currentCapacity, boolean terminated, int elements) {
				if(currentCapacity.get() > 0){
					MapManyAction.this.requestUpstream(currentCapacity, terminated, elements);
				}
			}
		};
		this.mergeAction.runningComposables.incrementAndGet();
	}

	@Override
	protected void doNext(I value) {
		mergeAction.runningComposables.incrementAndGet();
		E val = fn.apply(value);
		Action<O, Void> inlineMerge = new Action<O, Void>(getDispatcher(),1) {
			@Override
			protected void doFlush() {
				mergeAction.doFlush();
			}

			@Override
			protected void doComplete() {
				mergeAction.doComplete();
			}

			@Override
			protected void doNext(O ev) {
				mergeAction.doNext(ev);
			}

			@Override
			protected void doError(Throwable ev) {
				mergeAction.doError(ev);
			}
		};
		val.subscribe(inlineMerge);
		inlineMerge.prefetch(batchSize).available();
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
	public Stream<O> resume() {
		mergeAction.resume();
		return super.resume();
	}

	@Override
	public Stream<O> pause() {
		mergeAction.pause();
		return super.pause();
	}

	public MergeAction<O> mergedStream(){
		return mergeAction;
	}
}
