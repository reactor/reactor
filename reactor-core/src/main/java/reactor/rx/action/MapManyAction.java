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

import org.reactivestreams.api.Consumer;
import reactor.event.dispatch.Dispatcher;
import reactor.function.Function;
import reactor.rx.Stream;
import reactor.util.Assert;

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
		this.mergeAction = new MergeAction<O>(dispatcher);
	}

	@Override
	protected void doNext(I value) {
		E val = fn.apply(value);
		mergeAction.runningComposables.incrementAndGet();
		Action<O, Void> inlineMerge = new Action<O, Void>(getDispatcher()) {
			@Override
			protected void doFlush() {
				mergeAction.onFlush();
			}

			@Override
			protected void doComplete() {
				mergeAction.onComplete();
			}

			@Override
			protected void doNext(O ev) {
				mergeAction.onNext(ev);
			}

			@Override
			protected void doError(Throwable ev) {
				mergeAction.doError(ev);
			}
		};
		inlineMerge.prefetch(getBatchSize());

		val.produceTo(inlineMerge);
	}

	@Override
	protected void doFlush() {
		mergeAction.onFlush();
		super.doFlush();
	}

	@Override
	protected void doComplete() {
		mergeAction.onComplete();
		super.doComplete();
	}

	@Override
	public Stream<O> resume() {
		mergeAction.resume();
		return super.resume();
	}

	@Override
	public Stream<O> cancel() {
		mergeAction.cancel();
		return super.cancel();
	}

	@Override
	public Stream<O> pause() {
		mergeAction.pause();
		return super.pause();
	}

	@Override
	protected void doError(Throwable ev) {
		super.doError(ev);
	}

	@Override
	public void produceTo(Consumer<O> consumer) {
		mergeAction.produceTo(consumer);
	}
}
