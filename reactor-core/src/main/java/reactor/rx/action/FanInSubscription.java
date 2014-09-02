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

import com.gs.collections.api.block.function.Function;
import com.gs.collections.api.block.predicate.Predicate;
import com.gs.collections.api.block.procedure.Procedure;
import com.gs.collections.api.list.MutableList;
import com.gs.collections.impl.block.procedure.checked.CheckedProcedure;
import com.gs.collections.impl.list.mutable.MultiReaderFastList;
import org.reactivestreams.Subscriber;
import org.reactivestreams.Subscription;
import reactor.rx.StreamSubscription;

/**
 * @author Stephane Maldini
 * @since 2.0
 */
public class FanInSubscription<O> extends StreamSubscription<O> {
	final MultiReaderFastList<InnerSubscription> subscriptions;

	protected final Function<InnerSubscription, InnerSubscription> cleanFuntion = new CleanFunction();
	protected final Procedure<InnerSubscription> subRemoveProcedure = new SubRemoveProcedure();

	public FanInSubscription(Subscriber<O> subscriber) {
		this(subscriber, MultiReaderFastList.<InnerSubscription>newList(8));
	}

	public FanInSubscription(Subscriber<O> subscriber,
	                         MultiReaderFastList<InnerSubscription> subs) {
		super(null, subscriber);
		this.subscriptions = subs;
	}

	@Override
	public void request(final long elements) {
		super.request(elements);
		parallelRequest(elements);
	}

	protected void parallelRequest(long elements) {
		final int parallel = subscriptions.size();

		if (parallel > 0) {
			final long batchSize = elements / parallel;
			final long remaining = (elements % parallel > 0 ? elements : 0);
			if (batchSize == 0 && elements == 0) return;

			MutableList<InnerSubscription> toRemove = subscriptions.collectIf(new Predicate<InnerSubscription>() {
				@Override
				public boolean accept(InnerSubscription subscription) {
					subscription.request(batchSize + remaining);
					return subscription.toRemove;
				}
			}, cleanFuntion);

			pruneObsoleteSubs(toRemove);

		}
	}

	protected void pruneObsoleteSubs(final MutableList<InnerSubscription> toRemove){
		if(toRemove != null && !toRemove.isEmpty()){
			subscriptions.withWriteLockAndDelegate(new Procedure<MutableList<InnerSubscription>>() {
				@Override
				public void value(MutableList<InnerSubscription> each) {
					toRemove.forEach(subRemoveProcedure);
				}
			});
		}
	}

	@Override
	public void cancel() {
		subscriptions.forEach(new CheckedProcedure<Subscription>() {
			@Override
			public void safeValue(Subscription subscription) throws Exception {
				subscription.cancel();
			}
		});
		subscriptions.clear();
		super.cancel();
	}

	void addSubscription(final InnerSubscription s) {
		subscriptions.
				withWriteLockAndDelegate(new CheckedProcedure<MutableList<FanInSubscription.InnerSubscription>>() {
					@Override
					public void safeValue(MutableList<FanInSubscription.InnerSubscription> streamSubscriptions)
							throws Exception {

						streamSubscriptions.collectIf(new Predicate<InnerSubscription>() {
							@Override
							public boolean accept(InnerSubscription subscription) {
								return subscription.toRemove;
							}
						}, cleanFuntion).forEach(subRemoveProcedure);

						streamSubscriptions.add(s);
					}
				});

	}


	public static class InnerSubscription implements Subscription {

		final Subscription wrapped;
		boolean toRemove = false;

		public InnerSubscription(Subscription wrapped) {
			this.wrapped = wrapped;
		}

		@Override
		public void request(long n) {
			wrapped.request(n);
		}

		@Override
		public void cancel() {
			wrapped.cancel();
		}

		public Subscription getDelegate() {
			return wrapped;
		}
	}

	private static class CleanFunction implements Function<InnerSubscription, InnerSubscription> {
		@Override
		public InnerSubscription valueOf(InnerSubscription innerSubscription) {
			return innerSubscription;
		}
	}

	private class SubRemoveProcedure implements Procedure<InnerSubscription> {
		@Override
		public void value(InnerSubscription innerSubscription) {
			subscriptions.remove(innerSubscription);
		}
	}
}
