/*
 * Copyright (c) 2011-2016 Pivotal Software Inc, All Rights Reserved.
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
import reactor.Publishers;
import reactor.core.subscriber.SubscriberBarrier;
import reactor.fn.BiFunction;

/**
 * @author Stephane Maldini
 * @since 1.1, 2.0, 2.1
 */
public final class ScanOperator<T, A> implements Publishers.Operator<T, A> {

	private final BiFunction<A, ? super T, A> fn;
	private final A                           initialValue;

	public ScanOperator(BiFunction<A, ? super T, A> fn, A initialValue) {
		this.fn = fn;
		this.initialValue = initialValue;
	}

	@Override
	public Subscriber<? super T> apply(Subscriber<? super A> subscriber) {
		return new ScanAction<>(subscriber, initialValue, fn);
	}

	static final class ScanAction<T, A> extends SubscriberBarrier<T, A> {

		private final BiFunction<A, ? super T, A> fn;
		private final A                           initialValue;
		private       A                           acc;
		private boolean initialized = false;

		private static final Object NOVALUE_SENTINEL = new Object();

		@SuppressWarnings("unchecked")
		public ScanAction(Subscriber<? super A> actual, A initial, BiFunction<A, ? super T, A> fn) {
			super(actual);
			this.initialValue = initial == null ? (A) NOVALUE_SENTINEL : initial;
			this.acc = initialValue;
			this.fn = fn;
		}

	/*final AtomicBoolean once      = new AtomicBoolean();
	final AtomicBoolean excessive = new AtomicBoolean();

	@Override
	public void requestMore(long n) {
		if (once.compareAndSet(false, true)) {
			if (acc == NOVALUE_SENTINEL || n == Long.MAX_VALUE) {
				super.requestMore(n);
			} else if (n == 1) {
				excessive.set(true);
				super.requestMore(1);
			} else {
				super.requestMore(n - 1);
			}
		} else {
			if (excessive.compareAndSet(true, false) && n != Long.MAX_VALUE) {
				super.requestMore(n - 1);
			} else {
				super.requestMore(n);
			}
		}
	}*/

		@Override
		@SuppressWarnings("unchecked")
		protected void doNext(T ev) {
			checkInit();
			if (this.acc == NOVALUE_SENTINEL) {
				this.acc = (A) ev;
			}
			else {
				this.acc = fn.apply(acc, ev);
			}

			subscriber.onNext(acc);
		}

		@Override
		protected void doComplete() {
			checkInit();
			subscriber.onComplete();
		}

		private void checkInit() {
			if (!initialized) {
				initialized = true;
				if (initialValue != NOVALUE_SENTINEL) {
					subscriber.onNext(initialValue);
				}
			}
		}

	}


}
