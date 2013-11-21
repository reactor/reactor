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
package reactor.operations;

import reactor.core.Observable;
import reactor.event.Event;
import reactor.event.selector.Selector;
import reactor.function.Consumer;
import reactor.function.Function;
import reactor.function.Supplier;
import reactor.tuple.Tuple;
import reactor.tuple.Tuple2;

/**
 * @author Stephane Maldini
 */
public class ScanOperation<T, A> extends BaseOperation<T> {

	private final    Supplier<A>               accumulators;
	private final    Function<Tuple2<T, A>, A> fn;
	private volatile A                         acc;


	public ScanOperation(Supplier<A> accumulators, Function<Tuple2<T, A>, A> fn,
	                     Observable d, Object successKey, Object failureKey) {
		super(d, successKey, failureKey);
		this.accumulators = accumulators;
		this.fn = fn;
	}

	@Override
	protected void doOperation(Event<T> ev) {
		if (null == acc) {
			acc = (null != accumulators ? accumulators.get() : null);
		}
		acc = fn.apply(Tuple.of(ev.getData(), acc));
		notifyValue(ev.copy(acc));
	}

}
