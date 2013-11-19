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

import reactor.core.composable.Deferred;
import reactor.core.composable.Stream;
import reactor.event.Event;
import reactor.function.Consumer;
import reactor.function.Function;
import reactor.function.Supplier;
import reactor.tuple.Tuple;
import reactor.tuple.Tuple2;

import java.util.concurrent.atomic.AtomicLong;

/**
* @author Stephane Maldini
*/
public class ReduceOperation<T, A> implements Consumer<Event<T>> {
	private       Stream                    stream;
	private final AtomicLong                count;
	private final Supplier<A>               accumulators;
	private final Function<Tuple2<T, A>, A> fn;
	private final Deferred<A, Stream<A>>    d;
	private       A                         acc;

	public ReduceOperation(Stream stream, Supplier<A> accumulators, Function<Tuple2<T, A>, A> fn, Deferred<A, Stream<A>> d) {
		this.stream = stream;
		this.accumulators = accumulators;
		this.fn = fn;
		this.d = d;
		count = new AtomicLong(0);
	}

	@Override
	public void accept(Event<T> value) {
		if (null == acc) {
			acc = (null != accumulators ? accumulators.get() : null);
		}
		acc = fn.apply(Tuple.of(value.getData(), acc));

		if (stream.isBatch() && count.incrementAndGet() % stream.batchSize == 0) {
			d.acceptEvent(value.copy(acc));
		} else if (!stream.isBatch()) {
			d.acceptEvent(value.copy(acc));
		}
	}
}
