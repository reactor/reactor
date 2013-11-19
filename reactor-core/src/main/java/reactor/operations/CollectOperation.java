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

import java.util.ArrayList;
import java.util.List;

/**
* @author Stephane Maldini
*/
public class CollectOperation<T> implements Consumer<Event<T>> {
	private       Stream                             stream;
	private final List<T>                            values;
	private final Deferred<List<T>, Stream<List<T>>> d;

	public CollectOperation(Stream stream, List<T> values, Deferred<List<T>, Stream<List<T>>> d) {
		this.stream = stream;
		this.values = values;
		this.d = d;
	}

	@Override
	public void accept(Event<T> value) {
		synchronized (values) {
			values.add(value.getData());
			if (values.size() % stream.batchSize != 0) {
				return;
			}
			d.acceptEvent(value.copy((List<T>) new ArrayList<T>(values)));
			values.clear();
		}
	}
}
