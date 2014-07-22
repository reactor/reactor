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

import reactor.event.dispatch.Dispatcher;

import java.util.ArrayList;
import java.util.List;

/**
 * @author Stephane Maldini
 * @since 1.1
 */
public class CollectAction<T> extends BatchAction<T, List<T>>{

	private final List<T> values;

	public CollectAction(int batchsize, Dispatcher dispatcher) {
		super(batchsize, dispatcher, true, false, batchsize > 0);
		values = new ArrayList<T>();
	}

	@Override
	public void nextCallback(T value) {
		values.add(value);
	}

	@Override
	public void flushCallback(T ev) {
		if (values.isEmpty()) {
			return;
		}
		broadcastNext(new ArrayList<T>(values));
		values.clear();
	}

	@Override
	public String toString() {
		return super.toString() +"{collected="+values.size()+"}";
	}
}
