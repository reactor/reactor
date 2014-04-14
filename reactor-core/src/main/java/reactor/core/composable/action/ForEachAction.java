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
package reactor.core.composable.action;

import reactor.event.dispatch.Dispatcher;

/**
 * @author Stephane Maldini
 * @since 1.1
 */
public class ForEachAction<T, V> extends Action<T, V> implements Flushable<T> {

	final private Iterable<V> defaultValues;

	public ForEachAction(Dispatcher dispatcher, ActionProcessor<V> actionProcessor) {
		this(null, dispatcher, actionProcessor);
	}


	public ForEachAction(Iterable<V> defaultValues,
	                     Dispatcher dispatcher,
	                     ActionProcessor<V> actionProcessor) {
		super(dispatcher, actionProcessor);
		this.defaultValues = defaultValues;
	}

	@Override
	@SuppressWarnings("unchecked")
	public void doNext(Object value) {
		if (null == value) {
			return;
		}

		if(null != defaultValues || !Iterable.class.isAssignableFrom(value.getClass())){
			fromIterable(output, defaultValues);
		}else{
			fromIterable(output, (Iterable<V>)value);
		}
		output.flush();
		available();
	}

	@Override
	public Flushable<T> flush() {
		doNext(defaultValues);
		return this;
	}

	public static <T> void fromIterable(ActionProcessor<T> actionProcessor, Iterable<T> values) {
		for (T it : values) {
			actionProcessor.onNext(it);
		}
	}

}
