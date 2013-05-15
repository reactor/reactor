/*
 * Copyright (c) 2011-2013 the original author or authors.
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

package reactor.fn.dispatch;

import reactor.convert.Converter;
import reactor.fn.Consumer;
import reactor.fn.Event;
import reactor.fn.Registry;

/**
 * Abstract class that a {@link Dispatcher} will implement that provides a caller with a holder for the components of an
 * event dispatch. The {@link #submit()} method is called to actually place the task on the internal queue to be
 * executed.
 *
 * @author Jon Brisbin
 */
public abstract class Task<T> {

	private Object                                 key;
	private Registry<Consumer<? extends Event<?>>> consumerRegistry;
	private Event<T>                               event;
	private Converter                              converter;
	private Consumer<Event<T>>                     completionConsumer;
	private Consumer<Throwable>                    errorConsumer;

	public Object getKey() {
		return key;
	}

	public Task<T> setKey(Object key) {
		this.key = key;
		return this;
	}

	public Registry<Consumer<? extends Event<?>>> getConsumerRegistry() {
		return consumerRegistry;
	}

	public Task<T> setConsumerRegistry(Registry<Consumer<? extends Event<?>>> consumerRegistry) {
		this.consumerRegistry = consumerRegistry;
		return this;
	}

	public Event<T> getEvent() {
		return event;
	}

	public Task<T> setEvent(Event<T> event) {
		this.event = event;
		return this;
	}

	public Converter getConverter() {
		return converter;
	}

	public Task<T> setConverter(Converter converter) {
		this.converter = converter;
		return this;
	}

	public Consumer<Event<T>> getCompletionConsumer() {
		return completionConsumer;
	}

	public Task<T> setCompletionConsumer(Consumer<Event<T>> completionConsumer) {
		this.completionConsumer = completionConsumer;
		return this;
	}

	public Consumer<Throwable> getErrorConsumer() {
		return errorConsumer;
	}

	public Task<T> setErrorConsumer(Consumer<Throwable> errorConsumer) {
		this.errorConsumer = errorConsumer;
		return this;
	}

	public void reset() {
		key = null;
		consumerRegistry = null;
		event = null;
		completionConsumer = null;
		errorConsumer = null;
	}

	/**
	 * Submit this task for execution. Implementations might block on this method if there is no room to queue this task
	 * for execution.
	 */
	public abstract void submit();

}
