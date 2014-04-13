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
package reactor.core.composable.spec;

import org.reactivestreams.api.Producer;
import org.reactivestreams.spi.Publisher;
import org.reactivestreams.spi.Subscriber;
import reactor.core.Environment;
import reactor.core.Observable;

import reactor.core.composable.Composable;
import reactor.core.composable.Stream;
import reactor.event.Event;
import reactor.event.dispatch.Dispatcher;
import reactor.event.selector.Selector;
import reactor.function.Consumer;
import reactor.function.Supplier;
import reactor.tuple.Tuple2;

/**
 * A helper class for specifying a bounded {@link Stream}. {@link #each} must be called to
 * provide the stream with its values.
 *
 * @param <T> The type of values that the stream contains.
 * @author Jon Brisbin
 * @author Stephane Maldini
 */
public final class StreamSpec<T> extends ComposableSpec<StreamSpec<T>, Stream<T>> {

	private int batchSize = -1;
	private Iterable<T>  values;
	private Supplier<T>  valuesSupplier;
	private Publisher<T> source;

	/**
	 * Configures the stream to have the given {@code batchSize}. A value of {@code -1}, which
	 * is the default configuration, configures the stream to not be batched.
	 *
	 * @param batchSize The batch size of the stream
	 * @return {@code this}
	 */
	public StreamSpec<T> batchSize(int batchSize) {
		this.batchSize = batchSize;
		return this;
	}

	/**
	 * Configures the stream to tap data from the given {@param producer}.
	 *
	 * @param producer The {@link Producer<T>}
	 * @return {@code this}
	 */
	public StreamSpec<T> source(Producer<T> producer) {
		return source(producer.getPublisher());
	}

	/**
	 * Configures the stream to tap data from the given {@param publisher}.
	 *
	 * @param publisher The {@link Publisher<T>}
	 * @return {@code this}
	 */
	public StreamSpec<T> source(Publisher<T> publisher) {
		this.source = publisher;
		return this;
	}
	/**
	 *
	 * Configures the stream to tap data from the given {@param observable} and {@param selector}.
	 *
	 * @param observable The {@link Observable} to consume events from
	 * @param selector The {@link Selector} to listen to
	 * @return {@code this}
	 */
	public StreamSpec<T> source(final Observable observable, final Selector selector) {
		this.source = publisherFrom(observable, selector);
		return this;
	}

	static <T> Publisher<T> publisherFrom(final Observable observable, final Selector selector){
		return new Publisher<T>(){
			@Override
			public void subscribe(final Subscriber<T> subscriber) {
				observable.on(selector, new Consumer<Event<T>>() {
					@Override
					public void accept(Event<T> event) {
						subscriber.onNext(event.getData());
					}
				});
			}
		};
	}

	/**
	 * Configures the stream to contain the given {@code values}.
	 *
	 * @param values The stream's values
	 * @return {@code this}
	 */
	public StreamSpec<T> each(Iterable<T> values) {
		this.values = values;
		return this;
	}


	/**
	 * Configures the stream to pass value from a {@link Supplier} on flush.
	 *
	 * @param supplier The stream's value generator
	 * @return {@code this}
	 */
	public StreamSpec<T> generate(Supplier<T> supplier) {
		this.valuesSupplier = supplier;
		return this;
	}

	@Override
	protected Stream<T> createComposable(Environment env, Dispatcher dispatcher) {

		if (source == null && values == null && valuesSupplier == null) {
			throw new IllegalStateException("A bounded stream must be configured with some values source. Use " +
					DeferredStreamSpec.class.getSimpleName() + " to create a stream with no initial values or supplier");
		}

		Stream<T> stream = new Stream<T>(dispatcher, batchSize, null, env);
		if (source != null) {
			stream.getPublisher().source(source);
			return stream;
		}
		else if(values == null){
			return stream.propagate(valuesSupplier);
		}else{
			return stream.propagate(values);
		}
	}
}
