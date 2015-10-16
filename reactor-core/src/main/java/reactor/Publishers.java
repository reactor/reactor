/*
 * Copyright (c) 2011-2015 Pivotal Software Inc, All Rights Reserved.
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

package reactor;

import java.util.Queue;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ConcurrentLinkedQueue;

import org.reactivestreams.Publisher;
import org.reactivestreams.Subscriber;
import reactor.core.error.Exceptions;
import reactor.core.processor.BaseProcessor;
import reactor.core.publisher.FlatMapOperator;
import reactor.core.publisher.IgnoreOnNextOperator;
import reactor.core.publisher.IteratorSequencer;
import reactor.core.publisher.LogOperator;
import reactor.core.publisher.PublisherFactory;
import reactor.core.publisher.TrampolineOperator;
import reactor.core.publisher.ValuePublisher;
import reactor.core.publisher.convert.CompositionDependencyUtils;
import reactor.core.subscriber.BlockingQueueSubscriber;
import reactor.core.subscriber.Tap;
import reactor.core.support.SignalType;
import reactor.fn.BiConsumer;
import reactor.fn.Function;
import reactor.fn.Supplier;

/**
 * @author Stephane Maldini
 * @since 2.1
 */
public final class Publishers extends PublisherFactory {

	/**
	 * @param data
	 * @param <IN>
	 * @return
	 */
	public static <IN> Publisher<IN> just(final IN data) {
		return new ValuePublisher<>(data);
	}

	/**
	 *
	 * @param defaultValues
	 * @param <T>
	 * @return
	 */
	public static <T> Publisher<T> from(final Iterable<T> defaultValues) {
		IteratorSequencer<T> iteratorPublisher = new IteratorSequencer<>(defaultValues);
		return create(iteratorPublisher, iteratorPublisher);
	}

	/**
	 * @param error
	 * @param <IN>
	 * @return
	 */
	public static <IN> Publisher<IN> error(final Throwable error) {
		return Exceptions.publisher(error);
	}

	/**
	 * @param <IN>
	 * @return
	 */
	@SuppressWarnings("unchecked")
	public static <IN> Publisher<IN> empty() {
		return (EmptyPublisher<IN>) EMPTY;
	}

	/**
	 *
	 * @param source
	 * @param <IN>
	 * @return
	 */
	@SuppressWarnings("unchecked")
	public static <IN> Publisher<IN> convert(Object source) {

		if(Publisher.class.isAssignableFrom(source.getClass())){
			return (Publisher<IN>)source;
		}
		else if(Iterable.class.isAssignableFrom(source.getClass())){
			return from((Iterable<IN>)source);
		}
		else {
			return (Publisher<IN>)CompositionDependencyUtils.convertToPublisher(source);
		}
	}

	/**
	 *
	 * @param source
	 * @param to
	 * @param <T>
	 * @return
	 */
	@SuppressWarnings("unchecked")
	public static <T> T convert(Publisher<?> source, Class<T> to) {
		if(Publisher.class.isAssignableFrom(to.getClass())){
			return (T)source;
		}
		else {
			return CompositionDependencyUtils.convertFromPublisher(source, to);
		}
	}



	/**
	 * @param <IN>
	 * @return
	 */
	@SuppressWarnings("unchecked")
	public static <IN> Publisher<IN> never() {
		return (NeverPublisher<IN>) NEVER;
	}

	/**
	 * Intercept a source {@link Publisher} onNext signal to eventually transform, forward
	 * or filter the data by calling or not the right operand {@link Subscriber}.
	 * @param transformer A {@link Function} that transforms each emitting sequence item
	 * @param <I> The source type of the data sequence
	 * @param <O> The target type of the data sequence
	 * @return a fresh Reactive Streams publisher ready to be subscribed
	 */
	public static <I, O> Publisher<O> map(Publisher<I> source,
			final Function<? super I, ? extends O> transformer) {
		return lift(source, new MapOperator<>(transformer), null, null);
	}

	/**
	 * @param <I> The source type of the data sequence
	 * @return a fresh Reactive Streams publisher ready to be subscribed
	 */
	@SuppressWarnings("unchecked")
	public static <I> Publisher<I> merge(
			Publisher<? extends Publisher<? extends I>> source) {
		return flatMap(source, (PublisherToPublisherFunction<I>) P2P_FUNCTION);
	}

	/**
	 * @param transformer A {@link Function} that transforms each emitting sequence item
	 * @param <I> The source type of the data sequence
	 * @param <O> The target type of the data sequence
	 * @return a fresh Reactive Streams publisher ready to be subscribed
	 */
	@SuppressWarnings("unchecked")
	public static <I, O> Publisher<O> flatMap(Publisher<I> source,
			final Function<? super I, ? extends Publisher<? extends O>> transformer) {
		if (source instanceof Supplier) {
			try {
				I v = ((Supplier<I>) source).get();
				if (v != null) {
					return (Publisher<O>) transformer.apply(v);
				}
			}
			catch (Throwable e) {
				return error(e);
			}
		}
		return lift(source,
				new FlatMapOperator(transformer, BaseProcessor.SMALL_BUFFER_SIZE, 32));
	}

	/**
	 * @param <I> The source type of the data sequence
	 * @return a fresh Reactive Streams publisher ready to be subscribed
	 */
	@SuppressWarnings("unchecked")
	public static <I> Publisher<I> concat(
			Publisher<? extends Publisher<? extends I>> source) {
		return concatMap(source, (PublisherToPublisherFunction<I>) P2P_FUNCTION);
	}

	/**
	 * @param transformer A {@link Function} that transforms each emitting sequence item
	 * @param <I> The source type of the data sequence
	 * @param <O> The target type of the data sequence
	 * @return a fresh Reactive Streams publisher ready to be subscribed
	 */
	@SuppressWarnings("unchecked")
	public static <I, O> Publisher<O> concatMap(Publisher<I> source,
			final Function<? super I, Publisher<? extends O>> transformer) {
		if (source instanceof Supplier) {
			try {
				I v = ((Supplier<I>) source).get();
				if (v != null) {
					return (Publisher<O>) transformer.apply(v);
				}
			}
			catch (Throwable e) {
				return error(e);
			}
		}
		return lift(source, new FlatMapOperator(transformer, 1, 32));
	}

	/**
	 * Ignore sequence data (onNext) but bridge all other events:
	 * - downstream: onSubscribe, onComplete, onError
	 * - upstream: request, cancel
	 *
	 * This useful to acknowledge the completion of a data sequence and trigger further
	 *  processing using for instance {@link #concat(Publisher)}.
	 *
	 * @param source the emitted sequence to filter
	 *
	 * @return a new filtered {@link Publisher<Void>}
	 */
	@SuppressWarnings("unchecked")
	public static Publisher<Void> completable(Publisher<?> source) {
		return lift(source, IgnoreOnNextOperator.INSTANCE);
	}

	/**
	 * @param <IN>
	 * @return
	 */
	public static <IN> BlockingQueue<IN> toReadQueue(Publisher<IN> source) {
		return toReadQueue(source, BaseProcessor.SMALL_BUFFER_SIZE);
	}

	/**
	 * @param <IN>
	 * @return
	 */
	public static <IN> BlockingQueue<IN> toReadQueue(Publisher<IN> source, int size) {
		return toReadQueue(source, size, false);
	}

	/**
	 * @param <IN>
	 * @return
	 */
	public static <IN> BlockingQueue<IN> toReadQueue(Publisher<IN> source, int size,
			boolean cancelAfterFirstRequestComplete) {
		return toReadQueue(source, size, cancelAfterFirstRequestComplete,
				size == Integer.MAX_VALUE ? new ConcurrentLinkedQueue<IN>() :
						new ArrayBlockingQueue<IN>(size));
	}

	/**
	 * @param <IN>
	 * @return
	 */
	public static <IN> BlockingQueue<IN> toReadQueue(Publisher<IN> source, int size,
			boolean cancelAfterFirstRequestComplete, Queue<IN> store) {
		return new BlockingQueueSubscriber<>(source, null, store,
				cancelAfterFirstRequestComplete, size);
	}

	/**
	 * @param publisher
	 * @param <IN>
	 * @return
	 */
	public static <IN> Publisher<IN> log(Publisher<IN> publisher) {
		return log(publisher, null, LogOperator.ALL);
	}

	/**
	 * @param publisher
	 * @param category
	 * @param <IN>
	 * @return
	 */
	public static <IN> Publisher<IN> log(Publisher<IN> publisher, String category) {
		return log(publisher, category, LogOperator.ALL);
	}
	/**
	 * @param publisher
	 * @param category
	 * @param options
	 * @param <IN>
	 * @return
	 */
	public static <IN> Publisher<IN> log(Publisher<IN> publisher, String category, int options) {
		return Publishers.lift(publisher, new LogOperator<IN>(category, options));
	}

	/**
	 * @param publisher
	 * @param <IN>
	 * @return
	 */
	@SuppressWarnings("unchecked")
	public static <IN> Publisher<IN> trampoline(Publisher<IN> publisher) {
		return lift(publisher, TrampolineOperator.INSTANCE);
	}

	/**
	 * Monitor the most recent value of this publisher sequence to be returned by {@link
	 * Supplier#get}
	 * @param publisher the sequence to monitor
	 * @param <IN> the sequence type
	 * @return a new {@link Supplier} tapping into publisher (requesting an unbounded
	 * demand of Long.MAX_VALUE)
	 */
	public static <IN> Supplier<IN> tap(Publisher<IN> publisher) {
		Tap<IN> tap = Tap.create();
		publisher.subscribe(tap);
		return tap;
	}

	private static class MapOperator<I, O>
			implements BiConsumer<I, Subscriber<? super O>> {

		private final Function<? super I, ? extends O> transformer;

		public MapOperator(Function<? super I, ? extends O> transformer) {
			this.transformer = transformer;
		}

		@Override
		public void accept(I i, Subscriber<? super O> subscriber) {
			subscriber.onNext(transformer.apply(i));
		}

	}

	private static final NeverPublisher<?> NEVER = new NeverPublisher<>();

	private static class NeverPublisher<IN> implements Publisher<IN> {

		@Override
		public void subscribe(Subscriber<? super IN> s) {
			s.onSubscribe(SignalType.NOOP_SUBSCRIPTION);
		}
	}

	private static final EmptyPublisher<?> EMPTY = new EmptyPublisher<>();

	private static class EmptyPublisher<IN> implements Publisher<IN> {

		@Override
		public void subscribe(Subscriber<? super IN> s) {
			s.onSubscribe(SignalType.NOOP_SUBSCRIPTION);
			s.onComplete();
		}
	}

	private static final PublisherToPublisherFunction<?> P2P_FUNCTION =
			new PublisherToPublisherFunction<>();

	private static class PublisherToPublisherFunction<I>
			implements Function<Publisher<? extends I>, Publisher<? extends I>> {

		@Override
		public Publisher<? extends I> apply(Publisher<? extends I> publisher) {
			return publisher;
		}
	}
}
