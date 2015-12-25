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

package reactor;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Iterator;
import java.util.List;
import java.util.Queue;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.logging.Level;

import org.reactivestreams.Publisher;
import org.reactivestreams.Subscriber;
import reactor.core.error.Exceptions;
import reactor.core.processor.BaseProcessor;
import reactor.core.publisher.ForEachSequencer;
import reactor.core.publisher.PublisherAmb;
import reactor.core.publisher.PublisherFactory;
import reactor.core.publisher.PublisherJust;
import reactor.core.publisher.PublisherZip;
import reactor.core.publisher.convert.DependencyUtils;
import reactor.core.publisher.PublisherFlatMap;
import reactor.core.publisher.PublisherIgnoreElements;
import reactor.core.publisher.PublisherLog;
import reactor.core.publisher.PublisherMap;
import reactor.core.publisher.PublisherOnErrorResume;
import reactor.core.subscriber.BlockingQueueSubscriber;
import reactor.core.support.SignalType;
import reactor.fn.BiFunction;
import reactor.fn.Function;
import reactor.fn.Supplier;
import reactor.fn.tuple.Tuple;
import reactor.fn.tuple.Tuple2;

/**
 * Create Reactive Streams Publishers from existing data, from custom callbacks (PublisherFactory) or from existing
 * Publishers (lift or combinatory operators).
 * @author Stephane Maldini
 * @since 2.1
 */
public final class Publishers extends PublisherFactory {

	/**
	 *
	 * "Cold" Source Publisher Creation
	 *
	 *
	 *
	 */

	/**
	 * @param data
	 * @param <IN>
	 * @return
	 */
	public static <IN> Publisher<IN> just(final IN data) {
		return new PublisherJust<>(data);
	}

	/**
	 *
	 * @param defaultValues
	 * @param <T>
	 * @return
	 */
	public static <T> Publisher<T> from(final Iterable<? extends T> defaultValues) {
		ForEachSequencer.IterableSequencer<T> iterablePublisher =
				new ForEachSequencer.IterableSequencer<>(defaultValues);
		return create(iterablePublisher, iterablePublisher);
	}

	/**
	 *
	 * @param defaultValues
	 * @param <T>
	 * @return
	 */
	public static <T> Publisher<T> from(final Iterator<? extends T> defaultValues) {
		if (defaultValues == null || !defaultValues.hasNext()) {
			return empty();
		}
		ForEachSequencer.IteratorSequencer<T> iteratorPublisher =
				new ForEachSequencer.IteratorSequencer<>(defaultValues);
		return create(iteratorPublisher, iteratorPublisher);
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
	 * @param <IN>
	 * @return
	 */
	@SuppressWarnings("unchecked")
	public static <IN> Publisher<IN> never() {
		return (NeverPublisher<IN>) NEVER;
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
	 *
	 *  Liftable Operators
	 *
	 *
	 *
	 */

	/**
	 * @param fallbackValue
	 * @param <IN>
	 * @return
	 */
	public static <IN> Publisher<IN> onErrorReturn(final Publisher<IN> source, final IN fallbackValue) {
		return onErrorResumeNext(source, just(fallbackValue));
	}

	/**
	 * @param fallback
	 * @param <IN>
	 * @return
	 */
	public static <IN> Publisher<IN> onErrorResumeNext(final Publisher<IN> source,
			final Publisher<? extends IN> fallback) {
		return lift(source, new PublisherOnErrorResume<>(fallback));
	}

	/**
	 * @param fallbackFunction
	 * @param <IN>
	 * @return
	 */
	public static <IN> Publisher<IN> onErrorResumeNext(final Publisher<IN> source,
			Function<Throwable, ? extends Publisher<? extends IN>> fallbackFunction) {
		return lift(source, new PublisherOnErrorResume<>(fallbackFunction));
	}

	/**
	 * Intercept a source {@link Publisher} onNext signal to eventually transform, forward or filter the data by calling
	 * or not the right operand {@link Subscriber}.
	 * @param transformer A {@link Function} that transforms each emitting sequence item
	 * @param <I> The source type of the data sequence
	 * @param <O> The target type of the data sequence
	 * @return a fresh Reactive Streams publisher ready to be subscribed
	 */
	public static <I, O> Publisher<O> map(Publisher<I> source, final Function<? super I, ? extends O> transformer) {
		return lift(source, new PublisherMap<>(transformer));
	}

	/**
	 * Intercept a source {@link Publisher} onNext signal to eventually timestamp to the right operand {@link
	 * Subscriber}.
	 * @param <I> The source type of the data sequence
	 * @return a fresh Reactive Streams publisher ready to be subscribed
	 */
	@SuppressWarnings("unchecked")
	public static <I> Publisher<Tuple2<Long, I>> timestamp(Publisher<I> source) {
		return lift(source, PublisherMap.<I>timestamp());
	}

	/**
	 * @param publisher
	 * @param <IN>
	 * @return
	 */
	public static <IN> Publisher<IN> log(Publisher<IN> publisher) {
		return log(publisher, null, Level.INFO, PublisherLog.ALL);
	}

	/**
	 * @param publisher
	 * @param category
	 * @param <IN>
	 * @return
	 */
	public static <IN> Publisher<IN> log(Publisher<IN> publisher, String category) {
		return log(publisher, category, Level.INFO, PublisherLog.ALL);
	}

	/**
	 * @param publisher
	 * @param category
	 * @param level
	 * @param <IN>
	 * @return
	 */
	public static <IN> Publisher<IN> log(Publisher<IN> publisher, String category, Level level) {
		return log(publisher, category, level, PublisherLog.ALL);
	}

	/**
	 * @param publisher
	 * @param category
	 * @param level
	 * @param options
	 * @param <IN>
	 * @return
	 */
	public static <IN> Publisher<IN> log(Publisher<IN> publisher, String category, Level level, int options) {
		return lift(publisher, new PublisherLog<IN>(category, level, options));
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
		return lift(source, new PublisherFlatMap(transformer, BaseProcessor.SMALL_BUFFER_SIZE, 32));
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
		return lift(source, new PublisherFlatMap(transformer, 1, 32));
	}

	/**
	 * Ignore sequence data (onNext) but bridge all other events: - downstream: onSubscribe, onComplete, onError -
	 * upstream: request, cancel. The difference with ignoreElements is the generic type becoming Void with after.
	 *
	 * This useful to acknowledge the completion of a data sequence and trigger further processing using for instance
	 * {@link #concat(Publisher)}.
	 * @param source the emitted sequence to filter
	 * @return a new filtered {@link Publisher<Void>}
	 */
	@SuppressWarnings("unchecked")
	public static Publisher<Void> after(Publisher<?> source) {
		return lift(source, PublisherIgnoreElements.INSTANCE);
	}

	/**
	 * Ignore sequence data (onNext) but bridge all other events: - downstream: onSubscribe, onComplete, onError -
	 * upstream: request, cancel.
	 *
	 * This useful to acknowledge the completion of a data sequence and trigger further processing using for instance
	 * {@link #concat(Publisher)}.
	 * @param source the emitted sequence to filter
	 * @return a new filtered {@link Publisher<Void>}
	 */
	@SuppressWarnings("unchecked")
	public static <T> Publisher<T> ignoreElements(Publisher<T> source) {
		return lift(source, PublisherIgnoreElements.INSTANCE);
	}

	/**
	 *
	 *  Combinatory operations
	 *
	 *
	 */

	/**
	 * @param <I> The source type of the data sequence
	 * @return a fresh Reactive Streams publisher ready to be subscribed
	 */
	public static <I> Publisher<I> amb(Publisher<? extends I> source1, Publisher<? extends I> source2) {
		return new PublisherAmb<>(new Publisher[]{source1, source2});
	}

	/**
	 * @param <I> The source type of the data sequence
	 * @return a fresh Reactive Streams publisher ready to be subscribed
	 */
	@SuppressWarnings("unchecked")
	public static <I> Publisher<I> amb(Iterable<? extends Publisher<? extends I>> sources) {
		if (sources == null) {
			return empty();
		}

		Iterator<? extends Publisher<? extends I>> it = sources.iterator();
		if (!it.hasNext()) {
			return empty();
		}

		List<Publisher<? extends I>> list = null;
		Publisher<? extends I> p;
		do {
			p = it.next();
			if (list == null) {
				if (it.hasNext()) {
					list = new ArrayList<>();
				}
				else {
					return (Publisher<I>) p;
				}
			}
			list.add(p);
		}
		while (it.hasNext());

		return new PublisherAmb<>(list.toArray(new Publisher[list.size()]));
	}

	/**
	 * @param <I> The source type of the data sequence
	 * @return a fresh Reactive Streams publisher ready to be subscribed
	 */
	@SuppressWarnings("unchecked")
	public static <I> Publisher<I> concat(Publisher<? extends Publisher<? extends I>> source) {
		return concatMap(source, (PublisherToPublisherFunction<I>) P2P_FUNCTION);
	}

	/**
	 * @param <I> The source type of the data sequence
	 * @return a fresh Reactive Streams publisher ready to be subscribed
	 */
	@SuppressWarnings("unchecked")
	public static <I> Publisher<I> concat(Iterable<? extends Publisher<? extends I>> source) {
		return concatMap(from(source), (PublisherToPublisherFunction<I>) P2P_FUNCTION);
	}

	/**
	 * @param <I> The source type of the data sequence
	 * @return a fresh Reactive Streams publisher ready to be subscribed
	 */
	@SuppressWarnings("unchecked")
	public static <I> Publisher<I> concat(Publisher<? extends I> source1, Publisher<? extends I> source2) {
		return concatMap(from(Arrays.asList(source1, source2)), (PublisherToPublisherFunction<I>) P2P_FUNCTION);
	}

	/**
	 * @param <I> The source type of the data sequence
	 * @return a fresh Reactive Streams publisher ready to be subscribed
	 */
	@SuppressWarnings("unchecked")
	public static <I> Publisher<I> merge(Publisher<? extends Publisher<? extends I>> source) {
		return flatMap(source, (PublisherToPublisherFunction<I>) P2P_FUNCTION);
	}

	/**
	 * @param <I> The source type of the data sequence
	 * @return a fresh Reactive Streams publisher ready to be subscribed
	 */
	@SuppressWarnings("unchecked")
	public static <I> Publisher<I> merge(Iterable<? extends Publisher<? extends I>> source) {
		return flatMap(from(source), (PublisherToPublisherFunction<I>) P2P_FUNCTION);
	}

	/**
	 * @param <I> The source type of the data sequence
	 * @return a fresh Reactive Streams publisher ready to be subscribed
	 */
	@SuppressWarnings("unchecked")
	public static <I> Publisher<I> merge(Publisher<? extends I> source1, Publisher<? extends I> source2) {
		return flatMap(from(Arrays.asList(source1, source2)), (PublisherToPublisherFunction<I>) P2P_FUNCTION);
	}

	/**
	 *
	 * @param source1
	 * @param source2
	 * @param <T1>
	 * @param <T2>
	 * @return
	 */
	@SuppressWarnings("unchecked")
	public static <T1, T2> Publisher<Tuple2<T1, T2>> zip(Publisher<? extends T1> source1,
			Publisher<? extends T2> source2) {

		return new PublisherZip<>(new Publisher[]{source1, source2},
				(Function<Tuple2<T1, T2>, Tuple2<T1, T2>>) IDENTITY_FUNCTION,
				BaseProcessor.XS_BUFFER_SIZE);
	}

	/**
	 *
	 * @param source1
	 * @param source2
	 * @param combinator
	 * @param <O>
	 * @param <T1>
	 * @param <T2>
	 * @return
	 */
	public static <T1, T2, O> Publisher<O> zip(Publisher<? extends T1> source1,
			Publisher<? extends T2> source2,
			final BiFunction<? super T1, ? super T2, ? extends O> combinator) {

		return new PublisherZip<>(new Publisher[]{source1, source2}, new Function<Tuple2<T1, T2>, O>() {
			@Override
			public O apply(Tuple2<T1, T2> tuple) {
				return combinator.apply(tuple.getT1(), tuple.getT2());
			}
		}, BaseProcessor.XS_BUFFER_SIZE);
	}

	/**
	 *
	 * @param sources
	 * @return
	 */
	@SuppressWarnings("unchecked")
	public static Publisher<Tuple> zip(Iterable<? extends Publisher<?>> sources) {
		return zip(sources, IDENTITY_FUNCTION);
	}

	/**
	 *
	 * @param sources
	 * @param combinator
	 * @param <O>
	 * @return
	 */
	public static <O> Publisher<O> zip(Iterable<? extends Publisher<?>> sources,
			final Function<? super Tuple, ? extends O> combinator) {

		if (sources == null) {
			return empty();
		}

		Iterator<? extends Publisher<?>> it = sources.iterator();
		if (!it.hasNext()) {
			return empty();
		}

		List<Publisher<?>> list = null;
		Publisher<?> p;
		do {
			p = it.next();
			if (list == null) {
				if (it.hasNext()) {
					list = new ArrayList<>();
				}
				else {
					return map(p, new Function<Object, O>() {
						@Override
						public O apply(Object o) {
							return combinator.apply(Tuple.of(o));
						}
					});
				}
			}
			list.add(p);
		}
		while (it.hasNext());

		return new PublisherZip<>(list.toArray(new Publisher[list.size()]), combinator, BaseProcessor.XS_BUFFER_SIZE);
	}

	/**
	 *
	 *
	 * Miscellaneous : Convert, Blocking Queue conversion...
	 *
	 *
	 *
	 */

	/**
	 *
	 * @param source
	 * @param <IN>
	 * @return
	 */
	@SuppressWarnings("unchecked")
	public static <IN> Publisher<IN> convert(Object source) {

		if (Publisher.class.isAssignableFrom(source.getClass())) {
			return (Publisher<IN>) source;
		}
		else if (Iterable.class.isAssignableFrom(source.getClass())) {
			return from((Iterable<IN>) source);
		}
		else if (Iterator.class.isAssignableFrom(source.getClass())) {
			return from((Iterator<IN>) source);
		}
		else {
			return (Publisher<IN>) DependencyUtils.convertToPublisher(source);
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
		if (Publisher.class.isAssignableFrom(to.getClass())) {
			return (T) source;
		}
		else {
			return DependencyUtils.convertFromPublisher(source, to);
		}
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
	public static <IN> BlockingQueue<IN> toReadQueue(Publisher<IN> source,
			int size,
			boolean cancelAfterFirstRequestComplete) {
		return toReadQueue(source,
				size,
				cancelAfterFirstRequestComplete,
				size == Integer.MAX_VALUE ? new ConcurrentLinkedQueue<IN>() : new ArrayBlockingQueue<IN>(size));
	}

	/**
	 * @param <IN>
	 * @return
	 */
	public static <IN> BlockingQueue<IN> toReadQueue(Publisher<IN> source,
			int size,
			boolean cancelAfterFirstRequestComplete,
			Queue<IN> store) {
		return new BlockingQueueSubscriber<>(source, null, store, cancelAfterFirstRequestComplete, size);
	}

	/**
	 * Internals
	 */

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

	private static final PublisherToPublisherFunction<?> P2P_FUNCTION = new PublisherToPublisherFunction<>();

	private static class PublisherToPublisherFunction<I>
			implements Function<Publisher<? extends I>, Publisher<? extends I>> {

		@Override
		public Publisher<? extends I> apply(Publisher<? extends I> publisher) {
			return publisher;
		}
	}

	private static final IdentityFunction IDENTITY_FUNCTION = new IdentityFunction();

	private static class IdentityFunction implements Function {

		@Override
		public Object apply(Object o) {
			return o;
		}
	}
}
