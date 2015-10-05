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

import org.reactivestreams.Processor;
import org.reactivestreams.Publisher;
import org.reactivestreams.Subscriber;
import reactor.core.error.Exceptions;
import reactor.core.processor.BaseProcessor;
import reactor.core.publisher.LogOperator;
import reactor.core.publisher.PublisherFactory;
import reactor.core.publisher.TrampolineOperator;
import reactor.core.subscriber.SubscriberWithContext;
import reactor.core.subscriber.Tap;
import reactor.core.subscriber.BlockingQueueSubscriber;
import reactor.core.support.SignalType;
import reactor.fn.BiConsumer;
import reactor.fn.Consumer;
import reactor.fn.Function;
import reactor.fn.Supplier;

import java.util.Iterator;
import java.util.Queue;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ConcurrentLinkedQueue;

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
		return PublisherFactory.create(new Consumer<SubscriberWithContext<IN, IN>>() {
			@Override
			public void accept(SubscriberWithContext<IN, IN> sub) {
				if(sub.context() != null) {
					sub.onNext(sub.context());
				}

				sub.onComplete();
			}
		}, new Function<Subscriber<? super IN>, IN>() {
			@Override
			public IN apply(Subscriber<? super IN> subscriber) {
				return data;
			}
		});
	}

	public static <T> Publisher<T> from(final Iterable<T> defaultValues){
		return PublisherFactory.create(new Consumer<SubscriberWithContext<T, Iterator<? extends T>>>() {
			@Override
			public void accept(SubscriberWithContext<T, Iterator<? extends T>> subscriber) {
				final Iterator<? extends T> iterator = subscriber.context();
				if (iterator.hasNext()) {
					subscriber.onNext(iterator.next());
				} else {
					subscriber.onComplete();
					return;
				}

				if (!iterator.hasNext()) {
					subscriber.onComplete();
				}
			}

			@Override
			public String toString() {
				return "iterable="+defaultValues;
			}
		}, new Function<Subscriber<? super T>, Iterator<? extends T>>() {
			@Override
			public Iterator<? extends T> apply(Subscriber<? super T> subscriber) {
				if (defaultValues == null) {
					throw PublisherFactory.PrematureCompleteException.INSTANCE;
				}
				return defaultValues.iterator();
			}

			@Override
			public String toString() {
				return "iterable="+defaultValues;
			}
		});
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
	public static <IN> Publisher<IN> empty() {
		return new Publisher<IN>() {
			@Override
			public void subscribe(Subscriber<? super IN> s) {
				s.onSubscribe(SignalType.NOOP_SUBSCRIPTION);
				s.onComplete();
			}
		};
	}

	/**
	 * @param <IN>
	 * @return
	 */
	public static <IN> Publisher<IN> never() {
		return new Publisher<IN>() {
			@Override
			public void subscribe(Subscriber<? super IN> s) {
				s.onSubscribe(SignalType.NOOP_SUBSCRIPTION);
			}
		};
	}


	/**
	 * Intercept a source {@link Publisher} onNext signal to eventually transform, forward or filter the data by
	 * calling
	 * or not
	 * the right operand {@link Subscriber}.
	 *
	 * @param transformer A {@link Function} that transforms each emitting sequence item
	 * @param <I>         The source type of the data sequence
	 * @param <O>         The target type of the data sequence
	 * @return a fresh Reactive Streams publisher ready to be subscribed
	 */
	public static <I, O> Publisher<O> map(Publisher<I> source, final Function<? super I, ? extends O> transformer) {
		return lift(source, new BiConsumer<I, Subscriber<? super O>>() {
			@Override
			public void accept(I i, Subscriber<? super O> subscriber) {
				subscriber.onNext(transformer.apply(i));
			}
		}, null, null);
	}

	/**
	 *
	 *
	 * @param <I>         The source type of the data sequence
	 * @return a fresh Reactive Streams publisher ready to be subscribed
	 */
	public static <I> Publisher<I> merge(Publisher<? extends Publisher<? extends I>> source) {
		Processor<Publisher<? extends I>, I> flatMap = Processors.flatMap(
		  new Function<Publisher<? extends I>, Publisher<? extends I>>() {
			@Override
			public Publisher<? extends I> apply(Publisher<? extends I> publisher) {
				return publisher;
			}
		});
		source.subscribe(flatMap);
		return flatMap;
	}

	/**
	 *
	 * @param transformer A {@link Function} that transforms each emitting sequence item
	 * @param <I>         The source type of the data sequence
	 * @param <O>         The target type of the data sequence
	 * @return a fresh Reactive Streams publisher ready to be subscribed
	 */
	public static <I, O> Publisher<O> flatMap(Publisher<I> source,
	                                          final Function<? super I, ? extends Publisher<? extends O>> transformer) {
		Processor<I, O> flatMap = Processors.flatMap(transformer);
		source.subscribe(flatMap);
		return flatMap;
	}


	/**
	 *
	 *
	 * @param <I>         The source type of the data sequence
	 * @return a fresh Reactive Streams publisher ready to be subscribed
	 */
	public static <I> Publisher<I> concat(Publisher<? extends Publisher<? extends I>> source) {
		Processor<Publisher<? extends I>, I> concatMap = Processors.concatMap(
		  new Function<Publisher<? extends I>, Publisher<? extends I>>() {
			  @Override
			  public Publisher<? extends I> apply(Publisher<? extends I> publisher) {
				  return publisher;
			  }
		  });
		source.subscribe(concatMap);
		return concatMap;
	}


	/**
	 *
	 * @param transformer A {@link Function} that transforms each emitting sequence item
	 * @param <I>         The source type of the data sequence
	 * @param <O>         The target type of the data sequence
	 * @return a fresh Reactive Streams publisher ready to be subscribed
	 */
	public static <I, O> Publisher<O> concatMap(Publisher<I> source,
	                                          final Function<? super I, ? extends Publisher<? extends O>> transformer) {
		Processor<I, O> concatMap = Processors.concatMap(transformer);
		source.subscribe(concatMap);
		return concatMap;
	}

	/**
	 * @param <IN>
	 * @return
	 */
	public static <IN> BlockingQueue<IN> toReadQueue(Publisher<IN> source) {
		return toReadQueue(source, Integer.MAX_VALUE);
	}

	/**
	 * @param <IN>
	 * @return
	 */
	public static <IN> BlockingQueue<IN> toReadQueue(Publisher<IN> source, int size) {
		return toReadQueue(source, size, false, new ArrayBlockingQueue<IN>(size));
	}

	/**
	 * @param <IN>
	 * @return
	 */
	public static <IN> BlockingQueue<IN> toReadQueue(Publisher<IN> source, int size
	  , boolean cancelAfterFirstRequestComplete) {
		return toReadQueue(source, size, cancelAfterFirstRequestComplete,
		  size == Integer.MAX_VALUE ? new ConcurrentLinkedQueue<IN>() : new ArrayBlockingQueue<IN>(size));
	}

	/**
	 * @param <IN>
	 * @return
	 */
	public static <IN> BlockingQueue<IN> toReadQueue(Publisher<IN> source, int size
	  , boolean cancelAfterFirstRequestComplete, Queue<IN> store) {
		return new BlockingQueueSubscriber<>(source, null, store, cancelAfterFirstRequestComplete, size);
	}

	/**
	 * @param publisher
	 * @param <IN>
	 * @return
	 */
	public static <IN> Publisher<IN> log(Publisher<IN> publisher) {
		return log(publisher, null);
	}

	/**
	 * @param publisher
	 * @param category
	 * @param <IN>
	 * @return
	 */
	public static <IN> Publisher<IN> log(Publisher<IN> publisher, String category) {
		return Publishers.lift(publisher, new LogOperator<IN>(category));
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
	 * Monitor the most recent value of this publisher sequence to be returned by {@link Supplier#get}
	 *
	 * @param publisher the sequence to monitor
	 * @param <IN>      the sequence type
	 * @return a new {@link Supplier} tapping into publisher (requesting an unbounded demand of Long.MAX_VALUE)
	 */
	public static <IN> Supplier<IN> tap(Publisher<IN> publisher) {
		Tap<IN> tap = Tap.create();
		publisher.subscribe(tap);
		return tap;
	}

}
