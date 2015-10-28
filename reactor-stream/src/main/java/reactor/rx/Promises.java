/*
 * Copyright (c) 2011-2014 Pivotal Software, Inc.
 *
 *  Licensed under the Apache License, Version 2.0 (the "License");
 *  you may not use this file except in compliance with the License.
 *  You may obtain a copy of the License at
 *
 *         http://www.apache.org/licenses/LICENSE-2.0
 *
 *  Unless required by applicable law or agreed to in writing, software
 *  distributed under the License is distributed on an "AS IS" BASIS,
 *  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  See the License for the specific language governing permissions and
 *  limitations under the License.
 */

package reactor.rx;

import org.reactivestreams.Processor;
import org.reactivestreams.Publisher;
import org.reactivestreams.Subscriber;
import reactor.Publishers;
import reactor.Timers;
import reactor.core.error.Exceptions;
import reactor.core.subscriber.SubscriberWithContext;
import reactor.core.support.Assert;
import reactor.fn.Consumer;
import reactor.fn.Function;
import reactor.fn.Supplier;
import reactor.fn.timer.Timer;
import reactor.fn.tuple.*;

import java.util.Arrays;
import java.util.List;

/**
 * Helper methods for creating {@link reactor.rx.Promise} instances.
 *
 * @author Stephane Maldini
 * @author Jon Brisbin
 */
public final class Promises {

	private final static Promise COMPLETE = success(null);

	/**
	 * Take the next value from a Publisher on subscribe.
	 *
	 * @param publisher
	 * @param <T> type of the expected value
	 * @return A {@link Promise}.
	 */
	@SuppressWarnings("unchecked")
	public static <T> Promise<T> from(final Publisher<T> publisher) {
		if(Promise.class.isAssignableFrom(publisher.getClass())){
			return (Promise<T>)publisher;
		}
		else if(Supplier.class.isAssignableFrom(publisher.getClass())) {
			try{
				return success(((Supplier<T>)publisher).get());
			}
			catch (Throwable t){
				Exceptions.throwIfFatal(t);
				return error(t);
			}
		}
		Promise<T> p = prepare();
		publisher.subscribe(p);
		return p;
	}

	/**
	 * Create a synchronous {@link Promise}.
	 *
	 * @param <T> type of the expected value
	 * @return A {@link Promise}.
	 */
	public static <T> Promise<T> ready() {
		return ready(Timers.globalOrNull());
	}

	/**
	 * Create a {@link Promise}.
	 *
	 * @param timer        the {@link reactor.fn.timer.Timer} to use by default for scheduled operations
	 * @param <T>        type of the expected value
	 * @return a new {@link reactor.rx.Promise}
	 */
	public static <T> Promise<T> ready(Timer timer) {
		return new Promise<T>(timer);
	}

	/**
	 * Create a synchronous {@link Promise}.
	 *
	 * @param <T> type of the expected value
	 * @return A {@link Promise}.
	 */
	public static <T> Promise<T> prepare() {
		return prepare(Timers.globalOrNull());
	}

	/**
	 * Create a `{@link Promise}.
	 *
	 * @param timer        the {@link reactor.fn.timer.Timer} to use by default for scheduled operations
	 * @param <T>        type of the expected value
	 * @return a new {@link reactor.rx.Promise}
	 */
	public static <T> Promise<T> prepare(Timer timer) {
		Promise<T> p = new Promise<T>(timer);
		p.request(1);
		return p;
	}

	/**
	 * Create a {@link Promise} producing the value for the {@link Promise} using the
	 * given supplier.
	 *
	 * @param supplier {@link Supplier} that will produce the value
	 * @param <T>      type of the expected value
	 * @return A {@link Promise}.
	 */
	public static <T> Promise<T> task(Supplier<T> supplier) {
		return task(null, Timers.globalOrNull(),  supplier);
	}

	/**
	 * Create a {@link Promise} producing the value for the {@link Promise} using the
	 * given supplier.
	 *
	 * @param processor {@link Processor} that will execute the signals possibly asynchronously
	 * @param supplier {@link Supplier} that will produce the value
	 * @param <T>      type of the expected value
	 * @return A {@link Promise}.
	 */
	public static <T> Promise<T> task(Processor<T, T> processor, Supplier<T> supplier) {
		return task(processor, Timers.globalOrNull(),  supplier);
	}

	/**
	 * Create a {@link Promise} producing the value for the {@link Promise} using the
	 * given supplier.
	 *
	 * @param timer        the {@link reactor.fn.timer.Timer} to use by default for scheduled operations
	 * @param supplier {@link Supplier} that will produce the value
	 * @param <T>      type of the expected value
	 * @return A {@link Promise}.
	 */
	public static <T> Promise<T> task(Processor<T, T> processor, Timer timer, final Supplier<T> supplier) {
		Publisher<T> p = Publishers.create(new Consumer<SubscriberWithContext<T, Void>>() {
			@Override
			public void accept(SubscriberWithContext<T, Void> sub) {
				sub.onNext(supplier.get());
				sub.onComplete();
			}
		});

		if(processor == null) {
			return Streams.wrap(p)
			  .timer(timer)
			  .next();

		}else {
			return Streams.wrap(p)
			  .timer(timer)
			  .process(processor)
			  .next();
		}
	}

	/**
	 * Create a {@link Promise} already completed without any data.
	 *
	 * @return A {@link Promise} that is completed
	 */
	@SuppressWarnings("unchecked")
	public static Promise<Void> success() {
		return COMPLETE;
	}

	/**
	 * Create a {@link Promise} using the given value to complete the {@link Promise}
	 * immediately.
	 *
	 * @param value the value to complete the {@link Promise} with
	 * @param <T>   the type of the value
	 * @return A {@link Promise} that is completed with the given value
	 */
	public static <T> Promise<T> success(T value) {
		return success(Timers.globalOrNull(), value);
	}

	/**
	 * Create a {@link Promise} using the given value to complete the {@link Promise}
	 * immediately.
	 *
	 * @param value the value to complete the {@link Promise} with
	 * @param timer        the {@link reactor.fn.timer.Timer} to use by default for scheduled operations
	 * @param <T>   the type of the value
	 * @return A {@link Promise} that is completed with the given value
	 */
	public static <T> Promise<T> success(Timer timer, T value) {
		return new Promise<T>(value, timer);
	}

	/**
	 * Create synchronous {@link Promise} and use the given error to complete the {@link Promise}
	 * immediately.
	 *
	 * @param error the error to complete the {@link Promise} with
	 * @param <T>   the type of the value
	 * @return A {@link Promise} that is completed with the given error
	 */
	public static <T> Promise<T> error(Throwable error) {
		return error(Timers.globalOrNull(), error);
	}

	/**
	 * Create a {@link Promise} and use the given error to complete the {@link Promise}
	 * immediately.
	 *
	 * @param error the error to complete the {@link Promise} with
	 * @param timer        the {@link reactor.fn.timer.Timer} to use by default for scheduled operations
	 * @param <T>   the type of the value
	 * @return A {@link Promise} that is completed with the given error
	 */
	public static <T> Promise<T> error(Timer timer, Throwable error) {
		return new Promise<T>(error, timer);
	}


	/**
	 * Merge given promises into a new a {@literal Promise} that will be fulfilled when all of the given {@literal
	 * Promise
	 * Promises} have been fulfilled.
	 *
	 * @param p1,     p2
	 *                The promises to use.
	 * @param <T1,T2> The type of the function Tuple result.
	 * @return a {@link Promise}.
	 */
	@SuppressWarnings("unchecked")
	public static <T1, T2> Promise<Tuple2<T1, T2>> when(Promise<T1> p1, Promise<T2> p2) {
		return multiWhen(new Promise[]{p1, p2}).map(new Function<List<Object>, Tuple2<T1, T2>>() {
			@Override
			public Tuple2<T1, T2> apply(List<Object> objects) {
				return Tuple.of((T1) objects.get(0), (T2) objects.get(1));
			}
		});
	}

	/**
	 * Merge given promises into a new a {@literal Promise} that will be fulfilled when all of the given {@literal
	 * Promise
	 * Promises} have been fulfilled.
	 *
	 * @param p1,        p2, p3
	 *                   The promises to use.
	 * @param <T1,T2,T3> The type of the function Tuple result.
	 * @return a {@link Promise}.
	 */
	@SuppressWarnings("unchecked")
	public static <T1, T2, T3> Promise<Tuple3<T1, T2, T3>> when(Promise<T1> p1, Promise<T2> p2, Promise<T3> p3) {
		return multiWhen(new Promise[]{p1, p2, p3}).map(new Function<List<Object>, Tuple3<T1, T2, T3>>() {
			@Override
			public Tuple3<T1, T2, T3> apply(List<Object> objects) {
				return Tuple.of((T1) objects.get(0), (T2) objects.get(1), (T3) objects.get(2));
			}
		});
	}

	/**
	 * Merge given promises into a new a {@literal Promise} that will be fulfilled when all of the given {@literal
	 * Promise
	 * Promises} have been fulfilled.
	 *
	 * @param p1,           p2, p3, p4
	 *                      The promises to use.
	 * @param <T1,T2,T3,T4> The type of the function Tuple result.
	 * @return a {@link Promise}.
	 */
	@SuppressWarnings("unchecked")
	public static <T1, T2, T3, T4> Promise<Tuple4<T1, T2, T3, T4>> when(Promise<T1> p1, Promise<T2> p2, Promise<T3> p3,
	                                                                    Promise<T4> p4) {
		return multiWhen(new Promise[]{p1, p2, p3, p4}).map(new Function<List<Object>, Tuple4<T1, T2, T3, T4>>() {
			@Override
			public Tuple4<T1, T2, T3, T4> apply(List<Object> objects) {
				return Tuple.of((T1) objects.get(0), (T2) objects.get(1), (T3) objects.get(2), (T4) objects.get(3));
			}
		});
	}

	/**
	 * Merge given promises into a new a {@literal Promise} that will be fulfilled when all of the given {@literal
	 * Promise
	 * Promises} have been fulfilled.
	 *
	 * @param p1,              p2, p3, p4, p5
	 *                         The promises to use.
	 * @param <T1,T2,T3,T4,T5> The type of the function Tuple result.
	 * @return a {@link Promise}.
	 */
	@SuppressWarnings("unchecked")
	public static <T1, T2, T3, T4, T5> Promise<Tuple5<T1, T2, T3, T4, T5>> when(Promise<T1> p1, Promise<T2> p2,
	                                                                            Promise<T3> p3, Promise<T4> p4,
	                                                                            Promise<T5> p5) {
		return multiWhen(new Promise[]{p1, p2, p3, p4, p5}).map(new Function<List<Object>, Tuple5<T1, T2, T3, T4,
		  T5>>() {
			@Override
			public Tuple5<T1, T2, T3, T4, T5> apply(List<Object> objects) {
				return Tuple.of((T1) objects.get(0), (T2) objects.get(1), (T3) objects.get(2), (T4) objects.get(3),
				  (T5) objects.get(4));
			}
		});
	}

	/**
	 * Merge given promises into a new a {@literal Promise} that will be fulfilled when all of the given {@literal
	 * Promise
	 * Promises} have been fulfilled.
	 *
	 * @param p1,                 p2, p3, p4, p5, p6
	 *                            The promises to use.
	 * @param <T1,T2,T3,T4,T5,T6> The type of the function Tuple result.
	 * @return a {@link Promise}.
	 */
	@SuppressWarnings("unchecked")
	public static <T1, T2, T3, T4, T5, T6> Promise<Tuple6<T1, T2, T3, T4, T5, T6>> when(Promise<T1> p1, Promise<T2> p2,
	                                                                                    Promise<T3> p3, Promise<T4> p4,
	                                                                                    Promise<T5> p5, Promise<T6>
	                                                                                      p6) {
		return multiWhen(new Promise[]{p1, p2, p3, p4, p5, p6}).map(new Function<List<Object>, Tuple6<T1, T2, T3,
		  T4, T5, T6>>() {
			@Override
			public Tuple6<T1, T2, T3, T4, T5, T6> apply(List<Object> objects) {
				return Tuple.of((T1) objects.get(0), (T2) objects.get(1), (T3) objects.get(2), (T4) objects.get(3),
				  (T5) objects.get(4), (T6) objects.get(5));
			}
		});
	}

	/**
	 * Merge given promises into a new a {@literal Promise} that will be fulfilled when all of the given {@literal
	 * Promise
	 * Promises} have been fulfilled.
	 *
	 * @param p1,                    p2, p3, p4, p5, p6, p7
	 *                               The promises to use.
	 * @param <T1,T2,T3,T4,T5,T6,T7> The type of the function Tuple result.
	 * @return a {@link Promise}.
	 */
	@SuppressWarnings("unchecked")
	public static <T1, T2, T3, T4, T5, T6, T7> Promise<Tuple7<T1, T2, T3, T4, T5, T6, T7>> when(Promise<T1> p1,
	                                                                                            Promise<T2> p2,
	                                                                                            Promise<T3> p3,
	                                                                                            Promise<T4> p4,
	                                                                                            Promise<T5> p5,
	                                                                                            Promise<T6> p6,
	                                                                                            Promise<T7> p7) {
		return multiWhen(new Promise[]{p1, p2, p3, p4, p5, p6, p7}).map(new Function<List<Object>, Tuple7<T1, T2,
		  T3, T4, T5, T6, T7>>() {
			@Override
			public Tuple7<T1, T2, T3, T4, T5, T6, T7> apply(List<Object> objects) {
				return Tuple.of((T1) objects.get(0), (T2) objects.get(1), (T3) objects.get(2), (T4) objects.get(3),
				  (T5) objects.get(4), (T6) objects.get(5), (T7) objects.get(6));
			}
		});
	}

	/**
	 * Merge given promises into a new a {@literal Promise} that will be fulfilled when all of the given {@literal
	 * Promise
	 * Promises} have been fulfilled.
	 *
	 * @param p1,                       p2, p3, p4, p5, p6, p7, p8
	 *                                  The promises to use.
	 * @param <T1,T2,T3,T4,T5,T6,T7,T8> The type of the function Tuple result.
	 * @return a {@link Promise}.
	 */
	@SuppressWarnings("unchecked")
	public static <T1, T2, T3, T4, T5, T6, T7, T8> Promise<Tuple8<T1, T2, T3, T4, T5, T6, T7, T8>> when(Promise<T1> p1,
	                                                                                                    Promise<T2> p2,
	                                                                                                    Promise<T3> p3,
	                                                                                                    Promise<T4> p4,
	                                                                                                    Promise<T5> p5,
	                                                                                                    Promise<T6> p6,
	                                                                                                    Promise<T7> p7,
	                                                                                                    Promise<T8>
	                                                                                                          p8) {
		return multiWhen(new Promise[]{p1, p2, p3, p4, p5, p6, p7, p8}).map(new Function<List<Object>, Tuple8<T1,
		  T2, T3, T4, T5, T6, T7, T8>>() {
			@Override
			public Tuple8<T1, T2, T3, T4, T5, T6, T7, T8> apply(List<Object> objects) {
				return Tuple.of((T1) objects.get(0), (T2) objects.get(1), (T3) objects.get(2), (T4) objects.get(3),
				  (T5) objects.get(4), (T6) objects.get(5), (T7) objects.get(6), (T8) objects.get(7));
			}
		});
	}

	/**
	 * Aggregate given promises into a new a {@literal Promise} that will be fulfilled when all of the given {@literal
	 * Promise Promises} have been fulfilled.
	 *
	 * @param promises The promises to use.
	 * @param <T>      The type of the function result.
	 * @return a {@link Promise}.
	 */
	public static <T> Promise<List<T>> when(final List<? extends Promise<T>> promises) {
		Assert.isTrue(promises.size() > 0, "Must aggregate at least one promise");

		return Streams.merge(promises)
		  .buffer(promises.size()).consumeNext();
	}


	/**
	 * Pick the first result coming from any of the given promises and populate a new {@literal Promise}.
	 *
	 * @param promises The deferred promises to use.
	 * @param <T>      The type of the function result.
	 * @return a {@link Promise}.
	 */
	@SafeVarargs
	@SuppressWarnings("varargs")
	public static <T> Promise<T> any(Promise<T>... promises) {
		return any(Arrays.asList(promises));
	}


	/**
	 * Pick the first result coming from any of the given promises and populate a new {@literal Promise}.
	 *
	 * @param promises The promises to use.
	 * @param <T>      The type of the function result.
	 * @return a {@link Promise}.
	 */
	@SuppressWarnings("unchecked")
	public static <T> Promise<T> any(List<? extends Promise<T>> promises) {
		Assert.isTrue(promises.size() > 0, "Must aggregate at least one promise");

		Promise<T> d = new Promise<T>();

		Publishers.<T>merge(Publishers.from(promises))
				.subscribe(d);

		return d;
	}


	/**
	 * Aggregate given promises into a new a {@literal Promise} that will be fulfilled when all of the given {@literal
	 * Promise Promises} have been fulfilled.
	 *
	 * @param promises The promises to use.
	 * @param <T>      The type of the function result.
	 * @return a {@link Promise}.
	 */
	@SafeVarargs
	@SuppressWarnings("varargs")
	private static <T> Promise<List<T>> multiWhen(Promise<T>... promises) {
		return when(Arrays.<Promise<T>>asList(promises));
	}

}
