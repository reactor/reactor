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

import com.gs.collections.impl.list.mutable.FastList;
import org.slf4j.LoggerFactory;
import reactor.core.Environment;
import reactor.core.composable.Composable;
import reactor.core.composable.Deferred;
import reactor.core.composable.Promise;
import reactor.core.composable.Stream;
import reactor.event.dispatch.Dispatcher;
import reactor.function.Consumer;
import reactor.function.Function;
import reactor.function.Supplier;
import reactor.tuple.Tuple;
import reactor.tuple.Tuple2;
import reactor.tuple.Tuple3;
import reactor.tuple.Tuple4;
import reactor.tuple.Tuple5;
import reactor.tuple.Tuple6;
import reactor.tuple.Tuple7;
import reactor.tuple.Tuple8;
import reactor.tuple.TupleN;

import java.lang.Object;
import java.lang.Override;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.List;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * Helper methods for creating {@link Deferred} instances, backed by a {@link Promise}.
 *
 * @author Stephane Maldini
 * @author Jon Brisbin
 */
public abstract class Promises {

	/**
	 * Create a {@link Deferred} backed by a {@link Promise}.
	 *
	 * @param env
	 * 		the {@link reactor.core.Environment} to use
	 * @param <T>
	 * 		type of the expected value
	 *
	 * @return a new {@link reactor.core.composable.Deferred}
	 */
	public static <T> Deferred<T, Promise<T>> defer(Environment env) {
		return defer(env, env.getDefaultDispatcher());
	}

	/**
	 * Create a {@link Deferred} backed by a {@link Promise}.
	 *
	 * @param env
	 * 		the {@link reactor.core.Environment} to use
	 * @param dispatcher
	 * 		the name of the {@link reactor.event.dispatch.Dispatcher} to use
	 * @param <T>
	 * 		type of the expected value
	 *
	 * @return a new {@link reactor.core.composable.Deferred}
	 */
	public static <T> Deferred<T, Promise<T>> defer(Environment env, String dispatcher) {
		return defer(env, env.getDispatcher(dispatcher));
	}

	/**
	 * Create a {@link Deferred} backed by a {@link Promise}.
	 *
	 * @param env
	 * 		the {@link reactor.core.Environment} to use
	 * @param dispatcher
	 * 		the {@link reactor.event.dispatch.Dispatcher} to use
	 * @param <T>
	 * 		type of the expected value
	 *
	 * @return a new {@link reactor.core.composable.Deferred}
	 */
	public static <T> Deferred<T, Promise<T>> defer(Environment env, Dispatcher dispatcher) {
		return new DeferredPromiseSpec<T>().env(env).dispatcher(dispatcher).get();
	}

	/**
	 * Create a {@link Deferred} backed by a {@link Promise}.
	 *
	 * @param <T>
	 * 		type of the expected value
	 *
	 * @return A {@link DeferredPromiseSpec}.
	 */
	public static <T> DeferredPromiseSpec<T> defer() {
		return new DeferredPromiseSpec<T>();
	}

	/**
	 * Create a {@link Deferred} backed by a {@link Promise} and producing the value for the {@link Promise} using the
	 * given supplier.
	 *
	 * @param supplier
	 * 		{@link Supplier} that will produce the value
	 * @param <T>
	 * 		type of the expected value
	 *
	 * @return A {@link PromiseSpec}.
	 */
	public static <T> PromiseSpec<T> task(Supplier<T> supplier) {
		return new PromiseSpec<T>().supply(supplier);
	}

	/**
	 * Create a {@link Deferred} backed by a {@link Promise} and use the given value to complete the {@link Promise}
	 * immediately.
	 *
	 * @param value
	 * 		the value to complete the {@link Promise} with
	 * @param <T>
	 * 		the type of the value
	 *
	 * @return A {@link PromiseSpec} that will produce a {@link Promise} that is completed with the given value
	 */
	public static <T> PromiseSpec<T> success(T value) {
		return new PromiseSpec<T>().success(value);
	}

	/**
	 * Create a {@link Deferred} backed by a {@link Promise} and use the given error to complete the {@link Promise}
	 * immediately.
	 *
	 * @param error
	 * 		the error to complete the {@link Promise} with
	 * @param <T>
	 * 		the type of the value
	 *
	 * @return A {@link PromiseSpec} that will produce a {@link Promise} that is completed with the given error
	 */
	public static <T> PromiseSpec<T> error(Throwable error) {
		return new PromiseSpec<T>().error(error);
	}


    /**
     * Merge given promises into a new a {@literal Promise} that will be fulfilled when all of the given {@literal Promise
     * Promises} have been fulfilled.
     *
     * @param p1, p2
     * 		The promises to use.
     * @param <T1,T2>
     * 		The type of the function Tuple result.
     *
     * @return a {@link Promise}.
     */
    @SuppressWarnings("unchecked")
    public static <T1,T2> Promise<Tuple2<T1,T2>> heterogeneousWhen(Promise<T1> p1, Promise<T2> p2) {
        Promise<Object> untypedP1 = untypedPromise(p1);
        Promise<Object> untypedP2 = untypedPromise(p2);

        return when(untypedP1, untypedP2).map(new Function<List<Object>, Tuple2<T1,T2>>() {
            @Override
            public Tuple2<T1,T2> apply(List<Object> objects) {
                return Tuple.of((T1) objects.get(0), (T2) objects.get(1));
            }
        });
    }

    /**
     * Merge given promises into a new a {@literal Promise} that will be fulfilled when all of the given {@literal Promise
     * Promises} have been fulfilled.
     *
     * @param p1, p2, p3
     * 		The promises to use.
     * @param <T1,T2,T3>
     * 		The type of the function Tuple result.
     *
     * @return a {@link Promise}.
     */
    @SuppressWarnings("unchecked")
    public static <T1,T2,T3> Promise<Tuple3<T1,T2,T3>> heterogeneousWhen(Promise<T1> p1, Promise<T2> p2, Promise<T3> p3) {
        Promise<Object> untypedP1 = untypedPromise(p1);
        Promise<Object> untypedP2 = untypedPromise(p2);
        Promise<Object> untypedP3 = untypedPromise(p3);

        return Promises.when(untypedP1, untypedP2, untypedP3).map(new Function<List<Object>, Tuple3<T1,T2,T3>>() {
            @Override
            public Tuple3<T1,T2,T3> apply(List<Object> objects) {
                return Tuple.of((T1) objects.get(0), (T2) objects.get(1), (T3) objects.get(2));
            }
        });
    }

    /**
     * Merge given promises into a new a {@literal Promise} that will be fulfilled when all of the given {@literal Promise
     * Promises} have been fulfilled.
     *
     * @param p1, p2, p3, p4
     * 		The promises to use.
     * @param <T1,T2,T3,T4>
     * 		The type of the function Tuple result.
     *
     * @return a {@link Promise}.
     */
    @SuppressWarnings("unchecked")
    public static <T1,T2,T3,T4> Promise<Tuple4<T1,T2,T3,T4>> heterogeneousWhen(Promise<T1> p1, Promise<T2> p2, Promise<T3> p3, Promise<T4> p4) {
        Promise<Object> untypedP1 = untypedPromise(p1);
        Promise<Object> untypedP2 = untypedPromise(p2);
        Promise<Object> untypedP3 = untypedPromise(p3);
        Promise<Object> untypedP4 = untypedPromise(p4);

        return Promises.when(untypedP1, untypedP2, untypedP3, untypedP4).map(new Function<List<Object>, Tuple4<T1,T2,T3,T4>>() {
            @Override
            public Tuple4<T1,T2,T3,T4> apply(List<Object> objects) {
                return Tuple.of((T1) objects.get(0), (T2) objects.get(1), (T3) objects.get(2), (T4) objects.get(3));
            }
        });
    }

    /**
     * Merge given promises into a new a {@literal Promise} that will be fulfilled when all of the given {@literal Promise
     * Promises} have been fulfilled.
     *
     * @param p1, p2, p3, p4, p5
     * 		The promises to use.
     * @param <T1,T2,T3,T4,T5>
     * 		The type of the function Tuple result.
     *
     * @return a {@link Promise}.
     */
    @SuppressWarnings("unchecked")
    public static <T1,T2,T3,T4,T5> Promise<Tuple5<T1,T2,T3,T4,T5>> heterogeneousWhen(Promise<T1> p1, Promise<T2> p2, Promise<T3> p3, Promise<T4> p4, Promise<T5> p5) {
        Promise<Object> untypedP1 = untypedPromise(p1);
        Promise<Object> untypedP2 = untypedPromise(p2);
        Promise<Object> untypedP3 = untypedPromise(p3);
        Promise<Object> untypedP4 = untypedPromise(p4);
        Promise<Object> untypedP5 = untypedPromise(p5);

        return Promises.when(untypedP1, untypedP2, untypedP3, untypedP4, untypedP5).map(new Function<List<Object>, Tuple5<T1,T2,T3,T4,T5>>() {
            @Override
            public Tuple5<T1,T2,T3,T4,T5> apply(List<Object> objects) {
                return Tuple.of((T1) objects.get(0), (T2) objects.get(1), (T3) objects.get(2), (T4) objects.get(3), (T5) objects.get(4));
            }
        });
    }

    /**
     * Merge given promises into a new a {@literal Promise} that will be fulfilled when all of the given {@literal Promise
     * Promises} have been fulfilled.
     *
     * @param p1, p2, p3, p4, p5, p6
     * 		The promises to use.
     * @param <T1,T2,T3,T4,T5,T6>
     * 		The type of the function Tuple result.
     *
     * @return a {@link Promise}.
     */
    @SuppressWarnings("unchecked")
    public static <T1,T2,T3,T4,T5,T6> Promise<Tuple6<T1,T2,T3,T4,T5,T6>> heterogeneousWhen(Promise<T1> p1, Promise<T2> p2, Promise<T3> p3, Promise<T4> p4, Promise<T5> p5, Promise<T6> p6) {
        Promise<Object> untypedP1 = untypedPromise(p1);
        Promise<Object> untypedP2 = untypedPromise(p2);
        Promise<Object> untypedP3 = untypedPromise(p3);
        Promise<Object> untypedP4 = untypedPromise(p4);
        Promise<Object> untypedP5 = untypedPromise(p5);
        Promise<Object> untypedP6 = untypedPromise(p6);

        return Promises.when(untypedP1, untypedP2, untypedP3, untypedP4, untypedP5, untypedP6).map(new Function<List<Object>, Tuple6<T1,T2,T3,T4,T5,T6>>() {
            @Override
            public Tuple6<T1,T2,T3,T4,T5,T6> apply(List<Object> objects) {
                return Tuple.of((T1) objects.get(0), (T2) objects.get(1), (T3) objects.get(2), (T4) objects.get(3), (T5) objects.get(4), (T6) objects.get(5));
            }
        });
    }

    /**
     * Merge given promises into a new a {@literal Promise} that will be fulfilled when all of the given {@literal Promise
     * Promises} have been fulfilled.
     *
     * @param p1, p2, p3, p4, p5, p6, p7
     * 		The promises to use.
     * @param <T1,T2,T3,T4,T5,T6,T7>
     * 		The type of the function Tuple result.
     *
     * @return a {@link Promise}.
     */
    @SuppressWarnings("unchecked")
    public static <T1,T2,T3,T4,T5,T6,T7> Promise<Tuple7<T1,T2,T3,T4,T5,T6,T7>> heterogeneousWhen(Promise<T1> p1, Promise<T2> p2, Promise<T3> p3, Promise<T4> p4, Promise<T5> p5, Promise<T6> p6, Promise<T7> p7) {
        Promise<Object> untypedP1 = untypedPromise(p1);
        Promise<Object> untypedP2 = untypedPromise(p2);
        Promise<Object> untypedP3 = untypedPromise(p3);
        Promise<Object> untypedP4 = untypedPromise(p4);
        Promise<Object> untypedP5 = untypedPromise(p5);
        Promise<Object> untypedP6 = untypedPromise(p6);
        Promise<Object> untypedP7 = untypedPromise(p7);

        return Promises.when(untypedP1, untypedP2, untypedP3, untypedP4, untypedP5, untypedP6, untypedP7).map(new Function<List<Object>, Tuple7<T1,T2,T3,T4,T5,T6,T7>>() {
            @Override
            public Tuple7<T1,T2,T3,T4,T5,T6,T7> apply(List<Object> objects) {
                return Tuple.of((T1) objects.get(0), (T2) objects.get(1), (T3) objects.get(2), (T4) objects.get(3), (T5) objects.get(4), (T6) objects.get(5), (T7) objects.get(6));
            }
        });
    }

    /**
     * Merge given promises into a new a {@literal Promise} that will be fulfilled when all of the given {@literal Promise
     * Promises} have been fulfilled.
     *
     * @param p1, p2, p3, p4, p5, p6, p7, p8
     * 		The promises to use.
     * @param <T1,T2,T3,T4,T5,T6,T7,T8>
     * 		The type of the function Tuple result.
     *
     * @return a {@link Promise}.
     */
    @SuppressWarnings("unchecked")
    public static <T1,T2,T3,T4,T5,T6,T7,T8> Promise<Tuple8<T1,T2,T3,T4,T5,T6,T7,T8>> heterogeneousWhen(Promise<T1> p1, Promise<T2> p2, Promise<T3> p3, Promise<T4> p4, Promise<T5> p5, Promise<T6> p6, Promise<T7> p7, Promise<T8> p8) {
        Promise<Object> untypedP1 = untypedPromise(p1);
        Promise<Object> untypedP2 = untypedPromise(p2);
        Promise<Object> untypedP3 = untypedPromise(p3);
        Promise<Object> untypedP4 = untypedPromise(p4);
        Promise<Object> untypedP5 = untypedPromise(p5);
        Promise<Object> untypedP6 = untypedPromise(p6);
        Promise<Object> untypedP7 = untypedPromise(p7);
        Promise<Object> untypedP8 = untypedPromise(p8);

        return Promises.when(untypedP1, untypedP2, untypedP3, untypedP4, untypedP5, untypedP6, untypedP7, untypedP8).map(new Function<List<Object>, Tuple8<T1,T2,T3,T4,T5,T6,T7,T8>>() {
            @Override
            public Tuple8<T1,T2,T3,T4,T5,T6,T7,T8> apply(List<Object> objects) {
                return Tuple.of((T1) objects.get(0), (T2) objects.get(1), (T3) objects.get(2), (T4) objects.get(3), (T5) objects.get(4), (T6) objects.get(5), (T7) objects.get(6), (T8) objects.get(7));
            }
        });
    }

    /**
     * Merge given promises into a new a {@literal Promise} that will be fulfilled when all of the given {@literal Promise
     * Promises} have been fulfilled.
     *
     * @param p1, p2, p3, p4, p5, p6, p7, p8, pRest
     * 		The promises to use.
     * @param <T1,T2,T3,T4,T5,T6,T7,T8,TRest>
     * 		The type of the function Tuple result.
     *
     * @return a {@link Promise}.
     */
    @SuppressWarnings("unchecked")
    public static <T1,T2,T3,T4,T5,T6,T7,T8,TRest extends Tuple> Promise<TupleN<T1,T2,T3,T4,T5,T6,T7,T8,TRest>> heterogeneousWhen(Promise<T1> p1, Promise<T2> p2, Promise<T3> p3, Promise<T4> p4, Promise<T5> p5, Promise<T6> p6, Promise<T7> p7, Promise<T8> p8, Promise<TRest> pRest) {
        Promise<Object> untypedP1 = untypedPromise(p1);
        Promise<Object> untypedP2 = untypedPromise(p2);
        Promise<Object> untypedP3 = untypedPromise(p3);
        Promise<Object> untypedP4 = untypedPromise(p4);
        Promise<Object> untypedP5 = untypedPromise(p5);
        Promise<Object> untypedP6 = untypedPromise(p6);
        Promise<Object> untypedP7 = untypedPromise(p7);
        Promise<Object> untypedP8 = untypedPromise(p8);
        Promise<Object> untypedPRest = untypedPromise(pRest);

        return Promises.when(untypedP1, untypedP2, untypedP3, untypedP4, untypedP5, untypedP6, untypedP7, untypedP8, untypedPRest).map(new Function<List<Object>, TupleN<T1,T2,T3,T4,T5,T6,T7,T8,TRest>>() {
            @Override
            public TupleN<T1,T2,T3,T4,T5,T6,T7,T8,TRest> apply(List<Object> objects) {
                return Tuple.of((T1) objects.get(0), (T2) objects.get(1), (T3) objects.get(2), (T4) objects.get(3), (T5) objects.get(4), (T6) objects.get(5), (T7) objects.get(6), (T8) objects.get(7), (TRest) objects.get(8));
            }
        });
    }


    /**
	 * Merge given promises into a new a {@literal Promise} that will be fulfilled when all of the given {@literal Promise
	 * Promises} have been fulfilled.
	 *
	 * @param promises
	 * 		The promises to use.
	 * @param <T>
	 * 		The type of the function result.
	 *
	 * @return a {@link Promise}.
	 */
	public static <T> Promise<List<T>> when(Promise<T>... promises) {
		return when(Arrays.asList(promises));
	}

	/**
	 * Merge given deferred promises into a new a {@literal Promise} that will be fulfilled when all of the given
	 * {@literal Deferred Deferreds} have been fulfilled.
	 *
	 * @param promises
	 * 		The promises to use.
	 * @param <T>
	 * 		The type of the function result.
	 *
	 * @return a {@link Promise}.
	 */
	public static <T> Promise<List<T>> when(Deferred<T, Promise<T>>... promises) {
		return when(deferredToPromises(promises));
	}

	/**
	 * Aggregate given promises into a new a {@literal Promise} that will be fulfilled when all of the given {@literal
	 * Promise Promises} have been fulfilled.
	 *
	 * @param promises
	 * 		The promises to use.
	 * @param <T>
	 * 		The type of the function result.
	 *
	 * @return a {@link DeferredPromiseSpec}.
	 */
	public static <T> Promise<List<T>> when(Collection<? extends Promise<T>> promises) {
		final AtomicInteger count = new AtomicInteger(promises.size());
		final List<T> values = FastList.newList(promises.size());
		final Deferred<List<T>, Promise<List<T>>> d = new DeferredPromiseSpec<List<T>>()
				.synchronousDispatcher()
				.get();

		int i = 0;
		for (Promise<T> promise : promises) {
			final int idx = i++;
			if (promise.isComplete()) {
				count.decrementAndGet();
				try {
					values.add(idx, promise.get());
				} catch (Throwable t) {
					d.accept(t);
					return d.compose();
				}
			} else {
				promise
						.onSuccess(new Consumer<T>() {
							@Override
							public void accept(T t) {
								values.add(idx, t);
								if (count.decrementAndGet() == 0) {
									if (!d.compose().isComplete()) {
										d.accept(values);
									}
								}
							}
						})
						.onError(new Consumer<Throwable>() {
							@Override
							public void accept(Throwable throwable) {
								count.decrementAndGet();
								if (!d.compose().isComplete()) {
									d.accept(throwable);
								} else {
									LoggerFactory.getLogger(Promises.class.getName() + ".when")
									             .error(throwable.getMessage(), throwable);
								}
							}
						});
			}
		}

		return d.compose();
	}


	/**
	 * Pick the first result coming from any of the given promises and populate a new {@literal Promise}.
	 *
	 * @param promises
	 * 		The deferred promises to use.
	 * @param <T>
	 * 		The type of the function result.
	 *
	 * @return a {@link Promise}.
	 */
	public static <T> Promise<T> any(Deferred<T, Promise<T>>... promises) {
		return any(deferredToPromises(promises));
	}

	/**
	 * Pick the first result coming from any of the given promises and populate a new {@literal Promise}.
	 *
	 * @param promises
	 * 		The deferred promises to use.
	 * @param <T>
	 * 		The type of the function result.
	 *
	 * @return a {@link Promise}.
	 */
	public static <T> Promise<T> any(Promise<T>... promises) {
		return any(Arrays.asList(promises));
	}


	/**
	 * Pick the first result coming from any of the given promises and populate a new {@literal Promise}.
	 *
	 * @param promises
	 * 		The promises to use.
	 * @param <T>
	 * 		The type of the function result.
	 *
	 * @return a {@link DeferredStreamSpec}.
	 */
	public static <T> Promise<T> any(Collection<? extends Promise<T>> promises) {
		Stream<T> deferredStream = new DeferredStreamSpec<T>()
				.synchronousDispatcher()
				.batchSize(promises.size())
				.get()
				.compose();

		Stream<T> firstStream = deferredStream.first();

		Promise<T> resultPromise = new DeferredPromiseSpec<T>()
				.link(firstStream)
				.get()
				.compose();

		firstStream.connectValues(resultPromise).connectErrors(resultPromise);

		for (Promise<T> promise : promises) {
			promise.connectErrors(deferredStream).connectValues(deferredStream);
		}

		return resultPromise;
	}

	/**
	 * Consume the next value of the given {@link reactor.core.composable.Composable} and fulfill the returned {@link
	 * reactor.core.composable.Promise} on the next value.
	 *
	 * @param composable
	 * 		the {@literal Composable} to consume the next value from
	 * @param <T>
	 * 		type of the value
	 *
	 * @return a {@link reactor.core.composable.Promise} that will be fulfilled with the next value coming into the given
	 * Composable
	 */
	public static <T> Promise<T> next(Composable<T> composable) {
		final AtomicBoolean called = new AtomicBoolean(false);
		final Deferred<T, Promise<T>> d = Promises.<T>defer().get();

		composable
				.when(Throwable.class, new Consumer<Throwable>() {
					@Override
					public void accept(Throwable throwable) {
						if (!called.get()
								&& called.compareAndSet(false, true)
								&& !d.compose().isComplete()) {
							d.accept(throwable);
						}
					}
				})
				.consume(new Consumer<T>() {
					@Override
					public void accept(T t) {
						if (!called.get()
								&& called.compareAndSet(false, true)
								&& !d.compose().isComplete()) {
							d.accept(t);
						}
					}
				});

		return d.compose();
	}

	private static <T> List<Promise<T>> deferredToPromises(Deferred<T, Promise<T>>... promises) {
		List<Promise<T>> promiseList = new ArrayList<Promise<T>>();
		for (Deferred<T, Promise<T>> deferred : promises) {
			promiseList.add(deferred.compose());
		}
		return promiseList;
	}

    /**
     * Just hide type of a given promise
     * @param promise any promise to untyped
     * @return an untyped promise
     */
    private static <T> Promise<Object> untypedPromise(Promise<T> promise) {
        return promise.map(new Function<T, Object>() {
            @Override
            public Object apply(T o) {
                return o;
            }
        });
    }


}
