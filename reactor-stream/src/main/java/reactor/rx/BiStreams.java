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
package reactor.rx;

import java.util.Map;

import org.reactivestreams.Publisher;
import reactor.fn.BiFunction;
import reactor.fn.tuple.Tuple2;
import reactor.rx.stream.StreamKv;
import reactor.rx.stream.StreamReduceByKey;
import reactor.rx.stream.StreamScanByKey;

/**
 * A Streams add-on to work with key/value pairs hydrated in {@link reactor.fn.tuple.Tuple2}.
 * Main factories support binding incoming values into arbitrary {@link java.util.Map} stores by key.
 *
 * @author Stephane Maldini
 */
public class BiStreams extends Streams {

	private BiStreams() {
	}

	/**
	 * @param publisher
	 * @param accumulator
	 * @param <KEY>
	 * @param <VALUE>
	 * @return
	 */
	public static <KEY, VALUE> Stream<Tuple2<KEY, VALUE>> reduceByKey(Publisher<Tuple2<KEY, VALUE>> publisher,
	                                                                  BiFunction<VALUE, VALUE, VALUE> accumulator) {
		return reduceByKey(publisher, null, null, accumulator);
	}

	/**
	 * @param publisher
	 * @param mapStream
	 * @param accumulator
	 * @param <KEY>
	 * @param <VALUE>
	 * @return
	 */
	public static <KEY, VALUE> Stream<Tuple2<KEY, VALUE>> reduceByKey(Publisher<Tuple2<KEY, VALUE>> publisher,
	                                                                  StreamKv<KEY, VALUE> mapStream,
	                                                                  BiFunction<VALUE, VALUE, VALUE> accumulator) {
		return reduceByKey(publisher, mapStream.getStore(), mapStream, accumulator);
	}

	/**
	 * @param publisher
	 * @param store
	 * @param listener
	 * @param accumulator
	 * @param <KEY>
	 * @param <VALUE>
	 * @return
	 */
	public static <KEY, VALUE> Stream<Tuple2<KEY, VALUE>> reduceByKey(Publisher<Tuple2<KEY, VALUE>> publisher,
	                                                                  Map<KEY, VALUE> store,
	                                                                  Publisher<? extends StreamKv.Signal<KEY,
	                                                                    VALUE>> listener,
	                                                                  BiFunction<VALUE, VALUE, VALUE> accumulator) {
		return reduceByKeyOn(publisher, store, listener, accumulator);
	}

	/**
	 * @param publisher
	 * @param store
	 * @param listener
	 * @param accumulator
	 * @param <KEY>
	 * @param <VALUE>
	 * @return
	 */
	public static <KEY, VALUE> Stream<Tuple2<KEY, VALUE>> reduceByKeyOn(Publisher<Tuple2<KEY, VALUE>> publisher,
	                                                                    Map<KEY, VALUE> store,
	                                                                    Publisher<? extends StreamKv.Signal<KEY,
	                                                                      VALUE>> listener,
	                                                                    BiFunction<VALUE, VALUE, VALUE> accumulator) {
		return new StreamReduceByKey<>(publisher, accumulator, store, listener);
	}

	//scan

	/**
	 * @param publisher
	 * @param accumulator
	 * @param <KEY>
	 * @param <VALUE>
	 * @return
	 */
	public static <KEY, VALUE> Stream<Tuple2<KEY, VALUE>> scanByKey(Publisher<Tuple2<KEY, VALUE>> publisher,
	                                                                BiFunction<VALUE, VALUE, VALUE> accumulator) {
		return scanByKey(publisher, null, null, accumulator);
	}

	/**
	 * @param publisher
	 * @param mapStream
	 * @param accumulator
	 * @param <KEY>
	 * @param <VALUE>
	 * @return
	 */
	public static <KEY, VALUE> Stream<Tuple2<KEY, VALUE>> scanByKey(Publisher<Tuple2<KEY, VALUE>> publisher,
	                                                                StreamKv<KEY, VALUE> mapStream,
	                                                                BiFunction<VALUE, VALUE, VALUE> accumulator) {
		return scanByKey(publisher, mapStream.getStore(), mapStream, accumulator);
	}

	/**
	 * @param publisher
	 * @param store
	 * @param listener
	 * @param accumulator
	 * @param <KEY>
	 * @param <VALUE>
	 * @return
	 */
	public static <KEY, VALUE> Stream<Tuple2<KEY, VALUE>> scanByKey(Publisher<Tuple2<KEY, VALUE>> publisher,
	                                                                Map<KEY, VALUE> store,
	                                                                Publisher<? extends StreamKv.Signal<KEY, VALUE>>
	                                                                  listener,
	                                                                BiFunction<VALUE, VALUE, VALUE> accumulator) {
		return scanByKeyOn(publisher, store, listener, accumulator);
	}

	/**
	 * @param publisher
	 * @param store
	 * @param listener
	 * @param accumulator
	 * @param <KEY>
	 * @param <VALUE>
	 * @return
	 */
	public static <KEY, VALUE> Stream<Tuple2<KEY, VALUE>> scanByKeyOn(Publisher<Tuple2<KEY, VALUE>> publisher,
	                                                                  Map<KEY, VALUE> store,
	                                                                  Publisher<? extends StreamKv.Signal<KEY,
	                                                                    VALUE>> listener,
	                                                                  BiFunction<VALUE, VALUE, VALUE> accumulator) {
		return new StreamScanByKey<>(publisher, accumulator, listener, store);
	}
}
