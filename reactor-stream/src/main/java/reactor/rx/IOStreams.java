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
package reactor.rx;

import org.reactivestreams.Publisher;
import reactor.Environment;
import reactor.io.codec.Codec;
import reactor.rx.stream.MapStream;
import reactor.rx.stream.io.ChronicleReaderStream;
import reactor.rx.stream.io.ChronicleStream;
import reactor.rx.stream.io.DecoderStream;

import java.io.IOException;

/**
 * A Streams add-on to work with IO components such as ChronicleStreams.
 * Chronicle backed persistentMap require chronicle 3.3+ in the classpath (not provided transitively by reactor-core)
 *
 * @author Stephane Maldini
 */
public class IOStreams extends Streams {

	private IOStreams() {
	}

	/**
	 * A Persistent Map is a {@link reactor.rx.stream.MapStream} that fulfill both the Map and the Stream contract.
	 * Effectively it will act a store to be shared by consumers such as {@link reactor.rx.BiStreams} operators.
	 * Implementing the MapStream contract means that subscribers will have the ability to listen for put, putAll, clear
	 * and delete operations.
	 *
	 * A persistent map is read/write capable and will use an internal cached map for read operations. This cache will be
	 * updated when an operation has been notified. Read-only persistent map can be created with {@link #persistentMapReader(String)}.
	 *
	 * By default a persistentMap survives any application shutdown up to the latest written data. For a given name,
	 * the store is going to be located under the same default base location.
	 *
	 * @param name The identified name for the data store
	 * @param <KEY> the type of the key used to identify stored values
	 * @param <VALUE> the type of the payload stored
	 *
	 * @return a new MapStream that can be notified on any operation (put, putAll...)
	 */
	public static <KEY, VALUE> MapStream<KEY, VALUE> persistentMap(String name) {
		return persistentMap(name, false);
	}

	/**
	 * A Persistent Map is a {@link reactor.rx.stream.MapStream} that fulfill both the Map and the Stream contract.
	 * Effectively it will act a store to be shared by consumers such as {@link reactor.rx.BiStreams} operators.
	 * Implementing the MapStream contract means that subscribers will have the ability to listen for put, putAll, clear
	 * and delete operations.
	 *
	 * A persistent map is read/write capable and will use an internal cached map for read operations. This cache will be
	 * updated when an operation has been notified. Read-only persistent map can be created with {@link #persistentMapReader(String)}.
	 *
	 * By default a persistentMap survives any application shutdown up to the latest written data. For a given name,
	 * the store is going to be located under the same default base location.
	 *
	 * @param name The identified name for the data store
	 * @param deleteOnExit Make a persistent store transient, useful for producer queues coupled with persistent map readers.
	 * @param <KEY> the type of the key used to identify stored values
	 * @param <VALUE> the type of the payload stored
	 *
	 * @return a new MapStream that can be notified on any operation (put, putAll...)
	 */
	public static <KEY, VALUE> MapStream<KEY, VALUE> persistentMap(String name, boolean deleteOnExit){
		ChronicleStream<KEY,VALUE> chronicleStream;
		try {
			chronicleStream = new ChronicleStream<>(name);
		} catch (IOException e) {
			if(Environment.alive()){
				Environment.get().routeError(e);
			}
			return null;
		}
		if(deleteOnExit){
			chronicleStream.deleteOnExit();
		}
		return chronicleStream;
	}

	/**
	 * A Persistent Map is a {@link reactor.rx.stream.MapStream} that fulfill both the Map and the Stream contract.
	 * Effectively it will act a store to be shared by consumers such as {@link reactor.rx.BiStreams} operators.
	 * Implementing the MapStream contract means that subscribers will have the ability to listen for put, putAll, clear
	 * and delete operations.
	 *
	 * A persistent map reader is read capable only and will use an internal cached map for read/write operations. This cache will be
	 * updated when an operation has been notified. Read/Write persistent map can be created with {@link #persistentMap(String)}.
	 *
	 * By default a persistentMap survives any application shutdown up to the latest written data. For a given name,
	 * the store is going to be located under the same default base location.
	 *
	 * @param name The identified name for the data store
	 * @param <KEY> the type of the key used to identify stored values
	 * @param <VALUE> the type of the payload stored
	 * @return a new MapStream that can be notified on any operation (put, putAll...)
	 */
	public static <KEY, VALUE> MapStream<KEY, VALUE> persistentMapReader(String name) {
		try {
			return new ChronicleReaderStream<>(name);
		} catch (IOException e) {
			if(Environment.alive()){
				Environment.get().routeError(e);
			}
			return null;
		}
	}

	/**
	 *
	 * Use the given {@link Codec} to decode any {@code SRC} data published by the {@code publisher} reference.
	 * Some codec might result into N signals for one SRC data.
	 *
	 * @param codec  the codec decoder is going to be used to scan the incoming {@code SRC} data
	 * @param publisher The data stream publisher we want to decode
	 * @param <SRC> The type of the origin value
	 * @param <IN> The type of the decoded value
	 * @return a Stream of decoded values
	 */
	public static <SRC, IN> Stream<IN> decode(Codec<SRC, IN, ?> codec, Publisher<? extends SRC> publisher){
		return new DecoderStream<>(codec, publisher);
	}


}
