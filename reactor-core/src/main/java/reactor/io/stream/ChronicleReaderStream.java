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

package reactor.io.stream;

import net.openhft.chronicle.Chronicle;
import net.openhft.chronicle.ChronicleConfig;
import net.openhft.chronicle.ExcerptTailer;
import net.openhft.chronicle.IndexedChronicle;
import org.reactivestreams.Subscriber;
import reactor.core.support.NamedDaemonThreadFactory;
import reactor.io.buffer.Buffer;
import reactor.io.codec.Codec;
import reactor.io.queue.spec.PersistentQueueSpec;
import reactor.rx.Stream;
import reactor.rx.stream.MapStream;
import reactor.rx.subscription.PushSubscription;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.Collection;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.atomic.AtomicIntegerFieldUpdater;
import java.util.concurrent.locks.LockSupport;

import static reactor.rx.stream.MapStream.Operation.*;


/**
 * Implementation of a {@link reactor.core.Dispatcher} that uses a {@link net.openhft.chronicle.IndexedChronicle} to
 * queue tasks to execute.
 * <p>
 * Original design on https://github.com/peter-lawrey/Java-Chronicle (MapWrapper and Datastore)
 *
 * @author Stephane Maldini
 */
public class ChronicleReaderStream<K, V> extends MapStream<K, V> {


	//private final Logger log = LoggerFactory.getLogger(getClass());
	private final ExecutorService executor;
  protected final String name;
	protected final Codec<Buffer, K, K> keyCodec;
	protected final Codec<Buffer, V, V> valueCodec;
	protected final Chronicle           chronicle;
	protected final Map<K, V> localCache = new ConcurrentHashMap<>();

	protected volatile int consumers = 0;

	protected static final AtomicIntegerFieldUpdater<ChronicleReaderStream> CONSUMER_UPDATER =
			AtomicIntegerFieldUpdater
					.newUpdater(ChronicleReaderStream.class, "consumers");


	/**
	 * Create a chronicle dispatcher
	 *
	 * @param name The name of the dispatcher.
	 */
	public ChronicleReaderStream(String name) throws
			FileNotFoundException {
		this(name,
				new IndexedChronicle(PersistentQueueSpec.DEFAULT_BASE_PATH + "/" + name, ChronicleConfig.DEFAULT.clone())
		);
	}

	/**
	 * Create a chronicle dispatcher
	 *
	 * @param name      The name of the dispatcher.
	 * @param chronicle The chronicle instance to use
	 */
	public ChronicleReaderStream(String name, Chronicle chronicle) {
		this(name,
				chronicle,
				null,
				null
		);
	}

	/**
	 * @param name       The name of the dispatcher
	 * @param chronicle  The chronicle instance to use
	 * @param keyCodec   The codec to encode/decode key, if null will rely on Chronicle serialization
	 * @param valueCodec The codec to encode/decode values, if null will rely on Chronicle serialization
	 */
	public ChronicleReaderStream(String name, final Chronicle chronicle,
	                             Codec<Buffer, K, K> keyCodec, Codec<Buffer, V, V> valueCodec) {

		this.executor = Executors.newSingleThreadExecutor(new NamedDaemonThreadFactory(name));
		this.keyCodec = keyCodec;
		this.valueCodec = valueCodec;
		this.chronicle = chronicle;
		this.name = name;
	}

	@Override
	public void subscribe(Subscriber<? super MapStream.Signal<K, V>> s) {
		CONSUMER_UPDATER.incrementAndGet(this);
		s.onSubscribe(new ChronicleSubscription(this, s));
	}

	class ChronicleSubscription extends PushSubscription<MapStream.Signal<K, V>> {

		final ExcerptTailer readExcerpt;
		final MapStream.MutableSignal<K, V> signalContainer = new MapStream.MutableSignal<>();

		public ChronicleSubscription(Stream<MapStream.Signal<K, V>> publisher,
		                             Subscriber<? super MapStream.Signal<K, V>> subscriber) {
			super(publisher, subscriber);
			final ExcerptTailer tailer;
			try {
				tailer = chronicle.createTailer();
			} catch (IOException e) {
				subscriber.onError(e);
				throw new IllegalStateException(e);
			}
			readExcerpt = tailer;
		}

		@Override
		public void cancel() {
			super.cancel();
			if (CONSUMER_UPDATER.decrementAndGet(ChronicleReaderStream.this) == 0) {
				/*try {
					chronicle.close();
				} catch (IOException e) {
					subscriber.onError(e);
				}*/
			}
		}

		@Override
		protected void onRequest(final long n) {
			ChronicleReaderStream.this.executor.execute(new Runnable() {
				@Override
				public void run() {
					try {
						boolean found;
						long i = 0;
						while ((n == Long.MAX_VALUE || i < n) && terminated == 0) {
							found = readExcerpt.nextIndex();
							if (found) {
								i++;
								readExcerpt();
								signalContainer.op(put);
								signalContainer.key(null);
								signalContainer.value(null);
							} else {
								LockSupport.parkNanos(1l); //TODO expose
							}
						}
					} catch (Throwable t) {
						subscriber.onError(t);
					}
				}
			});
		}

		private void readExcerpt() {
			long position = readExcerpt.position();
			MapStream.Operation event = readExcerpt.readEnum(MapStream.Operation.class);
			if (event == null) {
				readExcerpt.position(position);
				//todo log unmatching entry?
				return;
			}

			try {
				switch (event) {
					case put: {
						onExcerptPut(readExcerpt);
						return;

					}
					case putAll: {
						int count = readExcerpt.readInt();
						for (int i = 0; i < count; i++)
							onExcerptPut(readExcerpt);
						return;
					}
					case remove: {
						signalContainer.op(remove);
						signalContainer.key(readKey(readExcerpt));
						break;
					}

					case clear: {
						signalContainer.op(clear);
						break;
					}
				}
				sync(signalContainer);
				subscriber.onNext(signalContainer);
			} catch (Exception e) {
				subscriber.onError(e);
			}
		}

		private void onExcerptPut(ExcerptTailer excerpt) {
			K key = readKey(excerpt);
			V value = readValue(excerpt);
			signalContainer.op(put);
			signalContainer.key(key);
			signalContainer.value(value);
			sync(signalContainer);
			subscriber.onNext(signalContainer);
		}

		@SuppressWarnings("unchecked")
		private V readValue(ExcerptTailer excerpt) {
			if (valueCodec == null) {
				return (V) excerpt.readObject();
			} else {
				int len = excerpt.readInt();
				ByteBuffer bb = ByteBuffer.allocate(len);
				excerpt.read(bb);
				bb.flip();
				return valueCodec.decoder(null).apply(new Buffer(bb));
			}
		}

		@SuppressWarnings("unchecked")
		private K readKey(ExcerptTailer excerpt) {
			if (keyCodec == null) {
				return (K) excerpt.readObject();
			} else {
				int len = excerpt.readInt();
				ByteBuffer bb = ByteBuffer.allocate(len);
				excerpt.read(bb);
				bb.flip();
				return keyCodec.decoder(null).apply(new Buffer(bb));
			}
		}

		// reload, and synchronise the map.
		public void sync(MapStream.Signal<K, V> signal) {
			switch (signal.op()) {
				case put: {
					localCache.put(signal.key(), signal.value());
					break;

				}
				case putAll: {
					localCache.put(signal.key(), signal.value());
					break;
				}
				case remove: {
					localCache.remove(signal.key());
					break;
				}

				case clear: {
					localCache.clear();
				}
			}
		}

	}

	//// Map
	@Override
	public boolean containsKey(Object key) {
		return localCache.containsKey(key);
	}

	@Override
	public boolean containsValue(Object value) {
		return localCache.containsValue(value);
	}

	@Override
	public Set<Map.Entry<K, V>> entrySet() {
		return localCache.entrySet();
	}

	@Override
	public boolean equals(Object o) {
		return localCache.equals(o);
	}

	@Override
	public V get(Object key) {
		return localCache.get(key);
	}

	@Override
	public int hashCode() {
		return localCache.hashCode();
	}

	@Override
	public boolean isEmpty() {
		return localCache.isEmpty();
	}


	@Override
	public Set<K> keySet() {
		return localCache.keySet();
	}

	@Override
	public void clear() {
		throw new UnsupportedOperationException();
	}

	@Override
	public V put(K key, V value) {
		throw new UnsupportedOperationException();
	}

	@Override
	public void putAll(Map<? extends K, ? extends V> m) {
		throw new UnsupportedOperationException();
	}

	@Override
	public V remove(Object key) {
		throw new UnsupportedOperationException();
	}

	@Override
	public int size() {
		return localCache.size();
	}

	public Map<K, V> localCache() {
		return localCache;
	}

	public Chronicle chronicle() {
		return chronicle;
	}

	public Codec<Buffer, K, K> keyCodec() {
		return keyCodec;
	}

	public Codec<Buffer, V, V> valueCodec() {
		return valueCodec;
	}

	@Override
	public String toString() {
		return localCache.toString()+"{name="+name+"}";
	}


	@Override
	public Collection<V> values() {
		return localCache.values();
	}


}

