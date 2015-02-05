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
import net.openhft.chronicle.ChronicleQueueBuilder;
import net.openhft.chronicle.ExcerptAppender;
import net.openhft.chronicle.tools.ChronicleTools;
import reactor.io.buffer.Buffer;
import reactor.io.codec.Codec;
import reactor.io.queue.spec.PersistentQueueSpec;
import reactor.rx.stream.MapStream;

import java.io.IOException;
import java.util.Map;

import static reactor.rx.stream.MapStream.Operation.*;

/**
 * @author Stephane Maldini
 * @since 2.0
 */
public final class ChronicleStream<K, V>
		extends ChronicleReaderStream<K, V> {

	private static final int DEFAULT_MESSAGE_SIZE_HINT = 1024 * 1024; // 1MB

	private final int messageSizeHint;

	public ChronicleStream(String name) throws IOException {
		this(name, DEFAULT_MESSAGE_SIZE_HINT);
	}

	public ChronicleStream(String name, int messageSizeHint) throws IOException {
		this(name,
				messageSizeHint,
				ChronicleQueueBuilder.indexed(PersistentQueueSpec.DEFAULT_BASE_PATH, name).build()
		);
	}

	public ChronicleStream(String name, int messageSizeHint, Chronicle chronicle) {
		this(name, messageSizeHint, chronicle, null, null);
	}

	public ChronicleStream(String name, int messageSizeHint, Chronicle chronicle,
	                       Codec<Buffer, K, K> keyCodec, Codec<Buffer, V, V> valueCodec) {
		super(name, chronicle, keyCodec, valueCodec);
		this.messageSizeHint = messageSizeHint;
	}

	@Override
	public V put(K key, V value) {
		V previous = localCache.put(key, value);
		if (sameOrNotEqual(previous, value))
			writePut(key, previous, value);
		return previous;
	}

	public void deleteOnExit() {
		ChronicleTools.deleteOnExit(chronicle.name());
	}

	protected boolean sameOrNotEqual(V previous, V value) {
		return previous == value || previous == null || !previous.equals(value);
	}

	@Override
	public void putAll(Map<? extends K, ? extends V> m) {
		writePutAll(m);
	}

	@Override
	public V remove(Object key) {
		V value = localCache.remove(key);
		if (value != null)
			writeRemove(key);
		return value;
	}

	@Override
	public int size() {
		return localCache.size();
	}

	@Override
	public void clear() {
		ExcerptAppender writeExcerpt = getExcerpt(16, clear);
		writeExcerpt.finish();
	}

	@SuppressWarnings("unchecked")
	protected void writeRemove(Object key) {
			ExcerptAppender excerpt = getExcerpt(messageSizeHint, remove);
			writeKey(excerpt, (K) key);
			excerpt.finish();
	}

	protected void writePut(K key, V previous, V value) {
			ExcerptAppender excerpt = getExcerpt(messageSizeHint, put);
			writeKey(excerpt, key);
			writeValue(excerpt, value);
			excerpt.finish();
	}

	protected void writePutAll(Map<? extends K, ? extends V> m) {
			ExcerptAppender excerpt = getExcerpt(m.size() * messageSizeHint, putAll);
			long pos = excerpt.position();
			excerpt.writeInt(0); // place holder for the actual size.
			int count = 0;
			for (Map.Entry<? extends K, ? extends V> entry : m.entrySet()) {
				K key = entry.getKey();
				V value = entry.getValue();
				V previous = localCache.put(key, value);
				if (sameOrNotEqual(previous, value)) {
					writeKey(excerpt, key);
					writeValue(excerpt, value);
					count++;
				}
			}
			excerpt.writeInt(pos, count);
			excerpt.finish();
	}

	private void writeValue(ExcerptAppender excerpt, V value) {
		if (valueCodec == null) {
			excerpt.writeObject(value);
		} else {
			Buffer buff = valueCodec.apply(value);
			int size = buff.remaining();
			excerpt.writeInt(size);
			excerpt.write(buff.byteBuffer());
		}
	}

	private void writeKey(ExcerptAppender excerpt, K key) {
		if (keyCodec == null) {
			excerpt.writeObject(key);
		} else {
			Buffer buff = keyCodec.apply(key);
			int size = buff.remaining();
			excerpt.writeInt(size);
			excerpt.write(buff.byteBuffer());
		}
	}

	private ExcerptAppender getExcerpt(int maxSize, MapStream.Operation event) {
		ExcerptAppender writeExcerpt;
		try {
			writeExcerpt = chronicle.createAppender();
		} catch (IOException e) {
			throw new IllegalStateException(e);
		}
		writeExcerpt.startExcerpt(maxSize + 2 + event.name().length());
		writeExcerpt.writeEnum(event);
		return writeExcerpt;
	}
}
