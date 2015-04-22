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

package reactor.io.queue;

import net.openhft.chronicle.*;
import net.openhft.chronicle.tools.ChronicleTools;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import reactor.fn.Function;
import reactor.io.buffer.Buffer;
import reactor.io.codec.Codec;
import reactor.io.codec.JavaSerializationCodec;

import javax.annotation.Nonnull;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.Collection;
import java.util.Iterator;
import java.util.concurrent.atomic.AtomicLong;

/**
 * A {@link QueuePersistor} implementation that uses a <a href="https://github.com/peter-lawrey/Java-Chronicle">Java
 * Chronicle</a> {@literal IndexedChronicle} to persist items in the queue.
 *
 * @author Jon Brisbin
 * @author Stephane Maldini
 * @see <a href="https://github.com/peter-lawrey/Java-Chronicle">Java Chronicle</a>
 */
public class ChronicleQueuePersistor<T> implements QueuePersistor<T> {

	private static final Logger LOG = LoggerFactory.getLogger(ChronicleQueuePersistor.class);

	private final Object     monitor = new Object();
	private final AtomicLong lastId  = new AtomicLong();
	private final AtomicLong size    = new AtomicLong(0);

	private final ExcerptTailer       exTrailer;
	private final ExcerptTailer       indexTrailer;
	private final ExcerptAppender     exAppender;
	private final String              basePath;
	private final Codec<Buffer, T, T> codec;
	private final boolean             deleteOnExit;
	private final Chronicle           data;


	/**
	 * Create an {@link ChronicleQueuePersistor} based on the given base path.
	 *
	 * @param basePath Directory in which to create the Chronicle.
	 * @throws IOException
	 */
	public ChronicleQueuePersistor(@Nonnull String basePath) throws IOException {
		this(basePath, new JavaSerializationCodec<T>(), false, false, ChronicleQueueBuilder.vanilla(basePath));
	}

	/**
	 * Create an {@link ChronicleQueuePersistor} based on the given base path, encoder and decoder. Optionally,
	 * passing {@literal false} to {@code clearOnStart} skips clearing the Chronicle on start for appending.
	 *
	 * @param basePath     Directory in which to create the Chronicle.
	 * @param codec        Codec to turn objects into {@link reactor.io.buffer.Buffer Buffers} and visa-versa.
	 * @param clearOnStart Whether or not to clear the Chronicle on start.
	 * @param deleteOnExit Whether or not to delete the Chronicle when the program exits.
	 * @param config       ChronicleConfig to use.
	 * @throws IOException
	 */
	public ChronicleQueuePersistor(@Nonnull String basePath,
	                               @Nonnull Codec<Buffer, T, T> codec,
	                               boolean clearOnStart,
	                               boolean deleteOnExit,
	                               @Nonnull ChronicleQueueBuilder config) throws IOException {
		this.basePath = basePath;
		this.codec = codec;
		this.deleteOnExit = deleteOnExit;

		data = config.build();
		lastId.set(data.lastWrittenIndex());
		if (clearOnStart) {
			data.clear();
		}

		ChronicleTools.warmup();

		Excerpt ex = data.createExcerpt();
		ex.skip(4);
		while (ex.nextIndex()) {
			int len = ex.readInt();
			size.incrementAndGet();
			ex.skip(len);
		}
		indexTrailer = data.createTailer();

		exTrailer = data.createTailer();
		exAppender = data.createAppender();
	}

	/**
	 * Close the underlying chronicles.
	 */
	@Override
	public void close() {
		try {
			data.close();

			if (deleteOnExit) {
				ChronicleTools.deleteOnExit(basePath);
			}
		} catch (IOException e) {
			throw new IllegalStateException(e.getMessage(), e);
		}
	}

	@Override
	public long lastId() {
		return lastId.get();
	}

	@Override
	public long size() {
		return size.get();
	}

	@Override
	public boolean hasNext() {
		return indexTrailer.nextIndex();
	}

	@Override
	public Long offer(@Nonnull T t) {
		synchronized (monitor) {
			Buffer buff = codec.apply(t);

			int len = buff.remaining();
			exAppender.startExcerpt(4 + len);
			exAppender.writeInt(len);
			exAppender.write(buff.byteBuffer());
			exAppender.finish();

			size.incrementAndGet();
			lastId.set(exAppender.lastWrittenIndex());
		}

		if (LOG.isTraceEnabled()) {
			LOG.trace("Offered {} to Chronicle at index {}, size {}", t, lastId(), size());
		}

		return lastId();
	}

	@Override
	public Long offerAll(@Nonnull Collection<T> t) {
		if(t.isEmpty()){
			return lastId();
		}

		Function<T, Buffer> encoder = codec;
		int elasticity = 16;
		Buffer buff;

		synchronized (monitor) {
			long count = 0l;
			for(T v : t) {
				buff = encoder.apply(v);
				int len = buff.remaining();
				if(count++ == 0){
					exAppender.startExcerpt(4 + elasticity*t.size()*len);
				}

				exAppender.writeInt(len);
				exAppender.write(buff.byteBuffer());

				size.incrementAndGet();
			}

			lastId.set(exAppender.lastWrittenIndex());
			exAppender.finish();
		}

		if (LOG.isTraceEnabled()) {
			LOG.trace("Offered {} to Chronicle at index {}, size {}", t, lastId(), size());
		}

		return lastId();
	}



	@Override
	public T get(Long id) {
		if (!exTrailer.index(id)) {
			return null;
		}
		return read(exTrailer);
	}

	public Codec<Buffer, T, T> codec() {
		return codec;
	}

	public ExcerptTailer reader() {
		return indexTrailer;
	}

	@Override
	public T remove() {
		synchronized (monitor) {
			T obj = read(indexTrailer);
			size.decrementAndGet();
			return obj;
		}


	}

	@Override
	public Iterator<T> iterator() {
		return new Iterator<T>() {
			public boolean hasNext() {
				return ChronicleQueuePersistor.this.hasNext();
			}

			@SuppressWarnings("unchecked")
			@Override
			public T next() {
				return read(indexTrailer);
			}

			@Override
			public void remove() {
				throw new IllegalStateException("This Iterator is read-only.");
			}
		};
	}

	@SuppressWarnings("unchecked")
	private T read(ExcerptCommon ex) {
		try {
			int len = ex.readInt();
			ByteBuffer bb = ByteBuffer.allocate(len);
			ex.read(bb);
			bb.flip();
			return codec.decoder(null).apply(new Buffer(bb));
		} finally {
			ex.finish();
		}
	}

}
