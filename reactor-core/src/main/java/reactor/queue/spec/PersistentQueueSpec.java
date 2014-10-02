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

package reactor.queue.spec;

import net.openhft.chronicle.ChronicleConfig;
import reactor.function.Supplier;
import reactor.io.Buffer;
import reactor.io.encoding.Codec;
import reactor.queue.IndexedChronicleQueuePersistor;
import reactor.queue.PersistentQueue;

import java.io.IOException;

/**
 * Helper spec to create a {@link PersistentQueue} instance.
 *
 * @author Jon Brisbin
 */
public class PersistentQueueSpec<T> implements Supplier<PersistentQueue<T>> {

	private String  basePath     = System.getProperty("java.io.tmpdir") + "/persistent-queue";
	private boolean clearOnStart = false;
	private boolean deleteOnExit = false;
	private Codec<Buffer, T, T> codec;
	private ChronicleConfig config = ChronicleConfig.DEFAULT.clone();

	public PersistentQueueSpec<T> codec(Codec<Buffer, T, T> codec) {
		this.codec = codec;
		return this;
	}

	public PersistentQueueSpec<T> basePath(String basePath) {
		this.basePath = basePath;
		return this;
	}

	public PersistentQueueSpec<T> clearOnStart(boolean clearOnStart) {
		this.clearOnStart = clearOnStart;
		return this;
	}

	public PersistentQueueSpec<T> deleteOnExit(boolean deleteOnExit) {
		this.deleteOnExit = deleteOnExit;
		return this;
	}

	public PersistentQueueSpec<T> cacheLineSize(int size) {
		config.cacheLineSize(size);
		return this;
	}

	public PersistentQueueSpec<T> dataBlockSize(int size) {
		config.dataBlockSize(size);
		return this;
	}

	public PersistentQueueSpec<T> indexFileCapacity(int size) {
		config.indexFileCapacity(size);
		return this;
	}

	public PersistentQueueSpec<T> indexFileCapacity(boolean synchronousMode) {
		config.synchronousMode(synchronousMode);
		return this;
	}

	public PersistentQueueSpec<T> indexFileExcerpts(int excerpts) {
		config.indexFileExcerpts(excerpts);
		return this;
	}

	public PersistentQueueSpec<T> minimiseFootprint(boolean minimiseFootprint) {
		config.minimiseFootprint(minimiseFootprint);
		return this;
	}

	@Override
	public PersistentQueue<T> get() {
		try {
			return new PersistentQueue<T>(new IndexedChronicleQueuePersistor<T>(basePath,
																																					codec,
																																					clearOnStart,
																																					deleteOnExit,
																																					config));
		} catch(IOException e) {
			throw new IllegalStateException(e.getMessage(), e);
		}
	}

}
