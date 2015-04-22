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

package reactor.io.queue.spec;

import net.openhft.chronicle.ChronicleQueueBuilder;
import reactor.fn.Supplier;
import reactor.io.buffer.Buffer;
import reactor.io.codec.Codec;
import reactor.io.queue.PersistentQueue;
import reactor.io.queue.VanillaChronicleQueuePersistor;

import java.io.IOException;

/**
 * Helper spec to create a {@link PersistentQueue} instance.
 *
 * @author Jon Brisbin
 */
public class PersistentQueueSpec<T> implements Supplier<PersistentQueue<T>> {

	public static String DEFAULT_BASE_PATH = System.getProperty("java.io.tmpdir") + "/persistent-queue";

	private String  basePath     = DEFAULT_BASE_PATH;
	private boolean clearOnStart = false;
	private boolean deleteOnExit = false;
	private Codec<Buffer, T, T> codec;
	private ChronicleQueueBuilder.VanillaChronicleQueueBuilder config = ChronicleQueueBuilder.vanilla(basePath);

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

	public PersistentQueueSpec<T> dataBlockSize(int size) {
		config.dataBlockSize(size);
		return this;
	}

	public PersistentQueueSpec<T> sync(boolean synchronousMode) {
		config.synchronous(synchronousMode);
		return this;
	}

	public PersistentQueueSpec<T> indexBlockSize(int excerpts) {
		config.indexBlockSize(excerpts);
		return this;
	}

	@Override
	public PersistentQueue<T> get() {
		try {
			return new PersistentQueue<T>(new VanillaChronicleQueuePersistor<T>(basePath,
					codec,
					clearOnStart,
					deleteOnExit,
					config));
		} catch(IOException e) {
			throw new IllegalStateException(e.getMessage(), e);
		}
	}

}
