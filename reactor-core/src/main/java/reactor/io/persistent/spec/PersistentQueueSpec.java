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

package reactor.io.persistent.spec;

import net.openhft.chronicle.ChronicleQueueBuilder;
import reactor.fn.Supplier;
import reactor.io.buffer.Buffer;
import reactor.io.codec.Codec;
import reactor.io.persistent.ChronicleQueuePersistor;
import reactor.io.persistent.PersistentQueue;

import java.io.IOException;

/**
 * Helper spec to create a {@link PersistentQueue} instance.
 *
 * @author Jon Brisbin
 */
public class PersistentQueueSpec<T> implements Supplier<PersistentQueue<T>> {

	public static String DEFAULT_BASE_PATH = System.getProperty("java.io.tmpdir") + "/persistent-queue";

	private String  basePath       = DEFAULT_BASE_PATH;
	private boolean clearOnStart   = false;
	private boolean deleteOnExit   = false;
	private long    indexBlockSize = 16L << 20;
	private boolean sync           = false;
	private long    dataBlockSize  = 64L << 20;
	private Codec<Buffer, T, T> codec;

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
		this.dataBlockSize = size;
		return this;
	}

	public PersistentQueueSpec<T> sync(boolean synchronousMode) {
		this.sync = synchronousMode;
		return this;
	}

	public PersistentQueueSpec<T> indexBlockSize(int indexBlockSize) {
		this.indexBlockSize = indexBlockSize;
		return this;
	}

	@Override
	public PersistentQueue<T> get() {
		try {
			return new PersistentQueue<T>(new ChronicleQueuePersistor<T>(basePath,
					codec,
					clearOnStart,
					deleteOnExit,
					ChronicleQueueBuilder.vanilla(basePath)
							.indexBlockSize(indexBlockSize)
							.synchronous(sync)
							.dataBlockSize(dataBlockSize)));
		} catch (IOException e) {
			throw new IllegalStateException(e.getMessage(), e);
		}
	}

}
