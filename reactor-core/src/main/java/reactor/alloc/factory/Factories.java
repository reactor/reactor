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

package reactor.alloc.factory;

/**
 * Helper class for creating object factories.
 *
 * @author Jon Brisbin
 * @since 1.1
 */
public abstract class Factories {

	private static final int DEFAULT_BATCH_SIZE = 2 * 1024;

	protected Factories() {
	}

	/**
	 * Create a {@link reactor.alloc.factory.BatchFactorySupplier} that supplies instances of the given type, which
	 * are created by invoked their no-arg constructor. An error will occur at creation if the type to be pooled has no
	 * zero-arg constructor (it can be private or protected; it doesn't have to be public, but it must exist).
	 *
	 * @param type
	 * 		the type to be pooled
	 * @param <T>
	 * 		the type of the pooled objects
	 *
	 * @return a {@link reactor.alloc.factory.BatchFactorySupplier} to supply instances of {@literal type}.
	 */
	public static <T> BatchFactorySupplier<T> create(Class<T> type) {
		return create(DEFAULT_BATCH_SIZE, type);
	}

	/**
	 * Create a {@link reactor.alloc.factory.BatchFactorySupplier} that supplies instances of the given type, which
	 * are created by invoked their no-arg constructor. An error will occur at creation if the type to be pooled has no
	 * zero-arg constructor (it can be private or protected; it doesn't have to be public, but it must exist).
	 *
	 * @param size
	 * 		size of the pool
	 * @param type
	 * 		the type to be pooled
	 * @param <T>
	 * 		the type of the pooled objects
	 *
	 * @return a {@link reactor.alloc.factory.BatchFactorySupplier} to supply instances of {@literal type}.
	 */
	public static <T> BatchFactorySupplier<T> create(int size, Class<T> type) {
		return new BatchFactorySupplier<T>(size, new NoArgConstructorFactory<T>(type));
	}

}
