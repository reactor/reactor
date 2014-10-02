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

import reactor.function.Supplier;

import java.lang.reflect.Constructor;

/**
 * A {@link reactor.function.Supplier} implementation that simply instantiates objects
 * using reflection from a no-arg constructor.
 *
 * @author Jon Brisbin
 * @since 1.1
 */
public class NoArgConstructorFactory<T> implements Supplier<T> {

	private final Class<T>       type;
	private final Constructor<T> ctor;

	public NoArgConstructorFactory(Class<T> type) {
		this.type = type;
		try {
			this.ctor = type.getConstructor();
			this.ctor.setAccessible(true);
		} catch(NoSuchMethodException e) {
			throw new IllegalArgumentException(e.getMessage(), e);
		}
	}

	@Override
	public T get() {
		try {
			return ctor.newInstance();
		} catch(Exception e) {
			throw new RuntimeException(e.getMessage(), e);
		}
	}

}
