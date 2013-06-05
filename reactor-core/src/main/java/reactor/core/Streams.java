/*
 * Copyright (c) 2011-2013 the original author or authors.
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

package reactor.core;

import java.util.Arrays;

/**
 * A public factory to build {@link Stream}
 *
 * @author Stephane Maldini
 */
public abstract class Streams {
	/**
	 * Create a delayed {@link Stream} with no initial state, ready to accept values.
	 *
	 * @return A {@link Stream.Spec} to further refine the {@link Stream} and then build it.
	 */
	public static <T> Stream.Spec<T> defer() {
		return new Stream.Spec<T>(null);
	}

	/**
	 * Create a delayed {@link Stream} with initial state, ready to accept values.
	 *
	 * @return A {@link Stream.Spec} to further refine the {@link Stream} and then build it.
	 */
	@SuppressWarnings("unchecked")
	public static <T> Stream.Spec<T> defer(T value) {
		return new Stream.Spec<T>(Arrays.asList(value));
	}

	/**
	 * Create a {@link Stream} from the given list of values.
	 *
	 * @param values The values to use.
	 * @param <T>    The type of the values.
	 * @return A {@link Stream.Spec} to further refine the {@link Stream} and then build it.
	 */
	public static <T> Stream.Spec<T> each(Iterable<T> values) {
		return new Stream.Spec<T>(values);
	}
}
