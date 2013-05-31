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

/**
 * @author Stephane Maldini
 */
public class Composables {
	/**
	 * Create a delayed {@link reactor.core.Composable} with no initial state, ready to accept values.
	 *
	 * @return A {@link reactor.core.Composable.Spec} to further refine the {@link reactor.core.Composable} and then build it.
	 */
	public static <T> Composable.Spec<T> defer() {
		return new Composable.Spec<T>(null);
	}

	/**
	 * Create a {@link reactor.core.Composable} from the given list of values.
	 *
	 * @param values The values to use.
	 * @param <T>    The type of the values.
	 * @return A {@link reactor.core.Composable.Spec} to further refine the {@link reactor.core.Composable} and then build it.
	 */
	public static <T> Composable.Spec<T> each(Iterable<T> values) {
		return new Composable.Spec<T>(values);
	}
}
