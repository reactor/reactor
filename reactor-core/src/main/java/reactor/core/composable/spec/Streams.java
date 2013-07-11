/*
 * Copyright (c) 2011-2013 GoPivotal, Inc. All Rights Reserved.
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

package reactor.core.composable.spec;

import java.util.Arrays;
import java.util.Collection;

import reactor.core.composable.Deferred;
import reactor.core.composable.Stream;
import reactor.event.dispatch.SynchronousDispatcher;

/**
 * A public factory to build {@link Stream Streams} that use a {@link SynchronousDispatcher}.
 *
 * @author Stephane Maldini
 * @author Jon Brisbin
 */
public abstract class Streams {

	/**
	 * Build a deferred {@literal Stream}, ready to accept values.
	 *
	 * @param <T> the type of values passing through the {@literal Stream}
	 *
	 * @return a new {@link DeferredStreamSpec}
	 */
	public static <T> DeferredStreamSpec<T> defer() {
		return new DeferredStreamSpec<T>();
	}

	/**
	 * Build a deferred {@literal Stream} that will implicitly {@link Deferred#accept(Object)}
	 * the given value whenever the {@link reactor.core.composable.Stream#resolve()} function
	 * is invoked.
	 *
	 * @param value The value to {@code accept()}
	 * @param <T>   type of the value
	 *
	 * @return a {@link DeferredStreamSpec} based on the given value
	 */
	@SuppressWarnings("unchecked")
	public static <T> DeferredStreamSpec<T> defer(T value) {
		return defer(Arrays.asList(value));
	}

	/**
	 * Build a deferred {@literal Stream} that will implicitly {@link Deferred#accept(Object)}
	 * the given values whenever the {@link reactor.core.composable.Stream#resolve()} function
	 * is invoked. If the {@code values} are a {@code Collection} the Stream's batch size will
	 * be set to the Collection's {@link Collection#size()}.
	 *
	 * @param values The values to {@code accept()}
	 * @param <T>    type of the values
	 *
	 * @return a {@link DeferredStreamSpec} based on the given values
	 */
	public static <T> DeferredStreamSpec<T> defer(Iterable<T> values) {
		int batchSize = (values instanceof Collection ? ((Collection<?>) values).size() : -1);
		return new DeferredStreamSpec<T>().each(values).batchSize(batchSize);
	}

}
