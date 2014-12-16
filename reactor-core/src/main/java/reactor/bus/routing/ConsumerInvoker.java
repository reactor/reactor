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

package reactor.bus.routing;

import reactor.core.support.Supports;
import reactor.fn.Consumer;

/**
 * Implementations of this interface are responsible for invoking a {@link reactor.fn.Consumer} that may take into account
 * automatic argument conversion, return values, and other situations that might be specific to a particular use-case.
 *
 * @author Jon Brisbin
 * @author Stephane Maldini
 */
public interface ConsumerInvoker extends Supports<Consumer<?>> {

	/**
	 * Invoke a {@link reactor.fn.Consumer}.
	 *
	 * @param consumer     The {@link reactor.fn.Consumer} to invoke.
	 * @param returnType   If the {@link reactor.fn.Consumer} also implements a value-returning type, convert it to this type before
	 *                     returning.
	 * @param possibleArg Possible argument that may or may not be used.
	 * @param <T>          The return type.
	 * @return A result if available, or {@literal null} otherwise.
	 * @throws Exception
	 */
	<T> T invoke(Consumer<?> consumer,
							 Class<? extends T> returnType,
							 Object possibleArg) throws Exception;

}
