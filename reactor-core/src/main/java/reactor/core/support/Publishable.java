/*
 * Copyright (c) 2011-2015 Pivotal Software Inc, All Rights Reserved.
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
package reactor.core.support;

import org.reactivestreams.Publisher;

/**
 * A component that is linked to a source {@link Publisher}.
 * Useful to traverse from left to right a pipeline of reactive actions implementing this interface.
 *
 * @author Stephane Maldini
 * @since 2.1
 */
public interface Publishable<T> {

	/**
	 * Return a source of data
	 */
	Publisher<T> upstream();

}
