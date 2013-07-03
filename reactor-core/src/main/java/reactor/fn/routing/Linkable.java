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

package reactor.fn.routing;

/**
 * Simple abstraction to provide linking components together.
 *
 * @param <T> the type that can be linked
 *
 * @author Jon Brisbin
 */
public interface Linkable<T> {

	/**
	 * Link components together.
	 *
	 * @param t
	 * 		Array of components to link to this parent.
	 *
	 * @return {@literal this}
	 */
	Linkable<T> link(T t);

	/**
	 * Unlink components.
	 *
	 * @param t
	 * 		Component to unlink when this parent.
	 *
	 * @return {@literal this}
	 */
	Linkable<T> unlink(T t);

}
