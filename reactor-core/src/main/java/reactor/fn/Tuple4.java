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

package reactor.fn;

/**
 * @author Jon Brisbin
 */
public class Tuple4<T1, T2, T3, T4> extends Tuple3<T1, T2, T3> {

	Tuple4(Object... values) {
		super(values);
	}

	/**
	 * Type-safe way to get the fourth object of this {@link reactor.fn.Tuple}.
	 *
	 * @return The fourth object, cast to the correct type.
	 */
	@SuppressWarnings("unchecked")
	public T4 getT4() {
		return (T4) get(3);
	}

}
