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

package reactor.tuple;

/**
 * A tuple that holds a single value
 *
 * @param <T1> The type held by this tuple
 *
 * @author Jon Brisbin
 */
public class Tuple1<T1> extends Tuple {

	private static final long serialVersionUID = -1467756857377152573L;

	Tuple1(Object... values) {
		super(values);
	}

	/**
	 * Type-safe way to get the first object of this {@link Tuple}.
	 *
	 * @return The first object, cast to the correct type.
	 */
	@SuppressWarnings("unchecked")
	public T1 getT1() {
		return (T1) get(0);
	}

}
