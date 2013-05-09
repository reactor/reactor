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
 * Implementations of this interface will provide matching of one {@link Selector} to another.
 *
 * @author Jon Brisbin
 * @uathor Andy Wilkinson
 */
public interface SelectionStrategy extends Supports<Selector> {

	/**
	 * Indicates whether or not the left-hand Selector matches the right-hand Selector.
	 *
	 * @param leftHand The left-hand selector
	 * @param rightHand The right-hand selector
	 *
	 * @return {@code true} if the Selectors match, otherwise {@code false}.
	 */
	boolean matches(Selector leftHand, Selector rightHand);

}
