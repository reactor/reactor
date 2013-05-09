/*
 * Copyright 2002-2013 the original author or authors.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *       http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND,
 * either express or implied.
 *
 * See the License for the specific language governing permissions
 * and limitations under the License.
 */

package reactor.fn;

/**
 * Match two {@link Selector}s like normal, but only do that check if the two have at least one tag in common.
 *
 * @author Jon Brisbin
 * @author Andy Wilkinson
 */
public class TagAwareSelectionStrategy implements SelectionStrategy {

	@Override
	public boolean matches(Selector sel1, Selector sel2) {
		for (String s2 : sel2.getTags()) {
			for (String s1 : sel1.getTags()) {
				if (s1.equals(s2) && sel1.matches(sel2)) {
					return true;
				}
			}
		}
		return false;
	}

	@Override
	public boolean supports(Selector sel) {
		return !sel.getTags().isEmpty();
	}

}
