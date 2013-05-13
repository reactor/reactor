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

package reactor.fn.selector;

import java.util.regex.Matcher;
import java.util.regex.Pattern;

import reactor.fn.Event;

/**
 * A {@link reactor.fn.Selector} implementation based on the given regular expression. Parses it into a {@link Pattern}
 * for efficient matching against keys.
 * <p/>
 * An example of creating a regex Selector would be:
 * <p/>
 * <code>Fn.R("event([0-9]+)")</code>
 * <p/>
 * This would match keys like:
 * <p/>
 * <code>"event1"</code>, <code>"event23"</code>, or <code>"event9"</code>
 *
 * @author Jon Brisbin
 * @author Andy Wilkinson
 */
public class RegexSelector extends BaseSelector<Pattern> {

	public RegexSelector(String pattern) {
		super(Pattern.compile(pattern));
	}

	@Override
	public boolean matches(Object key) {
		return getObject().matcher(key.toString()).matches();
	}

	@Override
	public <T> RegexSelector setHeaders(Object other, Event<T> ev) {
		Matcher m = getObject().matcher(other.toString());
		int groups = m.groupCount();
		for (int i = 1; i <= groups; i++) {
			String name = "group" + i;
			String value = m.group(i);
			ev.getHeaders().set(name, value);
		}
		return this;
	}

}
