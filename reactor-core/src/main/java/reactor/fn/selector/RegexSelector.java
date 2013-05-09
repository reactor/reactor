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

import reactor.fn.Selector;

import java.util.regex.Pattern;

/**
 * A {@link reactor.fn.Selector} implementation based on the given regular expression. Parses it into a {@link Pattern} for
 * efficient matching against other {@link reactor.fn.Selector}s.
 * <p/>
 * An example of creating a regex Selector would be:
 * <p/>
 * <code>Selectors.R("event([0-9]+)")</code>
 * <p/>
 * This would match other selectors like:
 * <p/>
 * <code>Fn.$("event1")</code>
 * <code>Fn.$("event23")</code>
 * <code>Fn.$("event9")</code>
 *
 * @author Jon Brisbin
 * @author Andy Wilkinson
 */
public class RegexSelector extends BaseSelector<Pattern> {

	public RegexSelector(String pattern) {
		super(Pattern.compile(pattern));
	}

	@Override
	public boolean matches(Selector s2) {
		return getObject().matcher(s2.getObject().toString()).matches();
	}
}
