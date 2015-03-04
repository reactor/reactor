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

package reactor.bus.selector;

import reactor.fn.Predicate;

import java.util.Set;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * Helper methods for creating {@link Selector}s.
 *
 * @author Andy Wilkinson
 * @author Jon Brisbin
 * @author Stephane Maldini
 *
 */
public abstract class Selectors {

	private static final AtomicInteger HASH_CODES = new AtomicInteger(Integer.MIN_VALUE);

	/**
	 * Creates an anonymous {@link Selector}.
	 *
	 * @return a new Selector
	 *
	 * @see ObjectSelector
	 */
	public static Selector anonymous() {
		Object obj = new AnonymousKey();
		return $(obj);
	}

	/**
	 * A short-hand alias for {@link Selectors#anonymous()}.
	 * <p/>
	 * Creates an anonymous {@link Selector}.
	 *
	 * @return a new Selector
	 *
	 * @see ObjectSelector
	 */
	public static Selector $() {
		return anonymous();
	}

	/**
	 * Creates a {@link Selector} based on the given object.
	 *
	 * @param obj
	 * 		The object to use for matching
	 *
	 * @return The new {@link ObjectSelector}.
	 *
	 * @see ObjectSelector
	 */
	public static <T> Selector object(T obj) {
		return new ObjectSelector<T>(obj);
	}

	/**
	 * A short-hand alias for {@link Selectors#object}.
	 * <p/>
	 * Creates a {@link Selector} based on the given object.
	 *
	 * @param obj
	 * 		The object to use for matching
	 *
	 * @return The new {@link ObjectSelector}.
	 *
	 * @see ObjectSelector
	 */
	public static <T> Selector $(T obj) {
		return object(obj);
	}

	/**
	 * Creates a {@link Selector} based on the given string format and arguments.
	 *
	 * @param fmt
	 * 		The {@code String.format} style format specification
	 * @param args
	 * 		The format args
	 *
	 * @return The new {@link ObjectSelector}.
	 *
	 * @see ObjectSelector
	 * @see String#format(String, Object...)
	 */
	public static Selector $(String fmt, Object... args) {
		return object(String.format(fmt, args));
	}

	/**
	 * Creates a {@link Selector} based on the given regular expression.
	 *
	 * @param regex
	 * 		The regular expression to compile and use for matching
	 *
	 * @return The new {@link RegexSelector}.
	 *
	 * @see RegexSelector
	 */
	public static Selector regex(String regex) {
		return new RegexSelector(regex);
	}

	/**
	 * A short-hand alias for {@link Selectors#regex(String)}.
	 * <p/>
	 * Creates a {@link Selector} based on the given regular expression.
	 *
	 * @param regex
	 * 		The regular expression to compile and use for matching
	 *
	 * @return The new {@link RegexSelector}.
	 *
	 * @see RegexSelector
	 */
	public static Selector R(String regex) {
		return new RegexSelector(regex);
	}

	/**
	 * Creates a {@link Selector} based on the given class type that matches objects whose type is
	 * assignable according to {@link Class#isAssignableFrom(Class)}.
	 *
	 * @param supertype
	 * 		The supertype to use for matching
	 *
	 * @return The new {@link ClassSelector}.
	 *
	 * @see ClassSelector
	 */
	public static ClassSelector type(Class<?> supertype) {
		return new ClassSelector(supertype);
	}

	/**
	 * A short-hand alias for {@link Selectors#type(Class)}.
	 * <p/>
	 * Creates a {@link Selector} based on the given class type that matches objects whose type is
	 * assignable according to {@link Class#isAssignableFrom(Class)}.
	 *
	 * @param supertype
	 * 		The supertype to compare.
	 *
	 * @return The new {@link ClassSelector}.
	 *
	 * @see ClassSelector
	 */
	public static ClassSelector T(Class<?> supertype) {
		return type(supertype);
	}

	/**
	 * Creates a {@link Selector} based on a URI template.
	 *
	 * @param uri
	 * 		The string to compile into a URI template and use for matching
	 *
	 * @return The new {@link UriPathSelector}.
	 *
	 * @see UriPathTemplate
	 * @see UriPathSelector
	 */
	public static Selector uri(String uri) {
		if(null == uri) {
			return null;
		}
		switch(uri.charAt(0)) {
			case '/':
				return new UriPathSelector(uri);
			default:
				return new UriSelector(uri);
		}
	}

	/**
	 * A short-hand alias for {@link Selectors#uri(String)}.
	 * <p/>
	 * Creates a {@link Selector} based on a URI template.
	 *
	 * @param uri
	 * 		The string to compile into a URI template and use for matching
	 *
	 * @return The new {@link UriPathSelector}.
	 *
	 * @see UriPathTemplate
	 * @see UriPathSelector
	 */
	public static Selector U(String uri) {
		return uri(uri);
	}

	/**
	 * Creates a {@link Selector} based on the given {@link Predicate}.
	 *
	 * @param predicate
	 * 		The {@link Predicate} to delegate to when matching objects.
	 *
	 * @return PredicateSelector
	 *
	 * @see PredicateSelector
	 */
	public static Selector predicate(Predicate<Object> predicate) {
		return new PredicateSelector(predicate);
	}

    /**
     * Creates a {@link Selector} that matches
     * all objects.
     * @return The new {@link MatchAllSelector}
     *
     * @see MatchAllSelector
     */
    public static Selector matchAll() {
        return new MatchAllSelector();
    }

	/**
	 * Creates a {@link Selector} that matches
	 * objects on set membership.
	 * @return The new {@link SetMembershipSelector}
	 *
	 * @see SetMembershipSelector
	 */
	public static Selector setMembership(Set set) {
		return new SetMembershipSelector(set);
	}

	public static class AnonymousKey {
		private final int hashCode = HASH_CODES.getAndIncrement() << 2;

		@Override
		public int hashCode() {
			return hashCode;
		}
	}
}
