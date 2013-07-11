package reactor.event.selector;

import reactor.tuple.Tuple;
import reactor.tuple.Tuple2;

/**
 * Helper methods for creating {@link Selector}s.
 *
 * @author Andy Wilkinson
 */
public abstract class Selectors {

	/**
	 * Creates an anonymous {@link reactor.event.selector.Selector}, returning a {@link Tuple}
	 * containing the {@link Selector} and the notification key that the selector matches.
	 *
	 * @return The Selector notification key tuple
	 *
	 * @see ObjectSelector
	 */
	public static Tuple2<Selector, Object> anonymous() {
		Object obj = new Object();
		return Tuple.of($(obj), obj);
	}

	/**
	 * A short-hand alias for {@link Selectors#anonymous()}.
	 *
	 * <p/>
	 *
	 * Creates an anonymous {@link reactor.event.selector.Selector}, returning a {@link Tuple}
	 * containing the {@link Selector} and the notification key that the selector matches.
	 *
	 * @return The Selector notification key tuple
	 *
	 * @see ObjectSelector
	 */
	public static Tuple2<Selector, Object> $() {
		return anonymous();
	}

	/**
	 * Creates a {@link Selector} based on the given object.
	 *
	 * @param obj The object to use for matching
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
	 *
	 * <p/>
	 *
	 * Creates a {@link Selector} based on the given object.
	 *
	 * @param obj The object to use for matching
	 *
	 * @return The new {@link ObjectSelector}.
	 *
	 * @see ObjectSelector
	 */
	public static <T> Selector $(T obj) {
		return object(obj);
	}

	/**
	 * Creates a {@link Selector} based on the given regular expression.
	 *
	 * @param regex The regular expression to compile and use for matching
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
	 * @param regex The regular expression to compile and use for matching
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
	 * @param supertype The supertype to use for matching
	 *
	 * @return The new {@link ClassSelector}.
	 *
	 * @see ClassSelector
	 */
	public static Selector type(Class<?> supertype) {
		return new ClassSelector(supertype);
	}

	/**
	 * A short-hand alias for {@link Selectors#type(Class)}.
	 *
	 * Creates a {@link Selector} based on the given class type that matches objects whose type is
	 * assignable according to {@link Class#isAssignableFrom(Class)}.
	 *
	 * @param supertype The supertype to compare.
	 *
	 * @return The new {@link ClassSelector}.
	 */
	public static Selector T(Class<?> supertype) {
		return type(supertype);
	}

	/**
	 * Creates a {@link Selector} based on a URI template.
	 *
	 * @param uriTemplate The string to compile into a URI template and use for matching
	 *
	 * @return The new {@link UriTemplateSelector}.
	 *
	 * @see UriTemplate
	 * @see UriTemplateSelector
	 */
	public static Selector uri(String uriTemplate) {
		return new UriTemplateSelector(uriTemplate);
	}

	/**
	 * A short-hand alias for {@link Selectors#uri(String)}.
	 * </p>
	 * Creates a {@link Selector} based on a URI template.
	 *
	 * @param uriTemplate The string to compile into a URI template and use for matching
	 *
	 * @return The new {@link UriTemplateSelector}.
	 *
	 * @see UriTemplate
	 * @see UriTemplateSelector
	 */
	public static Selector U(String uriTemplate) {
		return new UriTemplateSelector(uriTemplate);
	}
}
