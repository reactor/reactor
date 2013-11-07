package reactor.event.selector;

import reactor.function.Predicate;

/**
 * Implementation of {@link Selector} that delegates the work of matching an object to the given {@link Predicate}.
 *
 * @author Jon Brisbin
 */
public class PredicateSelector<T> extends ObjectSelector<Predicate<Object>,T> {

	public PredicateSelector(Predicate<Object> object) {
		super(object);
	}

	/**
	 * Creates a {@link Selector} based on the given {@link Predicate}.
	 *
	 * @param predicate
	 * 		The {@link Predicate} to delegate to when matching objects.
	 *
	 * @return PredicateSelector
	 */
	public static PredicateSelector predicateSelector(Predicate<Object> predicate) {
		return new PredicateSelector(predicate);
	}

	@Override
	public boolean matches(Object key) {
		return getObject().test(key);
	}

}
