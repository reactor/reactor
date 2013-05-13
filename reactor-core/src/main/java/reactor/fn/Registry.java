package reactor.fn;

/**
 * Implementations of this interface manage a registry of objects that works sort of like a Map, except Registries don't
 * use simple keys, they use {@link Selector}s to map their objects.
 *
 * @author Jon Brisbin
 * @author Andy Wilkinson
 */
public interface Registry<T> extends Iterable<Registration<? extends T>> {

	public enum LoadBalancingStrategy {
		NONE, ROUND_ROBIN, RANDOM
	}

	/**
	 * Registries can handle multiple objects registered with the same {@link Selector} differently, depending on how those
	 * multiple objects should be load-balanced.
	 *
	 * @param lb The {@link LoadBalancingStrategy} to use.
	 * @return {@literal this}
	 */
	Registry<T> setLoadBalancingStrategy(LoadBalancingStrategy lb);

	/**
	 * Registries can use a configurable {@link SelectionStrategy} to determine which objects in the registry match the
	 * given {@link Selector}.
	 *
	 * @param selectionStrategy The {@link SelectionStrategy} to use.
	 * @return {@literal this}
	 */
	Registry<T> setSelectionStrategy(SelectionStrategy selectionStrategy);

	/**
	 * Assign the given {@link Selector} with the given object.
	 *
	 * @param sel The left-hand side of the {@literal Selector} comparison check.
	 * @param obj The object to assign.
	 * @return {@literal this}
	 */
	<V extends T> Registration<V> register(Selector sel, V obj);

	/**
	 * Remove any objects matching this {@code key}. This will unregister <b>all</b> objects matching the given
	 * {@literal key}. There's no provision for removing only a specific object.
	 *
	 * @param key The key to be matched by the Selectors
	 * @return {@literal true} if any objects were unassigned, {@literal false} otherwise.
	 */
	boolean unregister(Object key);

	/**
	 * Select {@link Registration}s whose {@link Selector} {@link Selector#matches(Object)} the given {@code key}.
	 *
	 * @param key The key for the Selectors to match
	 * @return An {@link Iterable} of {@link Registration}s whose {@link Selector} matches the given key.
	 */
	Iterable<Registration<? extends T>> select(Object key);

}
