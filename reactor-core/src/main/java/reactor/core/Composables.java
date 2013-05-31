package reactor.core;

import reactor.fn.Supplier;

import java.util.Arrays;
import java.util.Collection;

/**
 * @author Stephane Maldini
 */
public class Composables {
	/**
	 * Create a delayed {@link reactor.core.Composable} with no initial state, ready to accept values.
	 *
	 * @return A {@link reactor.core.Composable.Spec} to further refine the {@link reactor.core.Composable} and then build it.
	 */
	public static <T> Composable.Spec<T> defer() {
		return new Composable.Spec<T>(null, null, null);
	}

	/**
	 * Create a {@link reactor.core.Composable} from the given value.
	 *
	 * @param value The value to use.
	 * @param <T>   The type of the value.
	 * @return A {@link reactor.core.Composable.Spec} to further refine the {@link reactor.core.Composable} and then build it.
	 */
	@SuppressWarnings("unchecked")
	public static <T> Composable.Spec<T> init(T value) {
		return new Composable.Spec<T>(Arrays.asList(value), null, null);
	}

	/**
	 * Create a {@link reactor.core.Composable} from the given list of values.
	 *
	 * @param values The values to use.
	 * @param <T>    The type of the values.
	 * @return A {@link reactor.core.Composable.Spec} to further refine the {@link reactor.core.Composable} and then build it.
	 */
	public static <T> Composable.Spec<T> each(Iterable<T> values) {
		return new Composable.Spec<T>(values, null, null);
	}

	/**
	 * Create a {@link reactor.core.Composable} from the given {@link reactor.fn.Supplier}.
	 *
	 * @param supplier The function to defer.
	 * @param <T>      The type of the values.
	 * @return A {@link reactor.core.Composable.Spec} to further refine the {@link reactor.core.Composable} and then build it.
	 */
	public static <T> Composable.Spec<T> task(Supplier<T> supplier) {
		return new Composable.Spec<T>(null, supplier, null);
	}

	/**
	 * Merge given composable into a new a {@literal Composable}.
	 *
	 * @param composables The composables to use.
	 * @param <T>         The type of the function result.
	 * @return a {@link reactor.core.Promise.Spec}.
	 */
	public static <T> Composable.Spec<Collection<T>> all(Composable<T>... composables) {
		return all(Arrays.asList(composables));
	}

	/**
	 * Merge given composable into a new a {@literal Composable}.
	 *
	 * @param composables The composables to use.
	 * @param <T>         The type of the function result.
	 * @return a {@link reactor.core.Promise.Spec}.
	 */
	public static <T> Composable.Spec<Collection<T>> all(Collection<? extends Composable<T>> composables) {
		return new Composable.Spec<Collection<T>>(null, null, composables);
	}
}
