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
		return new Composable.Spec<T>(null);
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
		return new Composable.Spec<T>(Arrays.asList(value));
	}

	/**
	 * Create a {@link reactor.core.Composable} from the given list of values.
	 *
	 * @param values The values to use.
	 * @param <T>    The type of the values.
	 * @return A {@link reactor.core.Composable.Spec} to further refine the {@link reactor.core.Composable} and then build it.
	 */
	public static <T> Composable.Spec<T> each(Iterable<T> values) {
		return new Composable.Spec<T>(values);
	}
}
