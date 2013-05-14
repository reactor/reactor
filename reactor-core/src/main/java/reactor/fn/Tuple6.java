package reactor.fn;

/**
 * @author Jon Brisbin
 */
public class Tuple6<T1, T2, T3, T4, T5, T6> extends Tuple5<T1, T2, T3, T4, T5> {

	Tuple6(Object... values) {
		super(values);
	}

	/**
	 * Type-safe way to get the sixth object of this {@link reactor.fn.Tuple}.
	 *
	 * @return The sixth object, cast to the correct type.
	 */
	@SuppressWarnings("unchecked")
	public T6 getT6() {
		return (T6) get(5);
	}

}
