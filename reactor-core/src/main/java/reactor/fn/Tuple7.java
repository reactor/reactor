package reactor.fn;

/**
 * @author Jon Brisbin
 */
public class Tuple7<T1, T2, T3, T4, T5, T6, T7> extends Tuple6<T1, T2, T3, T4, T5, T6> {

	Tuple7(Object... values) {
		super(values);
	}

	/**
	 * Type-safe way to get the seventh object of this {@link Tuple}.
	 *
	 * @return The seventh object, cast to the correct type.
	 */
	@SuppressWarnings("unchecked")
	public T7 getT7() {
		return (T7) get(6);
	}

}
