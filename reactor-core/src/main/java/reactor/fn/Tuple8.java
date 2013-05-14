package reactor.fn;

/**
 * @author Jon Brisbin
 */
public class Tuple8<T1, T2, T3, T4, T5, T6, T7, T8> extends Tuple7<T1, T2, T3, T4, T5, T6, T7> {

	Tuple8(Object... values) {
		super(values);
	}

	/**
	 * Type-safe way to get the eighth object of this {@link reactor.fn.Tuple}.
	 *
	 * @return The eighth object, cast to the correct type.
	 */
	@SuppressWarnings("unchecked")
	public T8 getT8() {
		return (T8) get(7);
	}

}
