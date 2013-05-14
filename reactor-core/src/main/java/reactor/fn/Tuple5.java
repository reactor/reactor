package reactor.fn;

/**
 * @author Jon Brisbin
 */
public class Tuple5<T1, T2, T3, T4, T5> extends Tuple4<T1, T2, T3, T4> {

	Tuple5(Object... values) {
		super(values);
	}

	/**
	 * Type-safe way to get the fifth object of this {@link Tuple}.
	 *
	 * @return The fifth object, cast to the correct type.
	 */
	@SuppressWarnings("unchecked")
	public T5 getT5() {
		return (T5) get(4);
	}

}
