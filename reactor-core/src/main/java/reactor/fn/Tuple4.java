package reactor.fn;

/**
 * @author Jon Brisbin
 */
public class Tuple4<T1, T2, T3, T4> extends Tuple3<T1, T2, T3> {

	Tuple4(Object... values) {
		super(values);
	}

	/**
	 * Type-safe way to get the fourth object of this {@link reactor.fn.Tuple}.
	 *
	 * @return The fourth object, cast to the correct type.
	 */
	@SuppressWarnings("unchecked")
	public T4 getT4() {
		return (T4) get(3);
	}

}
