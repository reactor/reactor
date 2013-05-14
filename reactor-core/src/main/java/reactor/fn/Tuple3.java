package reactor.fn;

/**
 * @author Jon Brisbin
 */
public class Tuple3<T1, T2, T3> extends Tuple2<T1, T2> {

	Tuple3(Object... values) {
		super(values);
	}

	/**
	 * Type-safe way to get the third object of this {@link Tuple}.
	 *
	 * @return The third object, cast to the correct type.
	 */
	@SuppressWarnings("unchecked")
	public T3 getT3() {
		return (T3) get(2);
	}

}
