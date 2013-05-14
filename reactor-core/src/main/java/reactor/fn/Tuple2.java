package reactor.fn;

/**
 * @author Jon Brisbin
 */
public class Tuple2<T1, T2> extends Tuple1<T1> {

	Tuple2(Object... values) {
		super(values);
	}

	/**
	 * Type-safe way to get the second object of this {@link Tuple}.
	 *
	 * @return The second object, cast to the correct type.
	 */
	@SuppressWarnings("unchecked")
	public T2 getT2() {
		return (T2) get(1);
	}

}
