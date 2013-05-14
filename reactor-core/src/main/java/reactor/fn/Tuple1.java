package reactor.fn;

/**
 * @author Jon Brisbin
 */
public class Tuple1<T1> extends Tuple {

	Tuple1(Object... values) {
		super(values);
	}

	/**
	 * Type-safe way to get the first object of this {@link Tuple}.
	 *
	 * @return The first object, cast to the correct type.
	 */
	@SuppressWarnings("unchecked")
	public T1 getT1() {
		return (T1) get(0);
	}

}
