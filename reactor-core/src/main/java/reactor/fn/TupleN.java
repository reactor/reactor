package reactor.fn;

/**
 * @author Jon Brisbin
 */
public class TupleN<T1, T2, T3, T4, T5, T6, T7, T8, TRest extends Tuple> extends Tuple8<T1, T2, T3, T4, T5, T6, T7, T8> {

	TupleN(Object... values) {
		super(values);
	}

	/**
	 * Type-safe way to get the remaining objects of this {@link Tuple}.
	 *
	 * @return The remaining objects, as a Tuple.
	 */
	@SuppressWarnings("unchecked")
	public TRest getTRest() {
		return (TRest) get(8);
	}

}
