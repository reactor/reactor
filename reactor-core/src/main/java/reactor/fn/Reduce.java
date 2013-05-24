package reactor.fn;

/**
 * A {@link reactor.core.Composable#reduce(Function)} operation needs a stateful object to pass as the argument,
 * which contains the
 * last accumulated value, as well as the next, just-accepted value.
 *
 * @param <NEXTVALUE> The type of the input value.
 * @param <LASTVALUE> The type of the accumulated or last value.
 */
public class Reduce<NEXTVALUE, LASTVALUE> {
	private final LASTVALUE lastValue;
	private final NEXTVALUE nextValue;

	public Reduce(LASTVALUE lastValue, NEXTVALUE nextValue) {
		this.lastValue = lastValue;
		this.nextValue = nextValue;
	}

	/**
	 * Get the accumulated value.
	 *
	 * @return
	 */
	public LASTVALUE getLastValue() {
		return lastValue;
	}

	/**
	 * Get the next input value.
	 *
	 * @return
	 */
	public NEXTVALUE getNextValue() {
		return nextValue;
	}
}
