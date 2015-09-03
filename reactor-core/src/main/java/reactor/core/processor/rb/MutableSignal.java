package reactor.core.processor.rb;

import reactor.core.support.SignalType;

/**
 * @author Stephane Maldini
 * @author Anatoly Kadyshev
 */
public final class MutableSignal<T> {

	public SignalType type  = SignalType.NEXT;
	public T          value = null;
	public Throwable  error = null;
	public long       seqId = -1;
}
