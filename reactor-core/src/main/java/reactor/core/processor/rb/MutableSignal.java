package reactor.core.processor.rb;

import reactor.core.support.Signal;

/**
 * @author Stephane Maldini
 * @author Anatoly Kadyshev
 */
public final class MutableSignal<T> {

	public Signal    type  = Signal.NEXT;
	public T         value = null;
	public Throwable error = null;
	public long      seqId = -1;
}
