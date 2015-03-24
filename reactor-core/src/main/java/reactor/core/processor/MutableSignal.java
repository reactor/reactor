package reactor.core.processor;

/**
 * Created by jbrisbin on 3/24/15.
 */
public class MutableSignal<T> {

	public enum Type {
		NEXT, ERROR, COMPLETE
	}

	public Type      type  = Type.NEXT;
	public T         value = null;
	public Throwable error = null;

}
