package reactor.event.support;

import reactor.event.Event;
import reactor.function.Consumer;

/**
 * Simple {@link Event} implementation that attaches a callback to an {@link Event} and
 * passes it to a delegate {@link Consumer}.
 *
 * @param <T> the type of the event payload
 *
 * @author Stephane Maldini
 */
public class CallbackEvent<T> extends Event<T>{
	final Consumer callback;

	public CallbackEvent(T data, Consumer callback) {
		this(null, data, callback);
	}

	public CallbackEvent(Headers headers, T data, Consumer callback) {
		super(headers, data);
		this.callback = callback;
	}

	@Override
	public <X> Event<X> copy(X data) {
		if (null != getReplyTo())
			return new CallbackEvent<X>(getHeaders(), data, callback).setReplyTo(getReplyTo());
		else
			return new CallbackEvent<X>(getHeaders(), data, callback);
	}


	/**
	 * Trigger callback with current payload
	 */
	@SuppressWarnings("unchecked")
	public void callback(){
		if(null != callback){
			callback.accept(getData());
		}
	}
}
