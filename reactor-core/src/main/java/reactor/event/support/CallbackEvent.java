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
	final Consumer<Object> callback;

	public CallbackEvent(T data, Consumer<Object> callback) {
		this(null, data, callback);
	}

	public CallbackEvent(Headers headers, T data, Consumer<Object> callback) {
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
	public void callback(){
		if(null != callback){
			callback.accept(getData());
		}
	}
}
