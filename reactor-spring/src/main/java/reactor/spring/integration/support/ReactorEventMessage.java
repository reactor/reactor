package reactor.spring.integration.support;

import org.springframework.integration.Message;
import org.springframework.integration.MessageHeaders;
import reactor.fn.Event;
import reactor.util.Assert;

import java.util.Map;

/**
 * @author Jon Brisbin
 */
public class ReactorEventMessage<T> implements Message<T> {

	private final    Event<T>       event;
	private volatile MessageHeaders headers;

	public ReactorEventMessage(Event<T> event) {
		Assert.notNull(event, "Event cannot be null.");
		this.event = event;
	}

	@SuppressWarnings("unchecked")
	@Override
	public MessageHeaders getHeaders() {
		if (null == headers) {
			synchronized (event) {
				Map m = (Map) event.getHeaders().asMap();
				m.put(MessageHeaders.ID, event.getId());
				headers = new MessageHeaders(m);
			}
		}
		return null;
	}

	@Override
	public T getPayload() {
		return event.getData();
	}

}
