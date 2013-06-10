package reactor.spring.integration.support;

import org.springframework.core.convert.converter.Converter;
import org.springframework.integration.Message;
import org.springframework.integration.MessageHeaders;
import org.springframework.integration.message.GenericMessage;
import reactor.fn.Event;

import java.util.Map;

/**
 * @author Jon Brisbin
 */
public class ReactorEventConverter<T> implements Converter<Event<T>, Message<T>> {
	@SuppressWarnings("unchecked")
	@Override
	public Message<T> convert(Event<T> ev) {
		MessageHeaders hdrs = new MessageHeaders((Map) ev.getHeaders().asMap());
		return new GenericMessage<T>(ev.getData(), hdrs);
	}
}
