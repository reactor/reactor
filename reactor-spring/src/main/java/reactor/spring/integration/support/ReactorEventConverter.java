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
public class ReactorEventConverter implements Converter<Event<?>, Message<?>> {
	@Override
	@SuppressWarnings("unchecked")
	public Message<?> convert(Event<?> ev) {
		MessageHeaders hdrs = new MessageHeaders((Map) ev.getHeaders().asMap());
		return new GenericMessage(ev.getData(), hdrs);
	}
}
