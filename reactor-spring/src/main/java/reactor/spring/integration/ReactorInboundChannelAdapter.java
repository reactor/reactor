package reactor.spring.integration;

import org.springframework.core.convert.converter.Converter;
import org.springframework.integration.Message;
import org.springframework.integration.MessageChannel;
import org.springframework.integration.endpoint.MessageProducerSupport;
import org.springframework.util.Assert;
import reactor.core.Reactor;
import reactor.fn.Consumer;
import reactor.fn.Event;
import reactor.fn.selector.Selector;
import reactor.spring.integration.support.ReactorEventConverter;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;

import static reactor.fn.Functions.T;

/**
 * @author Jon Brisbin
 */
public class ReactorInboundChannelAdapter extends MessageProducerSupport {

	private final    Reactor        reactor;
	private final    Selector       selector;
	private volatile MessageChannel errorChannel;
	@SuppressWarnings("unchecked")
	private volatile Converter<Event, Message> converter = new ReactorEventConverter();

	public ReactorInboundChannelAdapter(@Nonnull Reactor reactor) {
		this(reactor, null);
	}

	public ReactorInboundChannelAdapter(@Nonnull Reactor reactor,
																			@Nullable Selector selector) {
		Assert.notNull(reactor, "Reactor cannot be null.");
		this.reactor = reactor;
		this.selector = selector;
	}

	@Override
	public void setErrorChannel(MessageChannel errorChannel) {
		super.setErrorChannel(errorChannel);
		this.errorChannel = errorChannel;
	}

	public void setConverter(Converter<Event, Message> converter) {
		Assert.notNull(converter, "Converter cannot be null.");
		this.converter = converter;
	}

	@Override
	protected void onInit() {
		super.onInit();

		Consumer<Event<?>> consumer = new Consumer<Event<?>>() {
			@Override
			@SuppressWarnings("unchecked")
			public void accept(Event<?> ev) {
				sendMessage(converter.convert(ev));
			}
		};
		if (null != selector) {
			reactor.on(selector, consumer);
		} else {
			reactor.on(consumer);
		}

		if (null != errorChannel) {
			reactor.on(T(Throwable.class), new Consumer<Event<Throwable>>() {
				@Override
				public void accept(Event<Throwable> ev) {
					errorChannel.send(converter.convert(ev));
				}
			});
		}
	}

}
