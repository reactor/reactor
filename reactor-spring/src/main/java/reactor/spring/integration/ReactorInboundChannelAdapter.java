/*
 * Copyright (c) 2011-2013 the original author or authors.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *       http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package reactor.spring.integration;

import org.springframework.core.convert.converter.Converter;
import org.springframework.integration.Message;
import org.springframework.integration.MessageChannel;
import org.springframework.integration.endpoint.MessageProducerSupport;
import org.springframework.util.Assert;
import reactor.core.Reactor;
import reactor.fn.Event;
import reactor.fn.selector.Selector;
import reactor.spring.integration.support.ReactorEventConverter;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;

/**
 * @author Jon Brisbin
 */
public class ReactorInboundChannelAdapter extends MessageProducerSupport {

	private final    Reactor        reactor;
	private final    Selector       selector;
	private volatile MessageChannel outputChannel;
	private volatile MessageChannel errorChannel;
	@SuppressWarnings("unchecked")
	private volatile Converter<Event<?>, Message<?>> converter = new ReactorEventConverter();

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
	public void setOutputChannel(MessageChannel outputChannel) {
		super.setOutputChannel(outputChannel);
		this.outputChannel = outputChannel;
	}

	@Override
	public void setErrorChannel(MessageChannel errorChannel) {
		super.setErrorChannel(errorChannel);
		this.errorChannel = errorChannel;
	}

	public void setConverter(Converter<Event<?>, Message<?>> converter) {
		Assert.notNull(converter, "Converter cannot be null.");
		this.converter = converter;
	}

	@Override
	protected void onInit() {
		super.onInit();

		MessageChannelConsumer<Event<?>> consumer = new MessageChannelConsumer<Event<?>>(outputChannel, errorChannel);
		consumer.setConverter(converter);

		if (null != selector) {
			reactor.on(selector, consumer);
		} else {
			reactor.on(consumer);
		}
	}

}
