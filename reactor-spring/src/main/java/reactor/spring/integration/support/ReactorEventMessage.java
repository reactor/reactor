/*
 * Copyright (c) 2011-2013 GoPivotal, Inc. All Rights Reserved.
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
