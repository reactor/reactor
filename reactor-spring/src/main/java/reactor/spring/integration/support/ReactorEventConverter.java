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

import org.springframework.core.convert.converter.Converter;
import org.springframework.integration.Message;
import reactor.fn.Event;

/**
 * @author Jon Brisbin
 */
public class ReactorEventConverter implements Converter<Event<?>, Message<?>> {
	@Override
	@SuppressWarnings("unchecked")
	public Message<?> convert(Event<?> ev) {
		return new ReactorEventMessage<Object>((Event) ev);
	}
}
