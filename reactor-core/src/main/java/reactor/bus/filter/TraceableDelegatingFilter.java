/*
 * Copyright (c) 2011-2014 Pivotal Software, Inc.
 *
 *  Licensed under the Apache License, Version 2.0 (the "License");
 *  you may not use this file except in compliance with the License.
 *  You may obtain a copy of the License at
 *
 *         http://www.apache.org/licenses/LICENSE-2.0
 *
 *  Unless required by applicable law or agreed to in writing, software
 *  distributed under the License is distributed on an "AS IS" BASIS,
 *  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  See the License for the specific language governing permissions and
 *  limitations under the License.
 */

package reactor.bus.filter;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import reactor.core.support.Assert;

import java.util.Collections;
import java.util.List;

/**
 * @author Jon Brisbin
 */
public class TraceableDelegatingFilter implements Filter {

	private final Filter delegate;
	private final Logger log;

	public TraceableDelegatingFilter(Filter delegate) {
		Assert.notNull(delegate, "Delegate Filter cannot be null.");
		this.delegate = delegate;
		this.log = LoggerFactory.getLogger(delegate.getClass());
	}

	@Override
	public <T> List<T> filter(List<T> items, Object key) {
		if(log.isTraceEnabled()) {
			log.trace("filtering {} using key {}", items, key);
		}
		List<T> l = delegate.filter(items, key);
		if(log.isTraceEnabled()) {
			log.trace("items {} matched key {}", (null == items ? Collections.emptyList() : items), key);
		}
		return l;
	}

}
