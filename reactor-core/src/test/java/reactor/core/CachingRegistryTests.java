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

package reactor.core;

import static org.junit.Assert.assertEquals;
import static reactor.Fn.$;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import org.junit.Test;

import reactor.fn.Registration;
import reactor.fn.Selector;

public final class CachingRegistryTests {

	private final CachingRegistry<Object> cachingRegistry = new CachingRegistry<Object>(null, null);

	@Test
	public void registrationsWithTheSameSelectorAreOrderedByInsertionOrder() {
		String key = "selector";
		Selector selector = $(key);

		this.cachingRegistry.register(selector, "echo");
		this.cachingRegistry.register(selector, "bravo");
		this.cachingRegistry.register(selector, "alpha");
		this.cachingRegistry.register(selector, "charlie");
		this.cachingRegistry.register(selector, "delta");

		Iterable<Registration<? extends Object>> registrations = this.cachingRegistry.select(key);
		List<Object> objects = new ArrayList<Object>();
		for (Registration<? extends Object> registration : registrations) {
			objects.add(registration.getObject());
		}

		assertEquals(Arrays.asList("echo", "bravo", "alpha", "charlie", "delta"), objects);
	}
}
