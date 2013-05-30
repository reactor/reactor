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
import java.util.concurrent.atomic.AtomicInteger;

import org.junit.Test;

import reactor.fn.Registration;
import reactor.fn.routing.CachingRegistry;
import reactor.fn.routing.Registry;
import reactor.fn.routing.SelectionStrategy;
import reactor.fn.Selector;

public final class CachingRegistryTests {

	private final AtomicInteger cacheMisses = new AtomicInteger();

	private final CachingRegistry<Object> cachingRegistry = new CacheMissCountingCachingRegistry<Object>(null, null, cacheMisses);

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

	@Test
	public void nonEmptyResultsAreCached() {
		String key = "selector";
		Selector selector = $(key);

		this.cachingRegistry.register(selector, "alpha");

		this.cachingRegistry.select(key);
		this.cachingRegistry.select(key);

		assertEquals(1, this.cacheMisses.get());
	}

	@Test
	public void emptyResultsAreCached() {
		this.cachingRegistry.register($("another-key"), "alpha");
		this.cachingRegistry.select("key");
		this.cachingRegistry.select("key");

		assertEquals(1, this.cacheMisses.get());
	}

	@Test
	public void emptyResultsAreCachedWhenThereAreNoRegistrations() {
		this.cachingRegistry.select("key");
		this.cachingRegistry.select("key");

		assertEquals(1, this.cacheMisses.get());
	}

	@Test
	public void cacheIsRefreshedWhenANewRegistrationWithTheSameSelectorIsMade() {
		String key = "selector";
		Selector selector = $(key);

		this.cachingRegistry.register(selector, "alpha");

		this.cachingRegistry.select(key);
		this.cachingRegistry.select(key);

		assertEquals(1, this.cacheMisses.get());

		this.cachingRegistry.register(selector, "bravo");

		this.cachingRegistry.select(key);
		this.cachingRegistry.select(key);

		assertEquals(2, this.cacheMisses.get());
	}

	@Test
	public void cacheIsRefreshedWhenANewRegistrationWithADifferentSelectorIsMade() {
		String key1 = "selector";
		Selector selector1 = $(key1);

		this.cachingRegistry.register(selector1, "alpha");

		this.cachingRegistry.select(key1);
		this.cachingRegistry.select(key1);

		assertEquals(1, this.cacheMisses.get());

		String key2 = "selector2";
		Selector selector2 = $(key2);

		this.cachingRegistry.register(selector2, "bravo");

		this.cachingRegistry.select(key1);
		this.cachingRegistry.select(key1);

		assertEquals(2, this.cacheMisses.get());
	}

	private static final class CacheMissCountingCachingRegistry<T> extends CachingRegistry<T> {

		private final AtomicInteger cacheMisses;

		public CacheMissCountingCachingRegistry(
				Registry.LoadBalancingStrategy loadBalancingStrategy,
				SelectionStrategy selectionStrategy, AtomicInteger cacheMisses) {
			super(loadBalancingStrategy, selectionStrategy);
			this.cacheMisses = cacheMisses;
		}

		@Override
		protected void cacheMiss(Object key) {
			this.cacheMisses.incrementAndGet();
		}
	}
}
