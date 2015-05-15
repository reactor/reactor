/*
 * Copyright (c) 2011-2015 Pivotal Software Inc., Inc. All Rights Reserved.
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

package reactor.bus.registry;

import org.junit.Test;
import reactor.bus.selector.ObjectSelector;
import reactor.bus.selector.Selector;
import reactor.bus.selector.Selectors;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;

import static org.junit.Assert.assertEquals;

public final class CachingRegistryTests {

	private final AtomicInteger    cacheMisses     = new AtomicInteger();
	private final Registry<Object, Object> cachingRegistry = new CacheMissCountingCachingRegistry<Object>(cacheMisses);

	@Test
	public void registrationsWithTheSameSelectorAreOrderedByInsertionOrder() {
		String key = "selector";
		Selector selector = Selectors.$(key);

		this.cachingRegistry.register(selector, "echo");
		this.cachingRegistry.register(selector, "bravo");
		this.cachingRegistry.register(selector, "alpha");
		this.cachingRegistry.register(selector, "charlie");
		this.cachingRegistry.register(selector, "delta");

		Iterable<Registration<Object, ? extends Object>> registrations = this.cachingRegistry.select(key);
		List<Object> objects = new ArrayList<Object>();
		for (Registration<?, ? extends Object> registration : registrations) {
			if (null != registration) {
				objects.add(registration.getObject());
			}
		}

		assertEquals(Arrays.asList("echo", "bravo", "alpha", "charlie", "delta"), objects);
	}

	@Test
	public void nonEmptyResultsAreCached() {
		String key = "/**/selector";
		Selector selector = Selectors.uri(key);

		this.cachingRegistry.register(selector, "alpha");

		this.cachingRegistry.select("/test/selector");
		this.cachingRegistry.select("/test/selector");

		assertEquals(1, this.cacheMisses.get());
	}

	@Test
	public void emptyResultsAreCached() {
		this.cachingRegistry.register(Selectors.$("another-key"), "alpha");
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
		Selector selector = Selectors.$(key);

		this.cachingRegistry.register(selector, "alpha");

		this.cachingRegistry.select(key);
		this.cachingRegistry.select(key);

		assertEquals(1, this.cacheMisses.get());

		this.cachingRegistry.register(selector, "bravo");

		this.cachingRegistry.select(key);
		this.cachingRegistry.select(key);

		assertEquals(2, this.cacheMisses.get());
	}

	//@Test
	public void cacheIsRefreshedWhenANewRegistrationWithADifferentSelectorIsMade() {
		String key1 = "selector";
		Selector selector1 = Selectors.$(key1);

		this.cachingRegistry.register(selector1, "alpha");

		this.cachingRegistry.select(key1);
		this.cachingRegistry.select(key1);

		assertEquals(1, this.cacheMisses.get());

		String key2 = "selector2";
		Selector selector2 = Selectors.$(key2);

		this.cachingRegistry.register(selector2, "bravo");

		this.cachingRegistry.select(key1);
		this.cachingRegistry.select(key1);

		assertEquals(2, this.cacheMisses.get());
	}


	//Issue : https://github.com/eventBus/eventBus/issues/237
	@Test
	public void invokeConsumersWithCustomSelector() {

		Subscription sub1 = new Subscription("client1", "test");
		Selector s1 = new MySelector(sub1);

		// consumer1
		this.cachingRegistry.register(s1, "pseudo-consumer-1");

		// notify1
		List<Registration<Object, ?>> registrations = this.cachingRegistry.select("test");

		assertEquals("number of consumers incorrect", 1, registrations.size());


		// consumer2
		this.cachingRegistry.register(s1, "pseudo-consumer-2");

		// consumer3
		Subscription sub2 = new Subscription("client2", "test");
		Selector s2 = new MySelector(sub2);
		this.cachingRegistry.register(s2, "pseudo-consumer-3");

		//consumer 4
		Subscription sub3 = new Subscription("client2", "test2");
		Selector s3 = new MySelector(sub3);
		this.cachingRegistry.register(s3, "pseudo-consumer-4");

		//prepopulate and add another consumer
		this.cachingRegistry.select("test2");
		this.cachingRegistry.register(s3, "pseudo-consumer-5");

		// notify2
		registrations = this.cachingRegistry.select("test");
		assertEquals("number of consumers incorrect", 3, registrations.size());

		registrations = this.cachingRegistry.select("test2");
		assertEquals("number of consumers incorrect", 2, registrations.size());


		/*for(Registration<?> registration : registrations){
			System.out.println (registration.getObject());
		}*/
	}

	private static final class CacheMissCountingCachingRegistry<T> extends CachingRegistry<Object, T> {
		private final AtomicInteger cacheMisses;

		public CacheMissCountingCachingRegistry(AtomicInteger cacheMisses) {
			super(true, true, null);
			this.cacheMisses = cacheMisses;
		}

		@Override
		protected void cacheMiss(Object key) {
			this.cacheMisses.incrementAndGet();
		}
	}

	public static class Subscription {
		public final String clientId;
		public final String topic;

		public Subscription(String clientId, String topic) {
			this.clientId = clientId;
			this.topic = topic;
		}
	}

	static class MySelector extends ObjectSelector<Object, Subscription> {
		public MySelector(Subscription subscription) {
			super(subscription);
		}

		@Override
		public boolean matches(Object key) {
			if (!(key instanceof String)) {
				return false;
			}

			return key.equals(getObject().topic);
		}
	}

}
