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

package reactor.event;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.module.afterburner.AfterburnerModule;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import reactor.event.registry.CachingRegistry;
import reactor.event.registry.Registration;
import reactor.event.registry.Registry;
import reactor.event.selector.JsonPathSelector;
import reactor.event.selector.Selector;
import reactor.function.Consumer;
import reactor.function.Function;
import reactor.tuple.Tuple;
import reactor.tuple.Tuple2;

import java.io.IOException;
import java.util.Map;
import java.util.concurrent.atomic.AtomicLong;

import static org.hamcrest.MatcherAssert.assertThat;
import static reactor.event.selector.Selectors.$;
import static reactor.event.selector.Selectors.U;

/**
 * @author Jon Brisbin
 */
public class SelectorUnitTests {

	static final Logger LOG        = LoggerFactory.getLogger(SelectorUnitTests.class);
	static final int    selectors  = 500;
	static final int    iterations = 5000;

	@Test
	public void testSelectionThroughput() throws Exception {
		runTest("ObjectSelector", new Function<Integer, Tuple2<Selector, Object>>() {
			@Override
			public Tuple2<Selector, Object> apply(Integer i) {
				String key = "test" + i;
				return Tuple.<Selector, Object>of($(key), key);
			}
		});
	}

	@Test
	public void testUriTemplateSelectorThroughput() throws Exception {
		runTest("UriTemplateSelector", new Function<Integer, Tuple2<Selector, Object>>() {
			@Override
			public Tuple2<Selector, Object> apply(Integer i) {
				String key = "/test/" + i;
				return Tuple.<Selector, Object>of(U(key), key);
			}
		});
	}

	@Test
	public void testJsonPathSelectorThroughput() {
		runTest("JsonPath", new Function<Integer, Tuple2<Selector, Object>>() {
			ObjectMapper mapper = new ObjectMapper();

			{
				mapper.registerModule(new AfterburnerModule());
			}

			@Override
			public Tuple2<Selector, Object> apply(Integer i) {
				String json = "{\"data\": [{\"run\":\"" + i + "\"}]}";
				Object key;
				try {
					key = mapper.readValue(json, Map.class);
//					key = mapper.readTree(json);
				} catch(IOException e) {
					throw new IllegalStateException(e);
				}
				return Tuple.<Selector, Object>of(new JsonPathSelector("$.data[?(@.run == '" + i + "')]"), key);
			}
		});
	}

	private void runTest(String type, Function<Integer, Tuple2<Selector, Object>> fn) {
		final AtomicLong counter = new AtomicLong(selectors * iterations);
		Registry<Consumer<?>> registry = new CachingRegistry<Consumer<?>>();

		Consumer<?> countDown = new Consumer<Object>() {
			@Override
			public void accept(Object obj) {
				counter.decrementAndGet();
			}
		};

		Selector[] sels = new Selector[selectors];
		Object[] keys = new Object[selectors];

		for(int i = 0; i < selectors; i++) {
			Tuple2<Selector, Object> tup = fn.apply(i);
			sels[i] = tup.getT1();
			keys[i] = tup.getT2();
			registry.register(sels[i], countDown);
		}

		long start = System.currentTimeMillis();
		for(int i = 0; i < selectors * iterations; i++) {
			int j = i % selectors;
			for(Registration<? extends Consumer<?>> reg : registry.select(keys[j])) {
				reg.getObject().accept(null);
			}
		}
		long end = System.currentTimeMillis();
		double elapsed = (end - start);
		long throughput = Math.round((selectors * iterations) / (elapsed / 1000));
		LOG.info("{} throughput: {}/s", type, throughput);

		assertThat("All handlers have been found and executed.", counter.get() == 0);
	}

}
