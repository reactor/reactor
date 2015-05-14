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

package reactor.bus;

import com.fasterxml.jackson.databind.ObjectMapper;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import reactor.bus.registry.Registration;
import reactor.bus.registry.Registries;
import reactor.bus.registry.Registry;
import reactor.bus.selector.Selector;
import reactor.bus.selector.Selectors;
import reactor.fn.Consumer;
import reactor.fn.Function;
import reactor.fn.tuple.Tuple;
import reactor.fn.tuple.Tuple2;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicLong;

import static org.hamcrest.MatcherAssert.assertThat;
import static reactor.bus.selector.JsonPathSelector.J;

/**
 * @author Jon Brisbin
 */
public class SelectorUnitTests {

	static final Logger LOG        = LoggerFactory.getLogger(SelectorUnitTests.class);
	final        int    selectors  = 50;
	final        int    iterations = 10000;

	final ObjectMapper mapper = new ObjectMapper();

	@Test
	public void testSelectionThroughput() throws Exception {
		runTest("ObjectSelector", new Function<Integer, Tuple2<Selector, Object>>() {
			@Override
			public Tuple2<Selector, Object> apply(Integer i) {
				String key = "test" + i;
				return Tuple.<Selector, Object>of(Selectors.$(key), key);
			}
		});
	}

	@Test
	public void testUriTemplateSelectorThroughput() throws Exception {
		runTest("UriPathSelector", new Function<Integer, Tuple2<Selector, Object>>() {
			@Override
			public Tuple2<Selector, Object> apply(Integer i) {
				String key = "/test/" + i;
				return Tuple.<Selector, Object>of(Selectors.U(key), key);
			}
		});
	}

	@Test
	public void testJsonPathSelectorThroughput() {
		final String jsonTmpl = "{\"data\": [{\"run\": %s}]}";
		final String jsonPathTmpl = "$.data[?(@.run == %s)]";

		runTest("JsonPath[String]", new Function<Integer, Tuple2<Selector, Object>>() {
			@Override
			public Tuple2<Selector, Object> apply(Integer i) {
				Selector sel = J(String.format(jsonPathTmpl, i));
				String json = String.format(jsonTmpl, i);
				return Tuple.<Selector, Object>of(sel, json);
			}
		});

		runTest("JsonPath[POJO]", new Function<Integer, Tuple2<Selector, Object>>() {
			@Override
			public Tuple2<Selector, Object> apply(Integer i) {
				Selector sel = J(String.format(jsonPathTmpl, i));
				DataNode node = new DataNode(new DataNode.Run(i));
				return Tuple.<Selector, Object>of(sel, node);
			}
		});

		runTest("JsonPath[Map]", new Function<Integer, Tuple2<Selector, Object>>() {
			@Override
			public Tuple2<Selector, Object> apply(Integer i) {
				Selector sel = J(String.format(jsonPathTmpl, i));
				String json = String.format(jsonTmpl, i);
				Object key;
				try {
					key = mapper.readValue(json, Map.class);
				} catch(IOException e) {
					throw new IllegalStateException(e);
				}
				return Tuple.<Selector, Object>of(sel, key);
			}
		});

		runTest("JsonPath[Tree]", new Function<Integer, Tuple2<Selector, Object>>() {
			@Override
			public Tuple2<Selector, Object> apply(Integer i) {
				Selector sel = J(String.format(jsonPathTmpl, i));
				String json = String.format(jsonTmpl, i);
				Object key;
				try {
					key = mapper.readTree(json);
				} catch(IOException e) {
					throw new IllegalStateException(e);
				}
				return Tuple.<Selector, Object>of(sel, key);
			}
		});
	}

	private void runTest(String type, Function<Integer, Tuple2<Selector, Object>> fn) {
		final AtomicLong counter = new AtomicLong(selectors * iterations);
		Registry<Object, Consumer<?>> registry = Registries.create();

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
			for(Registration<?, ? extends Consumer<?>> reg : registry.select(keys[j])) {
				reg.getObject().accept(null);
			}
		}
		long end = System.currentTimeMillis();
		double elapsed = (end - start);
		long throughput = Math.round((selectors * iterations) / (elapsed / 1000));
		LOG.info("{} throughput: {}M/s in {}ms", type, throughput, Math.round(elapsed));

		assertThat("All handlers have been found and executed.", counter.get() == 0);
	}

	public static class DataNode {
		private final List<Run> data = new ArrayList<Run>();

		private DataNode(Run data) {
			this.data.add(data);
		}

		public List<Run> getData() {
			return data;
		}

		public static class Run {
			private final int run;

			Run(int run) {
				this.run = run;
			}

			public int getRun() {
				return run;
			}
		}
	}

}
