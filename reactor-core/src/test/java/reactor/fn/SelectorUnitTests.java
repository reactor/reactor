/*
 * Copyright 2002-2013 the original author or authors.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *       http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND,
 * either express or implied.
 *
 * See the License for the specific language governing permissions
 * and limitations under the License.
 */

package reactor.fn;

import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import reactor.core.CachingRegistry;

import java.util.concurrent.atomic.AtomicLong;

import static org.hamcrest.MatcherAssert.assertThat;
import static reactor.Fn.$;
import static reactor.Fn.U;

/**
 * @author Jon Brisbin
 */
public class SelectorUnitTests {

	static final Logger LOG        = LoggerFactory.getLogger(SelectorUnitTests.class);
	static final int    selectors  = 500;
	static final int    iterations = 5000;

	@Test
	@SuppressWarnings({"unchecked", "rawtypes"})
	public void testSelectionThroughput() throws Exception {
		final AtomicLong counter = new AtomicLong(selectors * iterations);
		Registry<Consumer<?>> registry = new CachingRegistry<Consumer<?>>();

		Consumer<?> hello = new Consumer<Object>() {
			@Override
			public void accept(Object obj) {
				counter.decrementAndGet();
			}
		};

		Selector[] sels = new Selector[selectors];
		for (int i = 0; i < selectors; i++) {
			sels[i] = $("test" + i);
			registry.register(sels[i], hello);
		}

		long start = System.currentTimeMillis();
		for (int i = 0; i < selectors * iterations; i++) {
			int j = i % selectors;
			for (Registration<? extends Consumer<?>> reg : registry.select(sels[j])) {
				reg.getObject().accept(null);
			}
		}
		long end = System.currentTimeMillis();
		double elapsed = (end - start);
		long throughput = Math.round((selectors * iterations) / (elapsed / 1000));
		LOG.info("Selector throughput: " + throughput + "/s");

		assertThat("All handlers have been found and executed.", counter.get() == 0);
	}

	@SuppressWarnings("unchecked")
	@Test
	public void testUriTemplateSelectorThroughput() throws Exception {
		final AtomicLong counter = new AtomicLong(selectors * iterations);
		Registry<Consumer<?>> registry = new CachingRegistry<Consumer<?>>();

		Consumer<?> hello = new Consumer<Object>() {
			@Override
			public void accept(Object obj) {
				counter.decrementAndGet();
			}
		};

		Selector sel1 = U("/test/{i}");
		registry.register(sel1, hello);

		Selector[] sels = new Selector[selectors];
		for (int i = 0; i < selectors; i++) {
			sels[i] = $("/test/" + i);
		}


		long start = System.currentTimeMillis();
		for (int i = 0; i < selectors * iterations; i++) {
			int j = i % selectors;
			for (Registration<? extends Consumer<?>> reg : registry.select(sels[j])) {
				reg.getObject().accept(null);
			}
		}
		long end = System.currentTimeMillis();
		double elapsed = (end - start);
		long throughput = Math.round((selectors * iterations) / (elapsed / 1000));
		LOG.info("UriTemplateSelector throughput: " + throughput + "/s");

		assertThat("All handlers have been found and executed.", counter.get() == 0);
	}

}
