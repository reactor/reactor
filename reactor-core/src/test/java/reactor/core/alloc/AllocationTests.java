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

package reactor.core.alloc;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import reactor.function.Function;

import java.util.Arrays;
import java.util.Iterator;
import java.util.NoSuchElementException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

import static junit.framework.Assert.assertTrue;

/**
 * @author Jon Brisbin
 */
public class AllocationTests {

	private final Logger log = LoggerFactory.getLogger(getClass());
	private ExecutorService tasks;

	@Before
	public void setup() {
		tasks = Executors.newCachedThreadPool();
	}

	@After
	public void cleanup() throws InterruptedException {
		tasks.shutdown();
		assertTrue("Tasks were terminated cleanly", tasks.awaitTermination(5, TimeUnit.SECONDS));
	}

	@Test
	public void testCustomIteratorThroughput() throws InterruptedException {
		final int items = 1000;
		final long timeout = 15000;
		final String[] strs = new String[items];
		for(int i = 0; i < items; i++) {
			strs[i] = "Hello World #" + i + "!";
		}

		final FixedIterator<String> fixedIter = new FixedIterator<String>();
		Function<Long, Integer> fn1 = new Function<Long, Integer>() {
			Iterator<String> iter;

			@Override
			public Integer apply(Long count) {
				if(null == iter || !iter.hasNext()) {
					iter = fixedIter.setValues(strs);
				}
				iter.next();
				return 1;
			}
		};
		doTest("Array-based, reusable Iterator", fn1, timeout);

		Thread.sleep(1000);

		Function<Long, Integer> fn2 = new Function<Long, Integer>() {
			Iterator<String> iter;

			@Override
			public Integer apply(Long count) {
				if(null == iter || !iter.hasNext()) {
					iter = Arrays.asList(strs).iterator();
				}
				iter.next();
				return 1;
			}
		};
		doTest("ArrayList, one-off Iterator", fn2, timeout);
	}

	private void doTest(String test, Function<Long, Integer> fn, long timeout) {
		long count = 0;
		long start = System.currentTimeMillis();
		long end;
		double elapsed;

		while(((end = System.currentTimeMillis()) - start) < timeout) {
			int delta = fn.apply(count);
			if(delta < 0) {
				break;
			}
			count += delta;
		}

		elapsed = end - start;
		long throughput = (long)(count / (elapsed / 1000.0));

		log.info("{} throughput: {}/sec in {}ms", test, throughput, (long)elapsed);
	}

	private static class FixedIterator<T> implements Iterator<T> {
		//		private static final AtomicIntegerFieldUpdater<FixedIterator> idxUpd = AtomicIntegerFieldUpdater.newUpdater(
		//				FixedIterator.class,
		//				"index"
		//		);
		private T[] values;
		private int len;
		private int index = 0;

		Iterator<T> setValues(T[] values) {
			this.values = values;
			this.len = values.length;
			this.index = 0;
			return this;
		}

		Iterator<T> reset() {
			this.values = null;
			this.len = 0;
			this.index = 0;
			return this;
		}

		@Override
		public void remove() {

		}

		@Override
		public boolean hasNext() {
			return index < len;
		}

		@Override
		public T next() {
			try {
				return values[index++];
			} catch(IndexOutOfBoundsException ignored) {
				throw new NoSuchElementException("No element at index " + (index - 1));
			}
		}
	}

}
