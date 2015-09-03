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

import org.junit.Test;
import reactor.core.support.UUIDUtils;

import java.util.*;

/**
 * @author Jon Brisbin
 */
public class CachingAlgorithmTests {

	static int items      = 10000;
	static int iterations = 500;

	@Test
	public void testUUIDGeneration() {
		long start = System.currentTimeMillis();
		for (int i = 0; i < items * iterations; i++) {
			UUIDUtils.create();
		}
		long end = System.currentTimeMillis();
		long elapsed = end - start;
		long throughput = Math.round((items * iterations) / ((elapsed * 1.0) / 1000));

		System.out.println("UUID generation throughput: " + throughput + "/sec");
	}

	@Test
	public void testHashMapStringKeyCache() {
		Map<String, List<?>> cache = new LinkedHashMap<String, List<?>>(items);

		for (int i = 0; i < items; i++) {
			List<Object> objs = new ArrayList<Object>();
			for (int j = 0; j < 10; j++) {
				objs.add(new Object());
			}
			cache.put("test" + i, objs);
		}

		long start = System.currentTimeMillis();
		for (int i = 0; i < items * iterations; i++) {
			String key = "test" + (i % items);
			cache.get(key);
		}
		long end = System.currentTimeMillis();
		long elapsed = end - start;
		long throughput = Math.round((items * iterations) / ((elapsed * 1.0) / 1000));

		System.out.println("LinkedHashMap<String,?> throughput: " + throughput + "/sec");
	}

	@Test
	public void testHashMapLongKeyCache() {
		Map<Long, List<?>> cache = new LinkedHashMap<Long, List<?>>(items);

		for (long l = 0; l < items; l++) {
			List<Object> objs = new ArrayList<Object>();
			for (int j = 0; j < 10; j++) {
				objs.add(new Object());
			}
			cache.put(l, objs);
		}

		long start = System.currentTimeMillis();
		for (long l = 0; l < items * iterations; l++) {
			cache.get(l % items);
		}
		long end = System.currentTimeMillis();
		long elapsed = end - start;
		long throughput = Math.round((items * iterations) / ((elapsed * 1.0) / 1000));

		System.out.println("LinkedHashMap<Long,?> throughput: " + throughput + "/sec");
	}

	@Test
	@SuppressWarnings({"unchecked", "rawtypes"})
	public void testSlottedMapCache() {
		int slots = 5000;
		Map[] cache = new Map[slots];

		for (long l = 0; l < items; l++) {
			int s = (int) (l % slots);
			if (null == cache[s]) {
				cache[s] = new LinkedHashMap(items / slots);
			}

			List<Object> objs = new ArrayList<Object>();
			for (int j = 0; j < 10; j++) {
				objs.add(new Object());
			}
			cache[s].put(l, objs);
		}

		long start = System.currentTimeMillis();
		for (long l = 0; l < items * iterations; l++) {
			int j = (int) (l % items);
			int x = (j % slots);
			cache[x].get(j);
		}
		long end = System.currentTimeMillis();
		long elapsed = end - start;
		long throughput = Math.round((items * iterations) / ((elapsed * 1.0) / 1000));

		System.out.println("Map[LinkedHashMap<Long,?>] throughput: " + throughput + "/sec");
	}

	@Test
	public void testSlottedArraysCache() {
		int slots = 256;
		int layer2slots = slots / 2;
		long[][][] cache = new long[slots][layer2slots][0];

		Random random = new Random();
		long[] ids = new long[items];

		for (int i = 0; i < items; i++) {
			ids[i] = random.nextLong();

			int j = (int) Math.abs(ids[i] % slots);
			int x = (i % layer2slots);

			int len = cache[j][x].length;
			if (len == 0) {
				cache[j][x] = new long[]{ids[i]};
			} else {
				long[] entry = Arrays.copyOf(cache[j][x], len + 1);
				entry[len] = ids[i];
			}
		}

		long start = System.currentTimeMillis();
		for (int i = 0; i < items * iterations; i++) {
			long id = ids[(i % items)];
			int j = (int) Math.abs(id % slots);
			int x = (j % layer2slots);

			int len = cache[j][x].length;
			for (int idi = 0; idi < len; idi++) {
				if (cache[j][x][idi] == id) {
					break;
				}
			}
		}
		long end = System.currentTimeMillis();
		long elapsed = end - start;
		long throughput = Math.round((items * iterations) / ((elapsed * 1.0) / 1000));

		System.out.println("long[][][] throughput: " + throughput + "/sec");
	}

}
