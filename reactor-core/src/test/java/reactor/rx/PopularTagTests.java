/*
 * Copyright (c) 2011-2015 Pivotal Software Inc, All Rights Reserved.
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
package reactor.rx;

import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import reactor.AbstractReactorTest;

import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

/**
 * @author Stephane Maldini
 */
public class PopularTagTests extends AbstractReactorTest {

	private static final Logger LOG = LoggerFactory.getLogger(PopularTagTests.class);

	private static final List<String> PULP_SAMPLE = Arrays.asList(
			"Look, ", "just because I don't be givin' no man a #foot massage don't make it right for #Marsellus #to throw " +
					"Antwone",
			" ",
			"into a glass #motherfucker house, ", "fuckin' up the way the nigger talks. ", "#Motherfucker do that shit #to" +
					" " +
					"me,", " he "
			, "better paralyze my #ass, ", "'cause I'll kill the #motherfucker , ", "know what I'm sayin'?"
	);


	@Test
	public void sampleTest() throws Exception {
		CountDownLatch latch = new CountDownLatch(1);

		Controls tail =
				Streams.from(PULP_SAMPLE)
						.flatMap(samuelJackson ->
										Streams
												.from(samuelJackson.split(" "))
												.filter(w -> w.startsWith("#"))
						)
						.window(5, TimeUnit.SECONDS)
						.flatMap(s -> {
							final Map<String, Long> store = new HashMap<>();
							return s
									.reduce(store, pairTuple -> {
												Long previous;
												if ((previous = store.putIfAbsent(pairTuple.t1.toLowerCase(), 1l)) != null) {
													store.put(pairTuple.t1.toLowerCase(), ++previous);
												}
												return store;
											}
									).flatMap(map -> Streams.from(map.entrySet()));
						})
						.sort((a, b) -> -a.getValue().compareTo(b.getValue()))
						.consume(
								entry -> LOG.info(entry.getKey() + ": " + entry.getValue()),
								error -> LOG.error("", error),
								nil -> latch.countDown()
						);

		awaitLatch(tail, latch);
	}

	@SuppressWarnings("unchecked")
	private void awaitLatch(Controls tail, CountDownLatch latch) throws Exception {
		if (!latch.await(5, TimeUnit.SECONDS)) {
			throw new Exception("Never completed: (" + latch.getCount() + ") "
					+ tail.debug());
		}
	}
}
