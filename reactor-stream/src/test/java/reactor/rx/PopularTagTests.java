/*
 * Copyright (c) 2011-2016 Pivotal Software Inc, All Rights Reserved.
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
import reactor.core.support.Logger;
import reactor.AbstractReactorTest;
import reactor.fn.tuple.Tuple;
import reactor.rx.action.Control;

import java.util.Arrays;
import java.util.List;
import java.util.concurrent.CountDownLatch;

import static java.util.concurrent.TimeUnit.SECONDS;

/**
 * @author Stephane Maldini
 */
public class PopularTagTests extends AbstractReactorTest {

	private static final Logger LOG = Logger.getLogger(PopularTagTests.class);

	private static final List<String> PULP_SAMPLE = Arrays.asList(
	  "Look, ", "just because I don't be givin' no man a #foot massage don't make it right for #Marsellus #to throw " +
		"Antwone",
	  " ",
	  "into a glass #motherfucker house, ", "fuckin' up the way the nigger talks. ", "#Motherfucker do that shit #to" +
		" " +
		"me,", " he "
	  , "better paralyze my ass, ", "'cause I'll kill the #motherfucker , ", "know what I'm sayin'?"
	);


	@Test
	public void sampleTest() throws Exception {
		CountDownLatch latch = new CountDownLatch(1);

		Control top10every1second =
		  Streams.from(PULP_SAMPLE)
		    .dispatchOn(asyncGroup)
			.flatMap(samuelJackson ->
				Streams
				  .from(samuelJackson.split(" "))
				  .dispatchOn(asyncGroup)
				  .filter(w -> !w.trim().isEmpty())
				  .observe(i -> simulateLatency())
			)
			.map(w -> Tuple.of(w, 1))
		    .window(2, SECONDS)
			.flatMap(s ->
				BiStreams.reduceByKey(s, (acc, next) -> acc + next)
				  .sort((a, b) -> -a.t2.compareTo(b.t2))
				  .take(10)
				  .finallyDo(_s -> LOG.info("------------------------ window complete! ----------------------"))
			)
			.consume(
			  entry -> LOG.info(entry.t1 + ": " + entry.t2),
			  error -> LOG.error("", error),
			  nil -> latch.countDown()
			);

		awaitLatch(top10every1second, latch);
	}

	private void simulateLatency() {
		try {
			Thread.sleep(100);
		} catch (InterruptedException e) {
			e.printStackTrace();
		}
	}

	@SuppressWarnings("unchecked")
	private void awaitLatch(Control tail, CountDownLatch latch) throws Exception {
		if (!latch.await(10, SECONDS)) {
			throw new Exception("Never completed: (" + latch.getCount() + ") "
			  + tail.debug());
		}
	}
}
