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

import org.junit.Test;
import reactor.AbstractReactorTest;
import reactor.Fn;
import reactor.fn.Consumer;
import reactor.fn.dispatch.ThreadPoolExecutorDispatcher;

import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

import static org.hamcrest.MatcherAssert.assertThat;

/**
 * @author Jon Brisbin
 */
@SuppressWarnings({"unchecked", "rawtypes"})
public class AwaitTests extends AbstractReactorTest {

	@Test
	public void testAwaitDoesntBlockUnnecessarily() throws InterruptedException {
		ThreadPoolExecutorDispatcher dispatcher = new ThreadPoolExecutorDispatcher(4, 64);
		Reactor reactor = R.reactor().using(env).threadPoolExecutor().get();
		Reactor innerReactor = R.reactor().using(env).using(dispatcher).get();
		for (int i = 0; i < 1000; i++) {
			final Promise<String> promise = P.<String>defer().using(env).using(reactor).get();
			final CountDownLatch latch = new CountDownLatch(1);

			promise.onSuccess(new Consumer<String>() {

				@Override
				public void accept(String t) {
					latch.countDown();
				}
			});
			Fn.schedule(new Consumer() {

				@Override
				public void accept(Object t) {
					promise.set("foo");

				}

			}, null, innerReactor);

			assertThat("latch is counted down", latch.await(5, TimeUnit.SECONDS));
		}
	}

}
