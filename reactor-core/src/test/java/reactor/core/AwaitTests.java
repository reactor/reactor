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

package reactor.core;

import static org.hamcrest.MatcherAssert.assertThat;

import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

import org.junit.Test;

import reactor.AbstractReactorTest;
import reactor.core.composable.Deferred;
import reactor.core.composable.Promise;
import reactor.core.composable.spec.Promises;
import reactor.core.spec.Reactors;
import reactor.event.dispatch.ThreadPoolExecutorDispatcher;
import reactor.function.Consumer;

/**
 * @author Jon Brisbin
 */
@SuppressWarnings({"unchecked", "rawtypes"})
public class AwaitTests extends AbstractReactorTest {

	@Test
	public void testAwaitDoesntBlockUnnecessarily() throws InterruptedException {
		ThreadPoolExecutorDispatcher dispatcher = new ThreadPoolExecutorDispatcher(4, 64);

		Reactor innerReactor = Reactors.reactor().env(env).dispatcher(dispatcher).get();

		for (int i = 0; i < 1000; i++) {
			final Deferred<String, Promise<String>> deferred = Promises.<String>defer().env(env).dispatcher("threadPoolExecutor").get();
			final CountDownLatch latch = new CountDownLatch(1);

			Promise<String> promise = deferred.compose();
			promise.onSuccess(new Consumer<String>() {

				@Override
				public void accept(String t) {
					latch.countDown();
				}
			});
			Reactors.schedule(new Consumer() {

				@Override
				public void accept(Object t) {
					deferred.accept("foo");
				}

			}, null, innerReactor);

			assertThat("latch is counted down", latch.await(5, TimeUnit.SECONDS));
		}
	}

}
