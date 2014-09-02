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

import org.junit.Test;
import reactor.AbstractReactorTest;
import reactor.core.spec.Reactors;
import reactor.event.dispatch.ThreadPoolExecutorDispatcher;
import reactor.function.Consumer;
import reactor.rx.Promise;
import reactor.rx.spec.Promises;

import java.util.concurrent.TimeUnit;

import static org.hamcrest.MatcherAssert.assertThat;

/**
 * @author Jon Brisbin
 * @author Stephane Maldini
 */
@SuppressWarnings({"unchecked", "rawtypes"})
public class AwaitTests extends AbstractReactorTest {

	@Test
	public void testAwaitDoesntBlockUnnecessarily() throws InterruptedException {
		ThreadPoolExecutorDispatcher dispatcher = new ThreadPoolExecutorDispatcher(4, 64);

		Reactor innerReactor = Reactors.reactor().env(env).dispatcher(dispatcher).get();

		for (int i = 0; i < 10000; i++) {
			final Promise<String> deferred = Promises.<String>config()
					.env(env)
					.get();

			innerReactor.schedule(new Consumer() {

				@Override
				public void accept(Object t) {
					deferred.onNext("foo");
				}

			}, null);

			String latchRes = deferred.await(10, TimeUnit.SECONDS);
			assertThat("latch is not counted down : " + deferred.debug(), "foo".equals(latchRes));
		}
	}

}
