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

package reactor.bus;

import java.util.concurrent.TimeUnit;

import org.junit.Test;
import reactor.Processors;
import reactor.bus.selector.Selectors;
import reactor.core.processor.RingBufferProcessor;
import reactor.fn.Consumer;
import reactor.rx.Promise;
import reactor.rx.broadcast.Broadcaster;

import static org.hamcrest.MatcherAssert.assertThat;

/**
 * @author Jon Brisbin
 * @author Stephane Maldini
 */
@SuppressWarnings({"unchecked", "rawtypes"})
public class AwaitTests extends AbstractReactorTest {

	@Test
	public void testAwaitDoesntBlockUnnecessarily() throws InterruptedException {
		EventBus innerReactor = EventBus.config().concurrency(4).processor(Processors.queue("simple-work",
		  64)).get();

		for (int i = 0; i < 10000; i++) {
			final Promise<String> deferred = Promise.<String>ready(timer);

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


	@Test
	public void testDoesntDeadlockOnError() throws InterruptedException {

		EventBus r = EventBus.create(RingBufferProcessor.create("rb", 8));

		Broadcaster<Event<Throwable>> stream = Broadcaster.<Event<Throwable>>create();
		Promise<Long> promise = stream.log().take(16).count().to(Promise.prepare());
		r.on(Selectors.T(Throwable.class), stream.toNextConsumer());
		r.on(Selectors.$("test"), (Event<?> ev) -> {
			try {
				Thread.sleep(100);
			} catch (InterruptedException e) {
				//IGNORE
			}
			throw new RuntimeException();
		});

		for (int i = 0; i < 16; i++) {
			r.notify("test", Event.wrap("test"));
		}
		promise.await(5, TimeUnit.SECONDS);

		assert promise.peek() == 16;
		try{
			r.getProcessor().onComplete();
		}catch(Throwable c){
			c.printStackTrace();
		}

	}

}
