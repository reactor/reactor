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
package reactor.reactivestreams.tck;

import org.junit.Test;
import org.reactivestreams.Processor;
import org.reactivestreams.Publisher;
import org.reactivestreams.Subscriber;
import org.reactivestreams.Subscription;
import org.reactivestreams.tck.TestEnvironment;
import reactor.core.Environment;
import reactor.rx.Stream;
import reactor.rx.spec.Streams;
import reactor.util.Assert;

import java.util.ArrayList;
import java.util.List;
import java.util.Random;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

/**
 * @author Stephane Maldini
 */
@org.testng.annotations.Test
public class StreamIdentityProcessorTests extends org.reactivestreams.tck.IdentityProcessorVerification<Integer> {


	private final Environment env = new Environment();

	public StreamIdentityProcessorTests() {
		super(new TestEnvironment(2500, true), 3500);
	}

	@Override
	public Processor<Integer, Integer> createIdentityProcessor(int bufferSize) {
		return
				Streams.<Integer>defer(env)
						.capacity(bufferSize)
						/*.parallel()
						.map(stream -> stream*/
										.map(integer -> integer)
										.distinctUntilChanged()
										.<Integer>scan(tuple -> tuple.getT1())
										.filter(integer -> integer >= 0)
										.buffer(1)
										.last()
										.<Integer>split()

					/*	)
						.<Integer>merge()
*/
						.combine()
				;
	}

	@Override
	public Publisher<Integer> createHelperPublisher(long elements) {
		if (elements != Long.MAX_VALUE && elements > 0) {
			List<Integer> list = new ArrayList<Integer>();
			for (int i = 1; i <= elements; i++) {
				list.add(i);
			}

			return Streams
					.defer(env, list)
					.filter(integer -> true)
					.map(integer -> integer);

		} else {
			final Random random = new Random();

			return Streams
					.generate(env, random::nextInt)
					.map(Math::abs);
		}
	}

	@Override
	public Publisher<Integer> createErrorStatePublisher() {
		Stream<Integer> stream = Streams.defer(env);
		stream.broadcastError(new Exception("oops"));
		return stream;
	}

	@Test
	public void testIdentityProcessor() throws InterruptedException {

		final int elements = 1_00_000;
		CountDownLatch latch = new CountDownLatch(elements + 1);

		Processor<Integer, Integer> processor = createIdentityProcessor(1000);

		Stream<Integer> stream = Streams.defer(env);

		stream.subscribe(processor);

		processor.subscribe(new Subscriber<Integer>() {
			@Override
			public void onSubscribe(Subscription s) {
				s.request(elements);
			}

			@Override
			public void onNext(Integer integer) {
				latch.countDown();
			}

			@Override
			public void onError(Throwable t) {
				t.printStackTrace();
			}

			@Override
			public void onComplete() {
				System.out.println("completed!");
				latch.countDown();
			}
		});

		System.out.println(stream.debug());

		for (int i = 0; i < elements; i++) {
			stream.broadcastNext(i);
		}
		stream.broadcastComplete();

		latch.await(10,TimeUnit.SECONDS);

		System.out.println(stream.debug());
		long count = latch.getCount();
		Assert.state(latch.getCount() == 0, "Count > 0 : " + count);

	}
}
