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
import org.reactivestreams.Publisher;
import org.reactivestreams.Subscriber;
import org.reactivestreams.Subscription;
import org.reactivestreams.tck.TestEnvironment;
import reactor.core.Environment;
import reactor.rx.Stream;
import reactor.rx.Streams;
import reactor.rx.action.CombineAction;
import reactor.rx.stream.HotStream;
import reactor.tuple.Tuple2;
import reactor.util.Assert;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;

/**
 * @author Stephane Maldini
 */
@org.testng.annotations.Test
public class StreamIdentityProcessorTests extends org.reactivestreams.tck.IdentityProcessorVerification<Integer> {


	private final Environment             env      = new Environment();
	private final Map<Thread, AtomicLong> counters = new ConcurrentHashMap<>();

	public StreamIdentityProcessorTests() {
		super(new TestEnvironment(2500, true), 3500);
	}

	@Override
	public CombineAction<Integer, Integer> createIdentityProcessor(int bufferSize) {

		Stream<String> otherStream = Streams.just("test", "test2", "test3");

		CombineAction<Integer, Integer> processor = Streams.<Integer>defer(env)
				.keepAlive(false)
				.capacity(bufferSize)
				.partition(2)
				.flatMap(stream -> stream
								.dispatchOn(env, env.getCachedDispatcher())
								.observe(i -> {
									AtomicLong counter = counters.get(Thread.currentThread());
									if (counter == null) {
										counter = new AtomicLong();
										counters.put(Thread.currentThread(), counter);
									}
									counter.incrementAndGet();
								})
								.scan(0, Tuple2::getT1)
								.filter(integer -> integer >= 0)
								.reduce(() -> 0, 1, tuple -> -tuple.getT1())
								.map(integer -> -integer)
								.sample(1)
								.buffer(1024, 200, TimeUnit.MILLISECONDS)
								.<Integer>split()

/*
	.flatMap(i ->
												//Streams.just(i)
												Streams.zip(Streams.just(i), otherStream, tuple -> tuple.getT1())
//														.dispatchOn(env.getCachedDispatcher())
								)
 */
				)
				.log("flatMap")
				.when(Throwable.class, Throwable::printStackTrace)
				.combine();

		return processor;
	}

	@Override
	public Publisher<Integer> createHelperPublisher(long elements) {
		if (elements != Long.MAX_VALUE && elements > 0) {
			List<Integer> list = new ArrayList<Integer>();
			for (int i = 1; i <= elements; i++) {
				list.add(i);
			}

			return Streams
					.defer(list)
					.log("iterable-publisher")
					.dispatchOn(env)
					.filter(integer -> true)
					.map(integer -> integer);

		} else {
			final Random random = new Random();

			return Streams
					.generate(random::nextInt)
					.log("random-publisher")
					.map(Math::abs);
		}
	}

	@Override
	public Publisher<Integer> createErrorStatePublisher() {
		return Streams.fail(new Exception("oops")).cast(Integer.class);
	}

	@Test
	public void testIdentityProcessor() throws InterruptedException {
		final int elements = 10000;
		CountDownLatch latch = new CountDownLatch(elements);

		CombineAction<Integer, Integer> processor = createIdentityProcessor(1000);

		HotStream<Integer> stream = Streams.defer(env);

		stream.subscribe(processor);
		System.out.println(processor.debug());
		Thread.sleep(2000);

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
		//stream.broadcastComplete();

		latch.await(8, TimeUnit.SECONDS);

		System.out.println(processor.debug());
		System.out.println(counters);
		long count = latch.getCount();
		Assert.state(latch.getCount() == 0, "Count > 0 : " + count + " , Running on " + Environment.PROCESSORS + " CPU");

	}
}
