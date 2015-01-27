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
package reactor.reactivestreams.tck;

import org.junit.Before;
import org.junit.Test;
import org.reactivestreams.Publisher;
import org.reactivestreams.Subscriber;
import org.reactivestreams.Subscription;
import org.reactivestreams.tck.TestEnvironment;
import org.testng.annotations.BeforeTest;
import reactor.Environment;
import reactor.core.DispatcherSupplier;
import reactor.core.support.Assert;
import reactor.fn.tuple.Tuple1;
import reactor.rx.Stream;
import reactor.rx.Streams;
import reactor.rx.action.combination.CombineAction;
import reactor.rx.broadcast.Broadcaster;

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


	private final Map<Thread, AtomicLong> counters = new ConcurrentHashMap<>();

	private DispatcherSupplier dispatchers;
	private Environment        env;

	public StreamIdentityProcessorTests() {
		super(new TestEnvironment(2500, true), 3500);
	}

	@BeforeTest
	@Before
	public void startEnv() {
		env = Environment.initializeIfEmpty().assignErrorJournal();
		dispatchers = Environment.newCachedDispatchers(2, "test-partition");

		//preinit the two dispatchers
		dispatchers.get();
		dispatchers.get();
	}

	@Override
	public CombineAction<Integer, Integer> createIdentityProcessor(int bufferSize) {

		Stream<String> otherStream = Streams.just("test", "test2", "test3");
		//Dispatcher dispatcherZip = env.getCachedDispatcher();
		AtomicLong total = new AtomicLong();
		System.out.println("Providing new processor");
		final CombineAction<Integer, Integer> integerIntegerCombineAction =
				Broadcaster.<Integer>create(Environment.get())
						.capacity(bufferSize)
						.partition(2)
						.flatMap(stream -> stream
										.dispatchOn(env, dispatchers.get())
										.scan(0, (prev, next) -> -next)
										.filter(integer -> integer <= 0)
										.sample(1)
										.observe(this::monitorThreadUse)
										.map(integer -> -integer)
										.buffer(1024, 200, TimeUnit.MILLISECONDS)
										.<Integer>split()
										.flatMap(i ->
														Streams.zip(Streams.just(i), otherStream, Tuple1::getT1)
														//Streams.just(i)
												//		.log(stream.key() + ":zip")
										)
						)
						.when(Throwable.class, Throwable::printStackTrace)
						.combine();

		/*Streams.period(env.getTimer(), 2, 1)
				.takeWhile(i -> integerIntegerCombineAction.isPublishing())
				.consume(i -> System.out.println(integerIntegerCombineAction.debug()) );
*/
		return integerIntegerCombineAction;
	}

	private void monitorThreadUse(int val) {
		AtomicLong counter = counters.get(Thread.currentThread());
		if (counter == null) {
			counter = new AtomicLong();
			counters.put(Thread.currentThread(), counter);
		}
		counter.incrementAndGet();
	}

	@Override
	public Publisher<Integer> createHelperPublisher(long elements) {
		if (elements < 100 && elements > 0) {
			List<Integer> list = new ArrayList<Integer>();
			for (int i = 1; i <= elements; i++) {
				list.add(i);
			}

			return Streams
					.from(list)
					.log("iterable-publisher")
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

	@Override
	@org.testng.annotations.Test
	public void spec212_mustNotCallOnSubscribeMoreThanOnceBasedOnObjectEquality_specViolation() throws Throwable {
	}

	@Override
	public void spec317_mustSignalOnErrorWhenPendingAboveLongMaxValue() throws Throwable {
		//IGNORE (since 1.0 RC2)
	}/*

	@Override
	public void spec317_mustSupportACumulativePendingElementCountUpToLongMaxValue() throws Throwable {
		for(int i = 0; i < 10000; i++)
		super.spec317_mustSupportACumulativePendingElementCountUpToLongMaxValue();
	}

	@Override
	public void spec317_mustSupportAPendingElementCountUpToLongMaxValue() throws Throwable {
		for(int i = 0; i < 10000; i++)
		super.spec317_mustSupportAPendingElementCountUpToLongMaxValue();
	}*/


	@Test
	public void testColdIdentityProcessor() throws InterruptedException {
		final int elements = 10;
		CountDownLatch latch = new CountDownLatch(elements+1);

		CombineAction<Integer, Integer> processor = createIdentityProcessor(16);

		createHelperPublisher(10).subscribe(processor);

		System.out.println(processor.debug());
		List<Integer> list = new ArrayList<>();

		processor.subscribe(new Subscriber<Integer>() {
			Subscription s;

			@Override
			public void onSubscribe(Subscription s) {
				this.s = s;
				s.request(1);
			}

			@Override
			public void onNext(Integer integer) {
				synchronized (list) {
					list.add(integer);
				}
				latch.countDown();
				if (latch.getCount() > 0) {
					s.request(1);
				}
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
		//stream.broadcastComplete();

		latch.await(8, TimeUnit.SECONDS);
		System.out.println(processor.debug());

		System.out.println(counters);
		long count = latch.getCount();
		Assert.state(latch.getCount() == 0, "Count > 0 : " + count + " ("+list+")  , Running on " + Environment.PROCESSORS + " CPU");
	}

	@Test
	public void testHotIdentityProcessor() throws InterruptedException {
		final int elements = 10000;
		CountDownLatch latch = new CountDownLatch(elements);

		CombineAction<Integer, Integer> processor = createIdentityProcessor(1000);

		Broadcaster<Integer> stream = Broadcaster.create();

		stream.subscribe(processor);
		System.out.println(processor.debug());

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


		for (int i = 0; i < elements; i++) {
			stream.onNext(i);
		}
		//stream.broadcastComplete();

		latch.await(8, TimeUnit.SECONDS);
		System.out.println(stream.debug());

		System.out.println(counters);
		long count = latch.getCount();
		Assert.state(latch.getCount() == 0, "Count > 0 : " + count + " , Running on " + Environment.PROCESSORS + " CPU");

	}
}
