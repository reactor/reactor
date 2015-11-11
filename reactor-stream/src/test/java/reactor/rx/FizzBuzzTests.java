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
import reactor.AbstractReactorTest;
import reactor.core.processor.RingBufferProcessor;
import reactor.core.support.Assert;
import reactor.rx.action.Control;

import java.util.List;
import java.util.Timer;
import java.util.TimerTask;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;

/**
 * https://github.com/reactor/reactor/issues/500
 *
 * @author nitzanvolman
 * @author Stephane Maldini
 */
public class FizzBuzzTests extends AbstractReactorTest {


	@Test
	public void fizzTest() throws Throwable {
		int numOfItems = 1024;
		int batchSize = 8;
		final Timer timer = new Timer();
		AtomicLong globalCounter = new AtomicLong();
		CountDownLatch latch = new CountDownLatch(1);

		Control c = Streams.createWith((demand, subscriber) -> {
			System.out.println("demand is " + demand);
			if (!subscriber.isCancelled()) {
				for (int i = 0; i < demand; i++) {
					long curr = globalCounter.incrementAndGet();
					if (curr % 5 == 0 && curr % 3 == 0) subscriber.onNext("FizBuz \r\n");
					else if (curr % 3 == 0) subscriber.onNext("Fiz ");
					else if (curr % 5 == 0) subscriber.onNext("Buz ");
					else subscriber.onNext(String.valueOf(curr) + " ");

					if (globalCounter.get() > numOfItems) {
						subscriber.onComplete();
						return;
					}
				}
			}
		})
		  .flatMap((s) -> Streams.withOverflowSupport((sub) -> timer.schedule(new TimerTask() {
			  @Override
			  public void run() {
				  sub.onNext(s);
				  sub.onComplete();
			  }
		  }, 10)))
		  .capacity(batchSize)
		  .log()
//                .observe(System.out::print)
		  .consume(numOfItems);

		while (c.isPublishing()) ;
	}


	@Test
	public void indexBugTest() throws InterruptedException {
		int numOfItems = 20;

		//this line causes an java.lang.ArrayIndexOutOfBoundsException unless there is a break point in ZipAction
		// .createSubscriber()
		RingBufferProcessor<String> ring = RingBufferProcessor.create("test", 1024);

		//this line works
//        Broadcaster<String> ring = Broadcaster.create(Environment.get());

		Stream<String> stream = Streams.wrap(ring.start());

		Stream<String> stream2 = stream
		  .zipWith(Streams.createWith((d, s) -> {
			  for (int i = 0; i < d; i++) {
				  s.onNext(System.currentTimeMillis());
			  }
		  }), t -> String.format("%s : %s", t.getT2(), t.getT1()))
		  .observeError(Throwable.class, (o, t) -> {
			  System.err.println(t.toString());
			  t.printStackTrace();
		  });

		Promise<List<String>> p = stream2
		  .observe(System.out::println)
		  .buffer(numOfItems)
		  .next();

		for (int curr = 0; curr < numOfItems; curr++) {
			if (curr % 5 == 0 && curr % 3 == 0) ring.onNext("FizBuz");
			else if (curr % 3 == 0) ring.onNext("Fiz");
			else if (curr % 5 == 0) ring.onNext("Buz");
			else ring.onNext(String.valueOf(curr));
		}

		Assert.isTrue(p.await(5, TimeUnit.SECONDS) != null, "Has not returned list");

	}
}

