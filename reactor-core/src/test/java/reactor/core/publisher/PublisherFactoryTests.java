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
package reactor.core.publisher;

import org.reactivestreams.Publisher;
import org.reactivestreams.Subscription;
import org.reactivestreams.tck.PublisherVerification;
import org.reactivestreams.tck.TestEnvironment;
import org.testng.annotations.Test;
import reactor.Publishers;

import java.util.concurrent.atomic.AtomicLong;

/**
 * @author Stephane Maldini
 */
@Test
public class PublisherFactoryTests extends PublisherVerification<Long> {

	public PublisherFactoryTests() {
		super(new TestEnvironment(500, true), 1000);
	}

	@org.junit.Test
	public void simpleTest(){

	}

	@Override
	public Publisher<Long> createPublisher(long elements) {
		return
		  Publishers.trampoline(
			Publishers.log(
			  Publishers.lift(
			    Publishers.<Long, AtomicLong>create(
				  (s) -> {
					  long cursor = s.context().getAndIncrement();
					  if (cursor < elements) {
						  s.onNext(cursor);
					  } else {
						  s.onComplete();
					  }
				  },
				  s -> new AtomicLong(0L)
			    ),
			    (data, sub) -> sub.onNext(data * 10)
			  ),
			  "log-test"
			)
		  );
	}

	@Override
	public Publisher<Long> createFailedPublisher() {
		return sub -> {
			sub.onSubscribe(new Subscription() {
				@Override
				public void request(long n) {

				}

				@Override
				public void cancel() {

				}
			});

			sub.onError(new Exception("test"));
		};
	}
}
