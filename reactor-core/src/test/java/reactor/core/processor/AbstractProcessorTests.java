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
package reactor.core.processor;

import org.reactivestreams.Publisher;
import org.reactivestreams.Subscription;
import org.reactivestreams.tck.TestEnvironment;
import reactor.core.reactivestreams.PublisherFactory;

import java.util.Random;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.atomic.AtomicLong;

/**
 * @author Stephane Maldini
 */
public abstract class AbstractProcessorTests extends org.reactivestreams.tck.IdentityProcessorVerification<Long> {

	public AbstractProcessorTests() {
		super(new TestEnvironment(2000, true), 3500);
	}

	@Override
	public ExecutorService publisherExecutorService() {
		return Executors.newCachedThreadPool();
	}

	@Override
	public Long createElement(int element) {
		return (long) element;
	}

	@Override
	public void required_mustRequestFromUpstreamForElementsThatHaveBeenRequestedLongAgo() throws Throwable {
		//IGNORE since subscribers see distinct data
	}

	@Override
	public Publisher<Long> createHelperPublisher(final long elements) {
		if (elements < 100 && elements > 0) {
			return PublisherFactory.forEach(
					(s) -> {
						long cursor = s.context().getAndIncrement();
						if (cursor < elements){
							s.onNext(cursor);
						}else{
							s.onComplete();
						}
					},
					s -> new AtomicLong(0L)
			);
		} else {
			final Random random = new Random();
			return PublisherFactory.create(
					(n, s) -> {
						for (long i = 0; i < n; i++) {
							s.onNext(random.nextLong());
						}
					}
			);
		}
	}

	@Override
	public Publisher<Long> createFailedPublisher() {
		return s -> {
			s.onSubscribe(new Subscription() {
				@Override
				public void request(long n) {
				}

				@Override
				public void cancel() {
				}
			});
			s.onError(new Exception("test"));

		};
	}
}
