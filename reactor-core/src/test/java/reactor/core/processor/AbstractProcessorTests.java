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
import org.reactivestreams.Subscriber;
import org.reactivestreams.Subscription;
import org.reactivestreams.tck.TestEnvironment;

import java.util.Random;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

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
		return (long)element;
	}

	@Override
	public void required_mustRequestFromUpstreamForElementsThatHaveBeenRequestedLongAgo() throws Throwable {
		//IGNORE since subscribers see distinct data
	}

	@Override
	public Publisher<Long> createHelperPublisher(final long elements) {
		if (elements < 100 && elements > 0) {
			return new Publisher<Long>() {
				long cursor = 0;
				@Override
				public void subscribe(final Subscriber<? super Long> s) {
					s.onSubscribe(new Subscription() {

						volatile boolean terminated = false;

						@Override
						public void request(long n) {
							if(terminated) return;
							long i = 0l;
							while(i < n && cursor < elements){
								if(terminated) {
									break;
								}
								s.onNext(cursor++);
								i++;
							}
							if(cursor == elements){
								terminated = true;
								s.onComplete();
							}
						}

						@Override
						public void cancel() {
							terminated = true;
						}
					});
				}
			};

		} else {
			final Random random = new Random();

			return new Publisher<Long>() {
				@Override
				public void subscribe(final Subscriber<? super Long> s) {
					s.onSubscribe(new Subscription() {
						volatile boolean terminated = false;

						@Override
						public void request(long n) {
							if(!terminated) {
								for(long i = 0; i< n; i++) {
									s.onNext(random.nextLong());
								}
							}
						}

						@Override
						public void cancel() {
							terminated = true;
						}
					});
				}
			};
		}
	}

	@Override
	public Publisher<Long> createFailedPublisher() {
		return new Publisher<Long>() {
			@Override
			public void subscribe(final Subscriber<? super Long> s) {
				s.onSubscribe(new Subscription() {
					@Override
					public void request(long n) {
					}

					@Override
					public void cancel() {
					}
				});
				s.onError(new Exception("test"));

			}
		};
	}
}
