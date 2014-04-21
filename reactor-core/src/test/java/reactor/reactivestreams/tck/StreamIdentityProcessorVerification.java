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

import org.reactivestreams.api.Processor;
import org.reactivestreams.spi.Publisher;
import org.reactivestreams.spi.Subscriber;
import org.reactivestreams.tck.IdentityProcessorVerification;
import org.reactivestreams.tck.TestEnvironment;
import org.testng.annotations.Test;
import reactor.core.Environment;
import reactor.rx.action.Action;
import reactor.rx.spec.Streams;

/**
 * @author Stephane Maldini
 */
@Test
public class StreamIdentityProcessorVerification extends IdentityProcessorVerification<Integer> {

	private final Environment env = new Environment();

	public StreamIdentityProcessorVerification() {
		super(new TestEnvironment(10000), 10000);
	}

	@Override
	public Processor<Integer, Integer> createIdentityProcessor(int bufferSize) {
		Action<Integer,Integer> action = new Action<Integer,Integer>(env.getDefaultDispatcher());
		action.env(env).prefetch(bufferSize);
		return action;
	}

	@Override
	public Publisher<Integer> createHelperPublisher(final int elements) {
		return new Publisher<Integer>() {
			@Override
			public void subscribe(Subscriber<Integer> subscriber) {
				for(int i = 0; i < elements; i++)
					subscriber.onNext(i);
				subscriber.onComplete();
			}
		};
	}

	@Override
	public Publisher<Integer> createCompletedStatePublisher() {
		Streams.<Integer>defer(env).broadcastComplete();
		return Streams.<Integer>defer(env).getPublisher();
	}

	@Override
	public Publisher<Integer> createErrorStatePublisher() {
		Streams.<Integer>defer(env).getPublisher().broadcastError(new Exception("oops"));
		return Streams.<Integer>defer(env).getPublisher();
	}
}
