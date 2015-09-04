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

import org.junit.Test;
import org.reactivestreams.Processor;
import org.testng.SkipException;
import reactor.Subscribers;

/**
 * @author Stephane Maldini
 */
@org.testng.annotations.Test
public class SimpleWorkProcessorTests extends AbstractProcessorVerification {

	@Override
	public Processor<Long, Long> createProcessor(int bufferSize) {
		return SimpleWorkProcessor.create("simple-work", bufferSize);
	}

	@Override
	public long maxSupportedSubscribers() {
		return 1L;
	}

	@Override
	public void required_spec104_mustCallOnErrorOnAllItsSubscribersIfItEncountersANonRecoverableError() throws
	  Throwable {
		throw new SkipException("Optional multi subscribe requirement");
	}

	@Override
	public void required_mustRequestFromUpstreamForElementsThatHaveBeenRequestedLongAgo() throws Throwable {
		throw new SkipException("Optional multi subscribe requirement");
	}

/*
	@Override
	public void stochastic_spec103_mustSignalOnMethodsSequentially() throws Throwable {
		for(int i = 0 ; i < 1000 ; i++)
		super.stochastic_spec103_mustSignalOnMethodsSequentially();
	}*/

	/*@Override
	public void
	required_spec205_mustCallSubscriptionCancelIfItAlreadyHasAnSubscriptionAndReceivesAnotherOnSubscribeSignal()
	  throws Throwable {
		for(int i = 0; i < 10000; i++) {
			super
			  .required_spec205_mustCallSubscriptionCancelIfItAlreadyHasAnSubscriptionAndReceivesAnotherOnSubscribeSignal();

		}
	}*/

	@Test
	public void simpleTest() throws Exception {
		SimpleWorkProcessor<String> processor = SimpleWorkProcessor.create("simple-test-work", 10);
		processor.subscribe(Subscribers.create(
		  subscription -> {
			  subscription.request(1);
			  return null;
		  },
		  (data, sub) -> {
			  System.out.println(Thread.currentThread() + " " + data);
			  sub.request(1);
		  }
		));

		for(int i = 0; i < 100; i++){
			processor.onNext(""+i);
		}

		processor.awaitAndShutdown();
	}
}
