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
import org.reactivestreams.tck.IdentityProcessorVerification;
import org.reactivestreams.tck.TestEnvironment;
import org.testng.annotations.Test;
import reactor.core.Environment;
import reactor.function.Supplier;
import reactor.rx.Stream;
import reactor.rx.action.Action;
import reactor.rx.spec.Streams;

import java.util.ArrayList;
import java.util.List;
import java.util.Random;

/**
 * @author Stephane Maldini
 */
@Test
public class StreamIdentityProcessorVerification extends IdentityProcessorVerification<Integer> {

	private final Environment env = new Environment();

	public StreamIdentityProcessorVerification() {
		super(new TestEnvironment(5000), 5000);
	}

	@Override
	public Processor<Integer, Integer> createIdentityProcessor(int bufferSize) {
		Action<Integer, Integer> action = new Action<Integer, Integer>(env.getDispatcher("sync")) {
			@Override
			protected void doNext(Integer ev) {
				broadcastNext(ev);
			}
		};
		action.env(env).prefetch(bufferSize);
		return action;
	}

	@Override
	public Publisher<Integer> createHelperPublisher(final int elements) {
		if (elements > 0) {
			List<Integer> list = new ArrayList<Integer>(elements);
			for (int i = 0; i < elements; i++) {
				list.add(i);
			}
			return Streams.defer(list);
		} else {
			final Random random = new Random();
			return Streams.defer(new Supplier<Integer>() {
				@Override
				public Integer get() {
					return random.nextInt();
				}
			});
		}
	}

	@Override
	public Publisher<Integer> createCompletedStatePublisher() {
		Stream<Integer> stream = Streams.defer(env);
		stream.broadcastComplete();
		return stream;
	}

	@Override
	public Publisher<Integer> createErrorStatePublisher() {
		Stream<Integer> stream = Streams.defer(env);
		stream.broadcastError(new Exception("oops"));
		return stream;
	}
}
