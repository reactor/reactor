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
package reactor.core.processor;

import java.util.ArrayList;
import java.util.List;

import org.junit.Ignore;
import org.junit.Test;
import reactor.Processors;
import reactor.Publishers;
import reactor.core.subscriber.TestSubscriber;
import reactor.io.buffer.Buffer;

/**?
 * @author Anatoly Kadyshev
 */
public class EmitterProcessorDemandTests {

	static final List<Integer> DATA = new ArrayList<>();
	static final int MAX_SIZE = 100;

	static {
		for(int i = 1; i < MAX_SIZE; i++){
			DATA.add(i);
		}
	}

	@Test
	public void testRed() throws InterruptedException {
		BaseProcessor<Buffer, Buffer> processor = Processors.emitter();
		TestSubscriber subscriber = TestSubscriber.createWithTimeoutSecs(1);
		processor.subscribe(subscriber);


		subscriber.request(1);
		Publishers.log(Publishers.map(Publishers.from(DATA), i -> Buffer.wrap("" + i))).subscribe(processor);

		subscriber.assertNextSignals("1");
	}

	@Test
	public void testGreen() throws InterruptedException {
		BaseProcessor<Buffer, Buffer> processor = Processors.emitter();
		TestSubscriber subscriber = TestSubscriber.createWithTimeoutSecs(1);
		processor.subscribe(subscriber);

		Publishers.log(Publishers.map(Publishers.from(DATA), i -> Buffer.wrap("" + i))).subscribe(processor);

		subscriber.request(1);

		subscriber.assertNextSignals("1");
	}

	@Test
	public void testHanging() throws InterruptedException {
		BaseProcessor<Buffer, Buffer> processor = Processors.emitter(2);
		Publishers.log(Publishers.map(Publishers.from(DATA), i -> Buffer.wrap("" + i))).subscribe(processor);

		TestSubscriber first = TestSubscriber.createWithTimeoutSecs(1);
		Publishers.log(processor, "after-1").subscribe(first);

		TestSubscriber second = TestSubscriber.createWithTimeoutSecs(1);
		Publishers.log(processor, "after-2").subscribe(second);

		second.request(1);
		second.assertNextSignals("1");

		first.request(3);
		first.assertNextSignals("1", "2", "3");
	}

	@Test
	public void testNPE() throws InterruptedException {
		BaseProcessor<Buffer, Buffer> processor = Processors.emitter(8);
		Publishers.log(Publishers.map(Publishers.from(DATA), i -> Buffer.wrap("" + i))).subscribe(processor);

		TestSubscriber first = TestSubscriber.createWithTimeoutSecs(1);
		processor.subscribe(first);

		first.request(1);
		first.assertNextSignals("1");

		TestSubscriber second = TestSubscriber.createWithTimeoutSecs(1);
		processor.subscribe(second);

		second.request(3);
		second.assertNextSignals("2", "3", "4");
	}

}