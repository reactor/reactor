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

import org.reactivestreams.Processor;
import reactor.Processors;

/**
 * @author Stephane Maldini
 */
@org.testng.annotations.Test
public class EmitterProcessorTests extends AbstractProcessorVerification {

	@Override
	public Processor<Long, Long> createProcessor(int bufferSize) {
		Processor<Long, Long> p = Processors.<Long>emitter(bufferSize);

		/*Processor<Long, Long> p2 = Processors.queue();
		Processor<Long, Long> p3 = Processors.topic();

		Processors.log(p2, "queue").subscribe(p);
		Processors.log(p, "emitter").subscribe(p3);

		return Processors.create(p2, Processors.log(p3, "topic"));*/

		return p;
	}

}
