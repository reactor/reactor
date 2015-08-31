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

package reactor;

import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import reactor.core.processor.ProcessorService;
import reactor.fn.timer.Timer;

/**
 * @author Stephane Maldini
 */
public abstract class AbstractReactorTest {

	protected static ProcessorService<?> asyncService;
	protected static ProcessorService<?> workService;
	protected static Timer            timer;

	@BeforeClass
	public static void loadEnv() {
		timer = Timers.global();
		workService = Processors.workService("work", 2048, 8, Throwable::printStackTrace, null, false);
		asyncService = Processors.asyncService("async", 2048, 8, Throwable::printStackTrace, null, false);
	}

	@AfterClass
	public static void closeEnv() {
		timer = null;
		Timers.unregisterGlobal();
		workService.shutdown();
		asyncService.shutdown();
	}

	static {
		System.setProperty("reactor.trace.cancel", "true");
	}

}
