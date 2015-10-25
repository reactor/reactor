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
package reactor.aeron.processor;

import reactor.core.support.Assert;
import reactor.fn.Supplier;

import java.util.concurrent.TimeUnit;

/**
 * @author Anatoly Kadyshev
 */
public class TestUtils {

	public static void waitForTrue(long timeoutSecs,
								   Supplier<String> errorMessageSupplier,
								   Supplier<Boolean> resultSupplier)
			throws InterruptedException {
		Assert.notNull(errorMessageSupplier);
		Assert.notNull(resultSupplier);
		Assert.isTrue(timeoutSecs > 0);

		long startTime = System.nanoTime();
		long timeoutNs = TimeUnit.SECONDS.toNanos(timeoutSecs);
		do {
			try {
				if(resultSupplier.get()) {
					return;
				}
			} catch (Exception e) {
				throw new RuntimeException(e);
			}
			Thread.sleep(100);
		} while (System.nanoTime() - startTime < timeoutNs);
		throw new AssertionError(errorMessageSupplier.get());
	}

	public static void waitForTrue(long timeoutSecs,
								   String errorMessage,
								   Supplier<Boolean> resultSupplier)
			throws InterruptedException {
		waitForTrue(timeoutSecs, () -> errorMessage, resultSupplier);
	}

}
