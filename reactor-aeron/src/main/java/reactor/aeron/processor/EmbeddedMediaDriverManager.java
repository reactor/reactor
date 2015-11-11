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
package reactor.aeron.processor;

import uk.co.real_logic.aeron.Aeron;
import uk.co.real_logic.aeron.driver.MediaDriver;
import uk.co.real_logic.agrona.CloseHelper;

/**
 * @author Anatoly Kadyshev
 */
class EmbeddedMediaDriverManager {

	private static final EmbeddedMediaDriverManager INSTANCE = new EmbeddedMediaDriverManager();

	private MediaDriver driver;

	private int counter = 0;

	private Aeron aeron;

	static EmbeddedMediaDriverManager getInstance() {
		return INSTANCE;
	}

	synchronized void launchDriver() {
		if (driver == null) {
			driver = MediaDriver.launchEmbedded();
			Aeron.Context ctx = new Aeron.Context();
			ctx.dirName(driver.contextDirName());
			this.aeron = Aeron.connect(ctx);
		}
		counter++;
	}

	synchronized void shutdownDriver() {
		if (counter > 0) {
			if (--counter == 0) {
				forceShutdown();
			}
		}
	}

	synchronized Aeron getAeron() {
		return aeron;
	}

	synchronized int getCounter() {
		return counter;
	}

	synchronized void forceShutdown() {
		counter = 0;

		aeron.close();
		aeron = null;

		CloseHelper.quietClose(driver);
		driver = null;
	}
}
