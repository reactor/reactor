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
package reactor.nexus.pylon;

import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

import org.junit.Test;
import reactor.core.subscription.ReactiveSession;
import reactor.core.timer.Timers;
import reactor.core.util.Exceptions;
import reactor.io.net.ReactiveNet;
import reactor.io.net.nexus.Nexus;

/**
 * @author Stephane Maldini
 */
public class PylonTests {


	public static void main(String... args) throws Exception {
		Nexus nexus = ReactiveNet.nexus();
		nexus.startAndAwait();

		Pylon.create().startAndAwait();

		final ReactiveSession<Object> s = nexus.streamCannon();
		Timers.create()
		      .schedule(aLong -> {
			      if (!s.isCancelled()) {
				      s.submit(s);
			      }
			      else {
				      Exceptions.failWithCancel();
			      }
		      }, 200, TimeUnit.MILLISECONDS);

		CountDownLatch latch = new CountDownLatch(1);

		latch.await();
	}

	@Test
	public void simpleTest(){

	}

}
